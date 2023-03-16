/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.StdUDF1;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.StdUDF3;
import com.linkedin.transport.api.udf.StdUDF4;
import com.linkedin.transport.api.udf.StdUDF5;
import com.linkedin.transport.api.udf.StdUDF6;
import com.linkedin.transport.api.udf.StdUDF7;
import com.linkedin.transport.api.udf.StdUDF8;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.typesystem.GenericTypeSignatureElement;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.SignatureBinder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ClassUtils;

import static com.linkedin.transport.trino.StdUDFUtils.quoteReservedKeywords;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.*;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.*;
import static io.trino.spi.function.TypeVariableConstraint.*;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.util.Reflection.*;

// Suppressing argument naming convention for the evalInternal methods
@SuppressWarnings({"checkstyle:regexpsinglelinejava"})
public abstract class StdUdfWrapper extends SqlScalarFunction {

  private static final int DEFAULT_REFRESH_INTERVAL_DAYS = 1;
  private static final int JITTER_FACTOR = 50;  // to calculate jitter from delay

  protected StdUdfWrapper(StdUDF stdUDF) {
    super(FunctionMetadata.builder(FunctionKind.SCALAR)
        .nullable()
        .nondeterministic()
        .description(((TopLevelStdUDF) stdUDF).getFunctionDescription())
        .signature(Signature.builder()
            .name(((TopLevelStdUDF) stdUDF).getFunctionName())
            .typeVariableConstraints(getTypeVariableConstraintsForStdUdf(stdUDF))
            .returnType(parseTypeSignature(quoteReservedKeywords(stdUDF.getOutputParameterSignature()), ImmutableSet.of()))
            .argumentTypes(stdUDF.getInputParameterSignatures().stream()
                .map(typeSignature -> parseTypeSignature(quoteReservedKeywords(typeSignature), ImmutableSet.of())).collect(Collectors.toList()))
            .build())
        .build());
  }

  @VisibleForTesting
  static List<TypeVariableConstraint> getTypeVariableConstraintsForStdUdf(StdUDF stdUdf) {
    Set<GenericTypeSignatureElement> genericTypes = new HashSet<>();
    for (String s : stdUdf.getInputParameterSignatures()) {
      genericTypes.addAll(com.linkedin.transport.typesystem.TypeSignature.parse(s).getGenericTypeSignatureElements());
    }
    genericTypes.addAll(com.linkedin.transport.typesystem.TypeSignature.parse(stdUdf.getOutputParameterSignature())
        .getGenericTypeSignatureElements());
    return genericTypes.stream().map(t -> typeVariable(t.toString())).collect(Collectors.toList());
  }

  protected long getRefreshIntervalMillis() {
    return TimeUnit.DAYS.toMillis(DEFAULT_REFRESH_INTERVAL_DAYS);
  }

  private void registerNestedDependencies(Type nestedType, FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder builder) {
    builder.addType(nestedType.getTypeSignature());

    if (nestedType instanceof RowType) {
      nestedType.getTypeParameters().forEach(type -> registerNestedDependencies(type, builder));
    } else if (nestedType instanceof ArrayType) {
      registerNestedDependencies(((ArrayType) nestedType).getElementType(), builder);
    } else if (nestedType instanceof MapType) {
      Type keyType = ((MapType) nestedType).getKeyType();
      Type valueType = ((MapType) nestedType).getValueType();
      builder.addOperator(EQUAL, ImmutableList.of(keyType, keyType));
      registerNestedDependencies(keyType, builder);
      registerNestedDependencies(valueType, builder);
    }
  }

  @Override
  public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature) {
    FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();

    registerNestedDependencies(boundSignature.getReturnType(), builder);
    List<Type> argumentTypes = boundSignature.getArgumentTypes();
    argumentTypes.forEach(type -> registerNestedDependencies(type, builder));

    return builder.build();
  }

  @Override
  public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies) {
    FunctionMetadata metadata = getFunctionMetadata();
    FunctionBinding functionBinding = SignatureBinder.bindFunction(metadata.getFunctionId(), metadata.getSignature(), boundSignature);
    StdFactory stdFactory = new TrinoFactory(functionBinding, functionDependencies);
    StdUDF stdUDF = getStdUDF();
    stdUDF.init(stdFactory);
    // Subtract a small jitter value so that refresh is triggered on first call
    // while ensuring subsequent calls do not happen at the same time across workers
    long initialJitter = getRefreshIntervalMillis() / JITTER_FACTOR;
    int initialJitterInt = initialJitter > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialJitter;
    AtomicLong requiredFilesNextRefreshTime = new AtomicLong(System.currentTimeMillis()
        - (new Random()).nextInt(initialJitterInt));
    boolean[] nullableArguments = stdUDF.getAndCheckNullableArguments();

    return new ChoicesSpecializedSqlScalarFunction(
        boundSignature,
        NULLABLE_RETURN,
        getNullConventionForArguments(nullableArguments),
        getMethodHandle(stdUDF, boundSignature, nullableArguments, requiredFilesNextRefreshTime));
  }

  private MethodHandle getMethodHandle(StdUDF stdUDF, BoundSignature boundSignature, boolean[] nullableArguments,
      AtomicLong requiredFilesNextRefreshTime) {
    Type[] inputTypes = boundSignature.getArgumentTypes().toArray(new Type[0]);
    Type outputType = boundSignature.getReturnType();

    // Generic MethodHandle for eval where all arguments are of type Object
    Class<?>[] genericMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, true);
    MethodHandle genericMethodHandle =
        methodHandle(StdUdfWrapper.class, "evalInternal", genericMethodHandleArgumentTypes).bindTo(this);

    Class<?>[] specificMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, false);
    Class<?> specificMethodHandleReturnType = getJavaTypeForNullability(outputType, true);
    MethodType specificMethodType =
        MethodType.methodType(specificMethodHandleReturnType, specificMethodHandleArgumentTypes);

    // Specific MethodHandle required by trino where argument types map to the type signature
    MethodHandle specificMethodHandle = MethodHandles.explicitCastArguments(genericMethodHandle, specificMethodType);
    return MethodHandles.insertArguments(specificMethodHandle, 0, stdUDF, inputTypes,
        outputType instanceof IntegerType, requiredFilesNextRefreshTime);
  }

  private List<InvocationConvention.InvocationArgumentConvention> getNullConventionForArguments(
      boolean[] nullableArguments) {
    return IntStream.range(0, nullableArguments.length)
        .mapToObj(idx -> nullableArguments[idx] ? BOXED_NULLABLE : NEVER_NULL)
        .collect(Collectors.toList());
  }

  private StdData[] wrapArguments(StdUDF stdUDF, Type[] types, Object[] arguments) {
    StdFactory stdFactory = stdUDF.getStdFactory();
    StdData[] stdData = new StdData[arguments.length];
    // TODO: Reuse wrapper objects by creating them once upon initialization and reuse them here
    // along the same lines of what we do in Hive implementation.
    // JIRA: https://jira01.corp.linkedin.com:8443/browse/LIHADOOP-34894
    for (int i = 0; i < stdData.length; i++) {
      stdData[i] = TrinoWrapper.createStdData(arguments[i], types[i], stdFactory);
    }
    return stdData;
  }

  protected Object eval(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object... arguments) {
    StdData[] args = wrapArguments(stdUDF, types, arguments);
    if (requiredFilesNextRefreshTime.get() <= System.currentTimeMillis()) {
      String[] requiredFiles = getRequiredFiles(stdUDF, args);
      processRequiredFiles(stdUDF, requiredFiles, requiredFilesNextRefreshTime);
    }
    StdData result;
    switch (args.length) {
      case 0:
        result = ((StdUDF0) stdUDF).eval();
        break;
      case 1:
        result = ((StdUDF1) stdUDF).eval(args[0]);
        break;
      case 2:
        result = ((StdUDF2) stdUDF).eval(args[0], args[1]);
        break;
      case 3:
        result = ((StdUDF3) stdUDF).eval(args[0], args[1], args[2]);
        break;
      case 4:
        result = ((StdUDF4) stdUDF).eval(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        result = ((StdUDF5) stdUDF).eval(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        result = ((StdUDF6) stdUDF).eval(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        result = ((StdUDF7) stdUDF).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        result = ((StdUDF8) stdUDF).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new RuntimeException("eval not supported yet for StdUDF" + args.length);
    }
    if (result == null) {
      return null;
    } else if (isIntegerReturnType) {
      return ((Number) ((PlatformData) result).getUnderlyingData()).longValue();
    } else {
      return ((PlatformData) result).getUnderlyingData();
    }
  }

  private String[] getRequiredFiles(StdUDF stdUDF, StdData[] args) {
    String[] requiredFiles;
    switch (args.length) {
      case 0:
        requiredFiles = ((StdUDF0) stdUDF).getRequiredFiles();
        break;
      case 1:
        requiredFiles = ((StdUDF1) stdUDF).getRequiredFiles(args[0]);
        break;
      case 2:
        requiredFiles = ((StdUDF2) stdUDF).getRequiredFiles(args[0], args[1]);
        break;
      case 3:
        requiredFiles = ((StdUDF3) stdUDF).getRequiredFiles(args[0], args[1], args[2]);
        break;
      case 4:
        requiredFiles = ((StdUDF4) stdUDF).getRequiredFiles(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        requiredFiles = ((StdUDF5) stdUDF).getRequiredFiles(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        requiredFiles = ((StdUDF6) stdUDF).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        requiredFiles = ((StdUDF7) stdUDF).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5],
            args[6]);
        break;
      case 8:
        requiredFiles = ((StdUDF8) stdUDF).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5],
            args[6], args[7]);
        break;
      default:
        throw new RuntimeException("getRequiredFiles not supported yet for StdUDF" + args.length);
    }
    return requiredFiles;
  }

  private synchronized void processRequiredFiles(StdUDF stdUDF, String[] requiredFiles,
      AtomicLong requiredFilesNextRefreshTime) {
    if (requiredFilesNextRefreshTime.get() <= System.currentTimeMillis()) {
      try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
        String[] copiedFiles = new String[requiredFiles.length];
        FileSystemClient client = new FileSystemClient();
        for (int i = 0; i < requiredFiles.length; i++) {
          String localFilename = client.copyToLocalFile(requiredFiles[i]);
          copiedFiles[i] = localFilename;
        }
        stdUDF.processRequiredFiles(copiedFiles);
        // Determine how many times _refreshIntervalMillis needs to be added to go above currentTimeMillis
        int refreshIntervalFactor = (int) Math.ceil(
            (System.currentTimeMillis() - requiredFilesNextRefreshTime.get()) / (double) getRefreshIntervalMillis());
        requiredFilesNextRefreshTime.getAndAdd(getRefreshIntervalMillis() * Math.max(1, refreshIntervalFactor));
      }
    }
  }

  private Class getJavaTypeForNullability(Type trinoType, boolean nullableArgument) {
    if (nullableArgument) {
      return ClassUtils.primitiveToWrapper(trinoType.getJavaType());
    } else {
      return trinoType.getJavaType();
    }
  }

  private Class<?>[] getMethodHandleArgumentTypes(Type[] argTypes, boolean[] nullableArguments,
      boolean useObjectForArgumentType) {
    Class<?>[] methodHandleArgumentTypes = new Class<?>[argTypes.length + 4];
    methodHandleArgumentTypes[0] = StdUDF.class;
    methodHandleArgumentTypes[1] = Type[].class;
    methodHandleArgumentTypes[2] = boolean.class;
    methodHandleArgumentTypes[3] = AtomicLong.class;
    for (int i = 0; i < argTypes.length; i++) {
      if (useObjectForArgumentType) {
        methodHandleArgumentTypes[i + 4] = Object.class;
      } else {
        methodHandleArgumentTypes[i + 4] = getJavaTypeForNullability(argTypes[i], nullableArguments[i]);
      }
    }
    return methodHandleArgumentTypes;
  }

  protected abstract StdUDF getStdUDF();

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6, Object arg7) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6,
        arg7);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6, Object arg7, Object arg8) {
    return eval(stdUDF, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6,
        arg7, arg8);
  }
}
