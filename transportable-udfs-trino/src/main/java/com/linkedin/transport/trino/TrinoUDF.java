/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Booleans;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.UDF0;
import com.linkedin.transport.api.udf.UDF1;
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.UDF3;
import com.linkedin.transport.api.udf.UDF4;
import com.linkedin.transport.api.udf.UDF5;
import com.linkedin.transport.api.udf.UDF6;
import com.linkedin.transport.api.udf.UDF7;
import com.linkedin.transport.api.udf.UDF8;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.typesystem.GenericTypeSignatureElement;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionKind;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.metadata.TypeVariableConstraint;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;
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

import static io.trino.metadata.Signature.*;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.*;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.*;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.util.Reflection.*;

// Suppressing argument naming convention for the evalInternal methods
@SuppressWarnings({"checkstyle:regexpsinglelinejava"})
public abstract class TrinoUDF extends SqlScalarFunction {

  private static final int DEFAULT_REFRESH_INTERVAL_DAYS = 1;
  private static final int JITTER_FACTOR = 50;  // to calculate jitter from delay

  protected TrinoUDF(UDF udf) {
    super(new FunctionMetadata(
            new Signature(
                    ((TopLevelUDF) udf).getFunctionName(),
                    getTypeVariableConstraintsForUdf(udf),
                    ImmutableList.of(),
                    parseTypeSignature(udf.getOutputParameterSignature(), ImmutableSet.of()),
                    udf.getInputParameterSignatures().stream()
                            .map(typeSignature -> parseTypeSignature(typeSignature, ImmutableSet.of()))
                            .collect(Collectors.toList()),
                    false),
            true,
            Booleans.asList(udf.getNullableArguments()).stream()
                    .map(FunctionArgumentDefinition::new)
                    .collect(Collectors.toList()),
            false,
            false,
            ((TopLevelUDF) udf).getFunctionDescription(),
            FunctionKind.SCALAR));
  }

  @VisibleForTesting
  static List<TypeVariableConstraint> getTypeVariableConstraintsForUdf(UDF udf) {
    Set<GenericTypeSignatureElement> genericTypes = new HashSet<>();
    for (String s : udf.getInputParameterSignatures()) {
      genericTypes.addAll(com.linkedin.transport.typesystem.TypeSignature.parse(s).getGenericTypeSignatureElements());
    }
    genericTypes.addAll(com.linkedin.transport.typesystem.TypeSignature.parse(udf.getOutputParameterSignature())
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
  public FunctionDependencyDeclaration getFunctionDependencies(FunctionBinding functionBinding) {
    FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();

    registerNestedDependencies(functionBinding.getBoundSignature().getReturnType(), builder);
    List<Type> argumentTypes = functionBinding.getBoundSignature().getArgumentTypes();
    argumentTypes.forEach(type -> registerNestedDependencies(type, builder));

    return builder.build();
  }

  @Override
  public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies) {
    TypeFactory typeFactory = new TrinoTypeFactory(functionBinding, functionDependencies);
    UDF udf = getUDF();
    udf.init(typeFactory);
    // Subtract a small jitter value so that refresh is triggered on first call
    // while ensuring subsequent calls do not happen at the same time across workers
    long initialJitter = getRefreshIntervalMillis() / JITTER_FACTOR;
    int initialJitterInt = initialJitter > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialJitter;
    AtomicLong requiredFilesNextRefreshTime = new AtomicLong(System.currentTimeMillis()
        - (new Random()).nextInt(initialJitterInt));
    boolean[] nullableArguments = udf.getAndCheckNullableArguments();

    return new ChoicesScalarFunctionImplementation(
        functionBinding,
        NULLABLE_RETURN,
        getNullConventionForArguments(nullableArguments),
        getMethodHandle(udf, functionBinding, nullableArguments, requiredFilesNextRefreshTime));
  }

  private MethodHandle getMethodHandle(UDF udf, FunctionBinding functionBinding, boolean[] nullableArguments,
      AtomicLong requiredFilesNextRefreshTime) {
    Type[] inputTypes = functionBinding.getBoundSignature().getArgumentTypes().toArray(new Type[0]);
    Type outputType = functionBinding.getBoundSignature().getReturnType();

    // Generic MethodHandle for eval where all arguments are of type Object
    Class<?>[] genericMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, true);
    MethodHandle genericMethodHandle =
        methodHandle(TrinoUDF.class, "evalInternal", genericMethodHandleArgumentTypes).bindTo(this);

    Class<?>[] specificMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, false);
    Class<?> specificMethodHandleReturnType = getJavaTypeForNullability(outputType, true);
    MethodType specificMethodType =
        MethodType.methodType(specificMethodHandleReturnType, specificMethodHandleArgumentTypes);

    // Specific MethodHandle required by trino where argument types map to the type signature
    MethodHandle specificMethodHandle = MethodHandles.explicitCastArguments(genericMethodHandle, specificMethodType);
    return MethodHandles.insertArguments(specificMethodHandle, 0, udf, inputTypes,
        outputType instanceof IntegerType, requiredFilesNextRefreshTime);
  }

  private List<InvocationConvention.InvocationArgumentConvention> getNullConventionForArguments(
      boolean[] nullableArguments) {
    return IntStream.range(0, nullableArguments.length)
        .mapToObj(idx -> nullableArguments[idx] ? BOXED_NULLABLE : NEVER_NULL)
        .collect(Collectors.toList());
  }

  private Object[] wrapArguments(UDF udf, Type[] types, Object[] arguments) {
    TypeFactory typeFactory = udf.getTypeFactory();
    Object[] data = new Object[arguments.length];
    // TODO: Reuse wrapper objects by creating them once upon initialization and reuse them here
    // along the same lines of what we do in Hive implementation.
    // JIRA: https://jira01.corp.linkedin.com:8443/browse/LIHADOOP-34894
    for (int i = 0; i < data.length; i++) {
      data[i] = TrinoConverters.toTransportData(arguments[i], types[i], typeFactory);
    }
    return data;
  }

  protected Object eval(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object... arguments) {
    Object[] args = wrapArguments(udf, types, arguments);
    if (requiredFilesNextRefreshTime.get() <= System.currentTimeMillis()) {
      String[] requiredFiles = getRequiredFiles(udf, args);
      processRequiredFiles(udf, requiredFiles, requiredFilesNextRefreshTime);
    }
    Object result;
    switch (args.length) {
      case 0:
        result = ((UDF0) udf).eval();
        break;
      case 1:
        result = ((UDF1) udf).eval(args[0]);
        break;
      case 2:
        result = ((UDF2) udf).eval(args[0], args[1]);
        break;
      case 3:
        result = ((UDF3) udf).eval(args[0], args[1], args[2]);
        break;
      case 4:
        result = ((UDF4) udf).eval(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        result = ((UDF5) udf).eval(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        result = ((UDF6) udf).eval(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        result = ((UDF7) udf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        result = ((UDF8) udf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new RuntimeException("eval not supported yet for UDF" + args.length);
    }

    return TrinoConverters.toPlatformData(result);
  }

  private String[] getRequiredFiles(UDF udf, Object[] args) {
    String[] requiredFiles;
    switch (args.length) {
      case 0:
        requiredFiles = ((UDF0) udf).getRequiredFiles();
        break;
      case 1:
        requiredFiles = ((UDF1) udf).getRequiredFiles(args[0]);
        break;
      case 2:
        requiredFiles = ((UDF2) udf).getRequiredFiles(args[0], args[1]);
        break;
      case 3:
        requiredFiles = ((UDF3) udf).getRequiredFiles(args[0], args[1], args[2]);
        break;
      case 4:
        requiredFiles = ((UDF4) udf).getRequiredFiles(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        requiredFiles = ((UDF5) udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        requiredFiles = ((UDF6) udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        requiredFiles = ((UDF7) udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5],
            args[6]);
        break;
      case 8:
        requiredFiles = ((UDF8) udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5],
            args[6], args[7]);
        break;
      default:
        throw new RuntimeException("getRequiredFiles not supported yet for UDF" + args.length);
    }
    return requiredFiles;
  }

  private synchronized void processRequiredFiles(UDF udf, String[] requiredFiles,
      AtomicLong requiredFilesNextRefreshTime) {
    if (requiredFilesNextRefreshTime.get() <= System.currentTimeMillis()) {
      try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
        String[] copiedFiles = new String[requiredFiles.length];
        FileSystemClient client = new FileSystemClient();
        for (int i = 0; i < requiredFiles.length; i++) {
          String localFilename = client.copyToLocalFile(requiredFiles[i]);
          copiedFiles[i] = localFilename;
        }
        udf.processRequiredFiles(copiedFiles);
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
    methodHandleArgumentTypes[0] = UDF.class;
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

  protected abstract UDF getUDF();

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6, Object arg7) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6,
        arg7);
  }

  public Object evalInternal(UDF udf, Type[] types, boolean isIntegerReturnType,
      AtomicLong requiredFilesNextRefreshTime, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5,
      Object arg6, Object arg7, Object arg8) {
    return eval(udf, types, isIntegerReturnType, requiredFilesNextRefreshTime, arg1, arg2, arg3, arg4, arg5, arg6,
        arg7, arg8);
  }
}
