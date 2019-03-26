/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ClassUtils;

import static com.facebook.presto.metadata.Signature.*;
import static com.facebook.presto.metadata.SignatureBinder.*;
import static com.facebook.presto.spi.type.TypeSignature.*;
import static com.facebook.presto.util.Reflection.*;

// Suppressing argument naming convention for the evalInternal methods
@SuppressWarnings({"checkstyle:regexpsinglelinejava"})
public abstract class StdUdfWrapper extends SqlScalarFunction {

  private static final int DEFAULT_REFRESH_INTERVAL_DAYS = 1;
  private static final int JITTER_FACTOR = 50;  // to calculate jitter from delay
  private volatile long _requiredFilesNextRefreshTime = Long.MAX_VALUE; // Will be set in specialize()
  private String _functionDescription;

  protected StdUdfWrapper(StdUDF stdUDF) {
    super(new Signature(((TopLevelStdUDF) stdUDF).getFunctionName(), FunctionKind.SCALAR,
        getTypeVariableConstraintsForStdUdf(stdUDF), ImmutableList.of(),
        parseTypeSignature(stdUDF.getOutputParameterSignature()), stdUDF.getInputParameterSignatures()
        .stream()
        .map(TypeSignature::parseTypeSignature)
        .collect(Collectors.toList()), false));
    _functionDescription = ((TopLevelStdUDF) stdUDF).getFunctionDescription();
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

  @Override
  public boolean isHidden() {
    return false;
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public String getDescription() {
    return _functionDescription;
  }

  @Override
  public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager,
      FunctionRegistry functionRegistry) {
    StdFactory stdFactory = new PrestoFactory(boundVariables, typeManager, functionRegistry);
    StdUDF stdUDF = getStdUDF();
    stdUDF.init(stdFactory);
    // Subtract a small jitter value so that refresh is triggered on first call
    // Do not add extra delay, if refresh time was set to lower value by an earlier specialize
    long initialJitter = getRefreshIntervalMillis() / JITTER_FACTOR;
    int initialJitterInt = initialJitter > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialJitter;
    _requiredFilesNextRefreshTime =
        Math.min(_requiredFilesNextRefreshTime, System.currentTimeMillis() - (new Random()).nextInt(initialJitterInt));
    boolean[] nullableArguments = stdUDF.getAndCheckNullableArguments();

    return new ScalarFunctionImplementation(true, getNullConventionForArguments(nullableArguments),
        getMethodHandle(stdUDF, typeManager, boundVariables, nullableArguments), isDeterministic());
  }

  private MethodHandle getMethodHandle(StdUDF stdUDF, TypeManager typeManager, BoundVariables boundVariables,
      boolean[] nullableArguments) {
    Type[] inputTypes = getPrestoTypes(stdUDF.getInputParameterSignatures(), typeManager, boundVariables);
    Type outputType = getPrestoType(stdUDF.getOutputParameterSignature(), typeManager, boundVariables);

    // Generic MethodHandle for eval where all arguments are of type Object
    Class<?>[] genericMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, true);
    MethodHandle genericMethodHandle =
        methodHandle(StdUdfWrapper.class, "evalInternal", genericMethodHandleArgumentTypes).bindTo(this);

    Class<?>[] specificMethodHandleArgumentTypes = getMethodHandleArgumentTypes(inputTypes, nullableArguments, false);
    Class<?> specificMethodHandleReturnType = getMethodHandleJavaType(outputType, true, 0);
    MethodType specificMethodType =
        MethodType.methodType(specificMethodHandleReturnType, specificMethodHandleArgumentTypes);

    // Specific MethodHandle required by presto where argument types map to the type signature
    MethodHandle specificMethodHandle = MethodHandles.explicitCastArguments(genericMethodHandle, specificMethodType);
    return MethodHandles.insertArguments(specificMethodHandle, 0, stdUDF, inputTypes,
        outputType instanceof IntegerType);
  }

  private List<ScalarFunctionImplementation.ArgumentProperty> getNullConventionForArguments(
      boolean[] nullableArguments) {
    return IntStream.range(0, nullableArguments.length)
        .mapToObj(idx -> ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty(
            nullableArguments[idx] ? ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE
                : ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL))
        .collect(Collectors.toList());
  }

  private StdData[] wrapArguments(StdUDF stdUDF, Type[] types, Object[] arguments) {
    StdFactory stdFactory = stdUDF.getStdFactory();
    StdData[] stdData = new StdData[arguments.length];
    // TODO: Reuse wrapper objects by creating them once upon initialization and reuse them here
    // along the same lines of what we do in Hive implementation.
    // JIRA: https://jira01.corp.linkedin.com:8443/browse/LIHADOOP-34894
    for (int i = 0; i < stdData.length; i++) {
      stdData[i] = PrestoWrapper.createStdData(arguments[i], types[i], stdFactory);
    }
    return stdData;
  }

  protected Object eval(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object... arguments) {
    StdData[] args = wrapArguments(stdUDF, types, arguments);
    if (_requiredFilesNextRefreshTime < System.currentTimeMillis()) {
      String[] requiredFiles = getRequiredFiles(stdUDF, args);
      processRequiredFiles(stdUDF, requiredFiles);
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

  private synchronized void processRequiredFiles(StdUDF stdUDF, String[] requiredFiles) {
    if (_requiredFilesNextRefreshTime < System.currentTimeMillis()) {
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
            (System.currentTimeMillis() - _requiredFilesNextRefreshTime) / (double) getRefreshIntervalMillis());
        _requiredFilesNextRefreshTime += getRefreshIntervalMillis() * Math.max(1, refreshIntervalFactor);
      }
    }
  }

  private Class getMethodHandleJavaType(Type prestoType, boolean nullableArgument, int idx) {
    Class prestoTypeJavaType = prestoType.getJavaType();
    Class methodHandleJavaType;
    if (prestoType.getJavaType() == void.class) {
      if (nullableArgument) {
        methodHandleJavaType = Void.class;
      } else {
        throw new RuntimeException("Error processing argument #" + idx + " of function " + getSignature().getName()
            + ". Cannot process a non-nullable argument with an unknown type."
            + " If type is unknown, argument must be nullable");
      }
    } else {
      if (nullableArgument) {
        methodHandleJavaType =
            prestoTypeJavaType.isPrimitive() ? ClassUtils.primitiveToWrapper(prestoTypeJavaType) : prestoTypeJavaType;
      } else {
        methodHandleJavaType = prestoTypeJavaType;
      }
    }
    return methodHandleJavaType;
  }

  private Type[] getPrestoTypes(List<String> parameterSignatures, TypeManager typeManager,
      BoundVariables boundVariables) {
    return parameterSignatures.stream().map(p -> getPrestoType(p, typeManager, boundVariables)).toArray(Type[]::new);
  }

  private Type getPrestoType(String parameterSignature, TypeManager typeManager, BoundVariables boundVariables) {
    return typeManager.getType(
        applyBoundVariables(TypeSignature.parseTypeSignature(parameterSignature), boundVariables));
  }

  private Class<?>[] getMethodHandleArgumentTypes(Type[] argTypes, boolean[] nullableArguments,
      boolean useObjectForArgumentType) {
    Class<?>[] methodHandleArgumentTypes = new Class<?>[argTypes.length + 3];
    methodHandleArgumentTypes[0] = StdUDF.class;
    methodHandleArgumentTypes[1] = Type[].class;
    methodHandleArgumentTypes[2] = boolean.class;
    for (int i = 0; i < argTypes.length; i++) {
      if (useObjectForArgumentType) {
        methodHandleArgumentTypes[i + 3] = Object.class;
      } else {
        methodHandleArgumentTypes[i + 3] = getMethodHandleJavaType(argTypes[i], nullableArguments[i], i);
      }
    }
    return methodHandleArgumentTypes;
  }

  protected abstract StdUDF getStdUDF();

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType) {
    return eval(stdUDF, types, isIntegerReturnType);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1) {
    return eval(stdUDF, types, isIntegerReturnType, arg1);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3, Object arg4) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3, arg4);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3, Object arg4, Object arg5) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3, arg4, arg5);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3, Object arg4, Object arg5, Object arg6) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3, arg4, arg5, arg6);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3, Object arg4, Object arg5, Object arg6, Object arg7) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }

  public Object evalInternal(StdUDF stdUDF, Type[] types, boolean isIntegerReturnType, Object arg1, Object arg2,
      Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, Object arg8) {
    return eval(stdUDF, types, isIntegerReturnType, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
  }
}
