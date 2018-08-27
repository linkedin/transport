package com.linkedin.stdudfs.presto;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.api.udf.StdUDF0;
import com.linkedin.stdudfs.api.udf.StdUDF1;
import com.linkedin.stdudfs.api.udf.StdUDF2;
import com.linkedin.stdudfs.api.udf.StdUDF3;
import com.linkedin.stdudfs.api.udf.StdUDF4;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
import com.linkedin.stdudfs.typesystem.GenericTypeSignatureElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang.ClassUtils;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;


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
      genericTypes.addAll(com.linkedin.stdudfs.typesystem.TypeSignature.parse(s).getGenericTypeSignatureElements());
    }
    genericTypes.addAll(com.linkedin.stdudfs.typesystem.TypeSignature.parse(stdUdf.getOutputParameterSignature())
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

    List<Type> argTypes = getPrestoTypes(stdUDF.getInputParameterSignatures(), typeManager, boundVariables);
    Type outputType = getPrestoType(stdUDF.getOutputParameterSignature(), typeManager, boundVariables);

    Class<?>[] methodHandleParameterTypes = getMethodHandleParameterTypes(argTypes, nullableArguments);

    MethodHandle methodHandle = methodHandle(getClass(),
        ((TopLevelStdUDF) stdUDF).getFunctionName() + "_" + getMethodHandleJavaType(outputType, true,
            0).getSimpleName(), methodHandleParameterTypes).bindTo(this);
    methodHandle = MethodHandles.insertArguments(methodHandle, 0, stdUDF);
    methodHandle = MethodHandles.insertArguments(methodHandle, 0, argTypes.toArray());

    List<ScalarFunctionImplementation.ArgumentProperty> argsNullConvention = IntStream.range(0, nullableArguments.length)
        .mapToObj(idx -> ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty(
            nullableArguments[idx] ? ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE
                : ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL))
        .collect(Collectors.toList());

    return new ScalarFunctionImplementation(true, argsNullConvention, methodHandle, isDeterministic());
  }

  private StdData[] wrapArguments(StdUDF stdUDF, Object[] arguments) {
    StdFactory stdFactory = stdUDF.getStdFactory();
    StdData[] stdData = new StdData[arguments.length / 2];
    // TODO: Reuse wrapper objects by creating them once upon initialization and reuse them here
    // along the same lines of what we do in Hive implementation.
    // JIRA: https://jira01.corp.linkedin.com:8443/browse/LIHADOOP-34894
    for (int i = 0; i < stdData.length; i++) {
      stdData[i] = PrestoWrapper.createStdData(arguments[stdData.length + i], (Type) arguments[i], stdFactory);
    }
    return stdData;
  }

  protected Object eval(StdUDF stdUDF, Object... arguments) {
    StdData[] args = wrapArguments(stdUDF, arguments);
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
      default:
        throw new RuntimeException("eval not supported yet for StdUDF" + args.length);
    }
    return result == null ? null : ((PlatformData) result).getUnderlyingData();
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

  private List<Type> getPrestoTypes(List<String> parameterSignatures, TypeManager typeManager,
      BoundVariables boundVariables) {
    return parameterSignatures.stream()
        .map(p -> getPrestoType(p, typeManager, boundVariables))
        .collect(Collectors.toList());
  }

  private Type getPrestoType(String parameterSignature, TypeManager typeManager, BoundVariables boundVariables) {
    return typeManager.getType(
        applyBoundVariables(TypeSignature.parseTypeSignature(parameterSignature), boundVariables));
  }

  private Class<?>[] getMethodHandleParameterTypes(List<Type> argTypes, boolean[] nullableArguments) {
    Class<?>[] methodHandleParameterTypes = new Class<?>[argTypes.size() * 2 + 1];
    methodHandleParameterTypes[0] = StdUDF.class;
    for (int i = 0; i < argTypes.size(); i++) {
      methodHandleParameterTypes[i + 1] = Type.class;
      methodHandleParameterTypes[i + 1 + argTypes.size()] =
          getMethodHandleJavaType(argTypes.get(i), nullableArguments[i], i);
    }
    return methodHandleParameterTypes;
  }

  protected abstract StdUDF getStdUDF();
}
