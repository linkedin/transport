/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.PlatformData;
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
import com.linkedin.transport.test.generic.typesystem.GenericTypeInference;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.utils.FileSystemUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.Path;


/**
 * Base class for all Test Transport UDFs. It provides a standard way of type validation, binding, and output type
 * inference through its initialize() method.
 */
public class GenericStdUDFWrapper {

  protected TestType[] _inputTypes;
  protected UDF _udf;
  protected boolean _requiredFilesProcessed;
  protected TypeFactory _typeFactory;
  private boolean[] _nullableArguments;
  private Object[] _args;
  private Class<? extends TopLevelUDF> _topLevelUdfClass;
  private List<Class<? extends UDF>> _stdUdfImplementations;
  private String[] _localFiles;

  public GenericStdUDFWrapper(Class<? extends TopLevelUDF> topLevelUdfClass,
      List<Class<? extends UDF>> stdUdfImplementations) {
    _topLevelUdfClass = topLevelUdfClass;
    _stdUdfImplementations = stdUdfImplementations;
  }

  /**
   * Given input {@link TestType}s, this method matches them to the expected {@link TestType}, and finds bindings to the
   * generic parameters. Once the generic parameter bindings are known, the method infers the output type (in the form
   * of a {@link TestType}) by substituting the binding values in the output {@link TestType}.
   *
   * @param arguments Input {@link TestType} of UDF parameters.
   * @return Inferred output {@link TestType}.
   */
  public TestType initialize(TestType[] arguments) {
    GenericTypeInference genericTypeInference = new GenericTypeInference();
    genericTypeInference.compile(arguments, getStdUdfImplementations(), getTopLevelUdfClass());
    _inputTypes = genericTypeInference.getInputDataTypes();
    _typeFactory = genericTypeInference.getStdFactory();
    _udf = genericTypeInference.getStdUdf();
    _nullableArguments = _udf.getAndCheckNullableArguments();
    _udf.init(_typeFactory);
    _requiredFilesProcessed = false;
    createStdData();
    return genericTypeInference.getOutputDataType();
  }

  protected boolean containsNullValuedNonNullableArgument(Object[] arguments) {
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i] == null && !_nullableArguments[i]) {
        return true;
      }
    }
    return false;
  }

  protected Object wrap(Object argument, Object stdData) {
    if (argument == null) {
      return null;
    } else {
      if (argument instanceof Integer || argument instanceof Long || argument instanceof Boolean
          || argument instanceof String || argument instanceof Double || argument instanceof Float
          || argument instanceof ByteBuffer) {
        return argument;
      } else {
        ((PlatformData) stdData).setUnderlyingData(argument);
        return stdData;
      }
    }
  }

  protected List<? extends UDF> getStdUdfImplementations() {
    return _stdUdfImplementations.stream().map(stdUdfClass -> {
      try {
        return stdUdfClass.getConstructor().newInstance();
      } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
    return _topLevelUdfClass;
  }

  protected void createStdData() {
    _args = new Object[_inputTypes.length];
    for (int i = 0; i < _inputTypes.length; i++) {
      _args[i] = GenericWrapper.createStdData(null, _inputTypes[i]);
    }
  }

  private Object[] wrapArguments(Object[] arguments) {
    return IntStream.range(0, _args.length).mapToObj(i -> wrap(arguments[i], _args[i])).toArray(Object[]::new);
  }

  public Object evaluate(Object[] arguments) {
    if (containsNullValuedNonNullableArgument(arguments)) {
      return null;
    }
    Object[] args = wrapArguments(arguments);
    if (!_requiredFilesProcessed) {
      String[] requiredFiles = getRequiredFiles(args);
      processRequiredFiles(requiredFiles);
    }
    Object result;
    switch (args.length) {
      case 0:
        result = ((UDF0) _udf).eval();
        break;
      case 1:
        result = ((UDF1) _udf).eval(args[0]);
        break;
      case 2:
        result = ((UDF2) _udf).eval(args[0], args[1]);
        break;
      case 3:
        result = ((UDF3) _udf).eval(args[0], args[1], args[2]);
        break;
      case 4:
        result = ((UDF4) _udf).eval(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        result = ((UDF5) _udf).eval(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        result = ((UDF6) _udf).eval(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        result = ((UDF7) _udf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        result = ((UDF8) _udf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new UnsupportedOperationException("eval not yet supported for StdUDF" + args.length);
    }
    return GenericWrapper.getPlatformData(result);
  }

  public String[] getRequiredFiles(Object[] args) {
    String[] requiredFiles;
    switch (args.length) {
      case 0:
        requiredFiles = ((UDF0) _udf).getRequiredFiles();
        break;
      case 1:
        requiredFiles = ((UDF1) _udf).getRequiredFiles(args[0]);
        break;
      case 2:
        requiredFiles = ((UDF2) _udf).getRequiredFiles(args[0], args[1]);
        break;
      case 3:
        requiredFiles = ((UDF3) _udf).getRequiredFiles(args[0], args[1], args[2]);
        break;
      case 4:
        requiredFiles = ((UDF4) _udf).getRequiredFiles(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        requiredFiles = ((UDF5) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        requiredFiles = ((UDF6) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        requiredFiles = ((UDF7) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        requiredFiles = ((UDF8) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new UnsupportedOperationException("getRequiredFiles not yet supported for StdUDF" + args.length);
    }
    _localFiles = Arrays.stream(requiredFiles).map(requiredFile -> {
      try {
        return FileSystemUtils.resolveLatest(requiredFile);
      } catch (IOException e) {
        throw new RuntimeException("Failed to resolve path: [" + requiredFile + "].", e);
      }
    }).toArray(String[]::new);

    return _localFiles;
  }

  private synchronized void processRequiredFiles(String[] requiredFiles) {
    if (!_requiredFilesProcessed) {
      _udf.processRequiredFiles(Arrays.stream(requiredFiles)
          .map(path -> Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString())
          .toArray(String[]::new));
      _requiredFilesProcessed = true;
    }
  }
}
