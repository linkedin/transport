/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

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
import com.linkedin.transport.test.generic.typesystem.GenericTypeInference;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.utils.FileSystemUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
  protected StdUDF _stdUdf;
  protected boolean _requiredFilesProcessed;
  protected StdFactory _stdFactory;
  private boolean[] _nullableArguments;
  private StdData[] _args;
  private Class<? extends TopLevelStdUDF> _topLevelUdfClass;
  private List<Class<? extends StdUDF>> _stdUdfImplementations;
  private String[] _localFiles;

  public GenericStdUDFWrapper(Class<? extends TopLevelStdUDF> topLevelUdfClass,
      List<Class<? extends StdUDF>> stdUdfImplementations) {
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
    _stdFactory = genericTypeInference.getStdFactory();
    _stdUdf = genericTypeInference.getStdUdf();
    _nullableArguments = _stdUdf.getAndCheckNullableArguments();
    _stdUdf.init(_stdFactory);
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

  protected StdData wrap(Object argument, StdData stdData) {
    if (argument != null) {
      ((PlatformData) stdData).setUnderlyingData(argument);
      return stdData;
    } else {
      return null;
    }
  }

  protected List<? extends StdUDF> getStdUdfImplementations() {
    return _stdUdfImplementations.stream().map(stdUdfClass -> {
      try {
        return stdUdfClass.getConstructor().newInstance();
      } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
    return _topLevelUdfClass;
  }

  protected void createStdData() {
    _args = new StdData[_inputTypes.length];
    for (int i = 0; i < _inputTypes.length; i++) {
      _args[i] = GenericWrapper.createStdData(null, _inputTypes[i]);
    }
  }

  private StdData[] wrapArguments(Object[] arguments) {
    return IntStream.range(0, _args.length).mapToObj(i -> wrap(arguments[i], _args[i])).toArray(StdData[]::new);
  }

  public Object evaluate(Object[] arguments) {
    if (containsNullValuedNonNullableArgument(arguments)) {
      return null;
    }
    StdData[] args = wrapArguments(arguments);
    if (!_requiredFilesProcessed) {
      String[] requiredFiles = getRequiredFiles(args);
      processRequiredFiles(requiredFiles);
    }
    StdData result;
    switch (args.length) {
      case 0:
        result = ((StdUDF0) _stdUdf).eval();
        break;
      case 1:
        result = ((StdUDF1) _stdUdf).eval(args[0]);
        break;
      case 2:
        result = ((StdUDF2) _stdUdf).eval(args[0], args[1]);
        break;
      case 3:
        result = ((StdUDF3) _stdUdf).eval(args[0], args[1], args[2]);
        break;
      case 4:
        result = ((StdUDF4) _stdUdf).eval(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        result = ((StdUDF5) _stdUdf).eval(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        result = ((StdUDF6) _stdUdf).eval(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        result = ((StdUDF7) _stdUdf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        result = ((StdUDF8) _stdUdf).eval(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new UnsupportedOperationException("eval not yet supported for StdUDF" + args.length);
    }
    return result == null ? null : ((PlatformData) result).getUnderlyingData();
  }

  public String[] getRequiredFiles(StdData[] args) {
    String[] requiredFiles;
    switch (args.length) {
      case 0:
        requiredFiles = ((StdUDF0) _stdUdf).getRequiredFiles();
        break;
      case 1:
        requiredFiles = ((StdUDF1) _stdUdf).getRequiredFiles(args[0]);
        break;
      case 2:
        requiredFiles = ((StdUDF2) _stdUdf).getRequiredFiles(args[0], args[1]);
        break;
      case 3:
        requiredFiles = ((StdUDF3) _stdUdf).getRequiredFiles(args[0], args[1], args[2]);
        break;
      case 4:
        requiredFiles = ((StdUDF4) _stdUdf).getRequiredFiles(args[0], args[1], args[2], args[3]);
        break;
      case 5:
        requiredFiles = ((StdUDF5) _stdUdf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4]);
        break;
      case 6:
        requiredFiles = ((StdUDF6) _stdUdf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5]);
        break;
      case 7:
        requiredFiles = ((StdUDF7) _stdUdf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        requiredFiles = ((StdUDF8) _stdUdf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        break;
      default:
        throw new UnsupportedOperationException("getRequiredFiles not yet supported for StdUDF" + args.length);
    }
    _localFiles = Arrays.stream(requiredFiles).map(requiredFile -> {
      try {
        return FileSystemUtils.resolveLatest(requiredFile, FileSystemUtils.getLocalFileSystem());
      } catch (IOException e) {
        throw new RuntimeException("Failed to resolve path: [" + requiredFile + "].", e);
      }
    }).toArray(String[]::new);

    return _localFiles;
  }

  private synchronized void processRequiredFiles(String[] requiredFiles) {
    if (!_requiredFilesProcessed) {
      _stdUdf.processRequiredFiles(Arrays.stream(requiredFiles)
          .map(path -> Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString())
          .toArray(String[]::new));
      _requiredFilesProcessed = true;
    }
  }
}
