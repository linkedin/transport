/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

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
import com.linkedin.transport.hive.typesystem.HiveTypeInference;
import com.linkedin.transport.utils.FileSystemUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

/**
 * Base class for all Hive Standard UDFs. It provides a standard way of type validation, binding, and output type
 * inference through its initialize() method.
 */
public abstract class HiveUDF extends GenericUDF {

  protected ObjectInspector[] _inputObjectInspectors;
  protected UDF _udf;
  protected boolean _requiredFilesProcessed;
  protected TypeFactory _typeFactory;
  private boolean[] _nullableArguments;
  private String[] _distributedCacheFiles;
  private Object[] _args;
  private ObjectInspector _outputObjectInspector;

  /**
   * Given input object inspectors, this method matches them to the expected type signatures, and finds bindings to the
   * generic parameters. Once the generic parameter bindings are known, the method infers the output type (in the form
   * of object inspectors) by substituting the binding values in the output type signature.
   * signature and
   * @param arguments Input object inspectors of UDF parameters.
   * @return Inferred output object inspector.
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) {
    HiveTypeInference hiveTypeInference = new HiveTypeInference();
    hiveTypeInference.compile(arguments, getUdfImplementations(), getTopLevelUdfClass());
    _inputObjectInspectors = hiveTypeInference.getInputDataTypes();
    _typeFactory = hiveTypeInference.getTypeFactory();
    _udf = hiveTypeInference.getUdf();
    _nullableArguments = _udf.getAndCheckNullableArguments();
    _udf.init(_typeFactory);
    _requiredFilesProcessed = false;
    createTransportData();
    _outputObjectInspector = hiveTypeInference.getOutputDataType();
    return _outputObjectInspector;
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    if (_udf == null) {
      return;
    }
    HiveUDF newWrapper = (HiveUDF) newInstance;
    newWrapper._inputObjectInspectors = _inputObjectInspectors;
    newWrapper._typeFactory = _typeFactory;
    newWrapper._udf = _udf;
    newWrapper._nullableArguments = _udf.getAndCheckNullableArguments();
    newWrapper._udf.init(_typeFactory);
    newWrapper._requiredFilesProcessed = false;
    newWrapper.createTransportData();
  }

  protected boolean containsNullValuedNonNullableArgument(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].get() == null && !_nullableArguments[i]) {
        return true;
      }
    }
    return false;
  }

  protected boolean containsNullValuedNonNullableConstants() {
    for (int i = 0; i < _inputObjectInspectors.length; i++) {
      if (!_nullableArguments[i] && _inputObjectInspectors[i] instanceof ConstantObjectInspector
          && ((ConstantObjectInspector) _inputObjectInspectors[i]).getWritableConstantValue() == null) {
        return true;
      }
    }
    return false;
  }

  protected Object wrap(DeferredObject deferredObject, ObjectInspector inputObjectInspector, Object transportData) {
    try {
      Object hiveObject = deferredObject.get();
      if (inputObjectInspector instanceof BinaryObjectInspector) {
        return hiveObject == null ? null : ByteBuffer.wrap(
            ((BinaryObjectInspector) inputObjectInspector).getPrimitiveJavaObject(hiveObject)
        );
      }
      if (inputObjectInspector instanceof PrimitiveObjectInspector) {
        return ((PrimitiveObjectInspector) inputObjectInspector).getPrimitiveJavaObject(hiveObject);
      } else {
        if (hiveObject != null) {
          ((PlatformData) transportData).setUnderlyingData(hiveObject);
          return transportData;
        } else {
          return null;
        }
      }
    } catch (HiveException e) {
      throw new RuntimeException("Cannot extract Hive Object from Deferred Object");
    }
  }

  protected abstract List<? extends UDF> getUdfImplementations();

  protected abstract Class<? extends TopLevelUDF> getTopLevelUdfClass();

  protected void createTransportData() {
    _args = new Object[_inputObjectInspectors.length];
    for (int i = 0; i < _inputObjectInspectors.length; i++) {
      _args[i] = HiveConverters.toTransportData(null, _inputObjectInspectors[i], _typeFactory);
    }
  }

  private Object getPlatformData(Object transportData) {
    if (transportData == null) {
      return null;
    } else if (transportData instanceof Integer || transportData instanceof Long || transportData instanceof Boolean
      || transportData instanceof String || transportData instanceof Float || transportData instanceof Double
        || transportData instanceof ByteBuffer) {
      return HiveConverters.toPlatformData(transportData, _outputObjectInspector);
    } else {
      return ((PlatformData) transportData).getUnderlyingData();
    }
  }

  private Object[] wrapArguments(DeferredObject[] deferredObjects) {
    return IntStream.range(0, _args.length).mapToObj(
        i -> wrap(deferredObjects[i], _inputObjectInspectors[i], _args[i])
    ).toArray(Object[]::new);
  }

  private Object[] wrapConstants() {
    return Arrays.stream(_inputObjectInspectors)
        .map(oi -> (oi instanceof ConstantObjectInspector) ? HiveConverters.toTransportData(
            ((ConstantObjectInspector) oi).getWritableConstantValue(), oi, _typeFactory) : null)
        .toArray(Object[]::new);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (containsNullValuedNonNullableArgument(arguments)) {
      return null;
    }
    if (!_requiredFilesProcessed) {
      processRequiredFiles();
    }
    Object[] args = wrapArguments(arguments);
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
        throw new UnsupportedOperationException("eval not yet supported for UDF" + args.length);
    }
    return getPlatformData(result);
  }

  @Override
  public String[] getRequiredFiles() {
    if (containsNullValuedNonNullableConstants()) {
      return new String[]{};
    }
    Object[] args = wrapConstants();
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
        requiredFiles =
            ((UDF7) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        break;
      case 8:
        requiredFiles =
            ((UDF8) _udf).getRequiredFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6],
                args[7]);
        break;
      default:
        throw new UnsupportedOperationException("getRequiredFiles not yet supported for UDF" + args.length);
    }
    _distributedCacheFiles = Arrays.stream(requiredFiles).map(requiredFile -> {
      try {
        return FileSystemUtils.resolveLatest(requiredFile);
      } catch (IOException e) {
        throw new RuntimeException("Failed to resolve path: [" + requiredFile + "].", e);
      }
    }).toArray(String[]::new);

    return _distributedCacheFiles;
  }

  private synchronized void processRequiredFiles() {
    if (!_requiredFilesProcessed) {
      String[] localFiles = Arrays.stream(_distributedCacheFiles).map(distributedCacheFile -> {
        try {
          return getLocalFilePath(distributedCacheFile).toString();
        } catch (IOException e) {
          throw new RuntimeException("Failed to resolve path: [" + distributedCacheFile + "].", e);
        }
      }).toArray(String[]::new);
      _udf.processRequiredFiles(localFiles);
      _requiredFilesProcessed = true;
    }
  }

  public static Path getLocalFilePath(String origPath) throws FileNotFoundException {
    Path pathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(origPath));
    FileSystem fs = FileSystemUtils.getLocalFileSystem();

    // (1) check in hive.downloaded.resources.dir
    SessionState ss = SessionState.get();
    if (ss != null) {
      String downloadedResourcesDir = ss.getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR);
      Path resourcesDirPath = Path.getPathWithoutSchemeAndAuthority(
          new Path(downloadedResourcesDir, pathWithoutSchemeAndAuthority.getName()));
      try {
        if (fs.exists(resourcesDirPath)) {
          return resourcesDirPath;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      Path currentDirPath = new Path(pathWithoutSchemeAndAuthority.getName());
      if (fs.exists(currentDirPath)) {  // (2) check in current directory
        return currentDirPath;
      } else if (fs.exists(pathWithoutSchemeAndAuthority)) {  // (3) check full path on local disk
        return pathWithoutSchemeAndAuthority;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    throw new FileNotFoundException("Original file " + origPath + " not found locally in "
        + "hive.downloaded.resources.dir, the current directory, or at " + pathWithoutSchemeAndAuthority);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(((TopLevelUDF) _udf).getFunctionName(), children);
  }
}
