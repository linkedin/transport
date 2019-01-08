/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

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
import com.linkedin.transport.hive.typesystem.HiveTypeInference;
import com.linkedin.transport.utils.FileSystemUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
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


/**
 * Base class for all Hive Standard UDFs. It provides a standard way of type validation, binding, and output type
 * inference through its initialize() method.
 */
public abstract class StdUdfWrapper extends GenericUDF {

  protected ObjectInspector[] _inputObjectInspectors;
  protected StdUDF _stdUdf;
  protected boolean _requiredFilesProcessed;
  protected StdFactory _stdFactory;
  private boolean[] _nullableArguments;
  private String[] _distributedCacheFiles;
  private StdData[] _args;

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
    hiveTypeInference.compile(
        arguments,
        getStdUdfImplementations(),
        getTopLevelUdfClass()
    );
    _inputObjectInspectors = hiveTypeInference.getInputDataTypes();
    _stdFactory = hiveTypeInference.getStdFactory();
    _stdUdf = hiveTypeInference.getStdUdf();
    _nullableArguments = _stdUdf.getAndCheckNullableArguments();
    _stdUdf.init(_stdFactory);
    _requiredFilesProcessed = false;
    createStdData();
    return hiveTypeInference.getOutputDataType();
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    if (_stdUdf == null) {
      return;
    }
    StdUdfWrapper newWrapper = (StdUdfWrapper) newInstance;
    newWrapper._inputObjectInspectors = _inputObjectInspectors;
    newWrapper._stdFactory = _stdFactory;
    newWrapper._stdUdf = _stdUdf;
    newWrapper._nullableArguments = _stdUdf.getAndCheckNullableArguments();
    newWrapper._stdUdf.init(_stdFactory);
    newWrapper._requiredFilesProcessed = false;
    newWrapper.createStdData();
  }

  protected boolean containsNullValuedNonNullableArgument(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].get() == null && !_nullableArguments[i]) {
        return true;
      }
    }
    return false;
  }

  protected StdData wrap(DeferredObject hiveDeferredObject, StdData stdData) {
    try {
      Object hiveObject = hiveDeferredObject.get();
      if (hiveObject != null) {
        ((PlatformData) stdData).setUnderlyingData(hiveObject);
        return stdData;
      } else {
        return null;
      }
    } catch (HiveException e) {
      throw new RuntimeException("Cannot extract Hive Object from Deferred Object");
    }
  }

  protected abstract List<? extends StdUDF> getStdUdfImplementations();

  protected abstract Class<? extends TopLevelStdUDF> getTopLevelUdfClass();

  protected void createStdData() {
    _args = new StdData[_inputObjectInspectors.length];
    for (int i = 0; i < _inputObjectInspectors.length; i++) {
      _args[i] = HiveWrapper.createStdData(null, _inputObjectInspectors[i], _stdFactory);
    }
  }

  private StdData[] wrapArguments(DeferredObject[] deferredObjects) {
    return IntStream.range(0, _args.length).mapToObj(i -> wrap(deferredObjects[i], _args[i])).toArray(StdData[]::new);
  }

  private StdData[] wrapConstants() {
    return Arrays.stream(_inputObjectInspectors).map(oi ->
        (oi instanceof ConstantObjectInspector) ? HiveWrapper.createStdData(
            ((ConstantObjectInspector) oi).getWritableConstantValue(),
            oi,
            _stdFactory
        ) : null
    ).toArray(StdData[]::new);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (containsNullValuedNonNullableArgument(arguments)) {
      return null;
    }
    if (!_requiredFilesProcessed) {
      processRequiredFiles();
    }
    StdData[] args = wrapArguments(arguments);
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

  @Override
  public String[] getRequiredFiles() {
    StdData[] args = wrapConstants();
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
    _distributedCacheFiles = Arrays.stream(requiredFiles).map(requiredFile -> {
      try {
        return FileSystemUtils.resolveLatest(requiredFile, FileSystemUtils.getHDFSFileSystem());
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
          }
      ).toArray(String[]::new);
      _stdUdf.processRequiredFiles(localFiles);
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
    return getStandardDisplayString(((TopLevelStdUDF) _stdUdf).getFunctionName(), children);
  }
}
