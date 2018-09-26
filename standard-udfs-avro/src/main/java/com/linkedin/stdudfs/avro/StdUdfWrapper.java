/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.avro;

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
import com.linkedin.stdudfs.avro.typesystem.AvroTypeInference;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.avro.Schema;


/**
 * Base class for all Avro Standard UDFs. It provides a standard way of type validation, binding, and output type
 * inference through its initialize() method.
 */
public abstract class StdUdfWrapper {

  protected Schema[] _inputSchemas;
  protected StdUDF _stdUdf;
  protected boolean _requiredFilesProcessed;
  protected StdFactory _stdFactory;
  private boolean[] _nullableArguments;
  private StdData[] _args;

  /**
   * Given input schemas, this method matches them to the expected type signatures, and finds bindings to the
   * generic parameters. Once the generic parameter bindings are known, the method infers the output type (in the form
   * of an Avro schema) by substituting the binding values in the output type signature.
   * signature and
   * @param arguments Input Avro Schemas of UDF parameters.
   * @return Inferred output Avro Schema.
   */
  public Schema initialize(Schema[] arguments) {
    AvroTypeInference avroTypeInference = new AvroTypeInference();
    avroTypeInference.compile(arguments, getStdUdfImplementations(), getTopLevelUdfClass());
    _inputSchemas = avroTypeInference.getInputDataTypes();
    _stdFactory = avroTypeInference.getStdFactory();
    _stdUdf = avroTypeInference.getStdUdf();
    _nullableArguments = _stdUdf.getAndCheckNullableArguments();
    _stdUdf.init(_stdFactory);
    _requiredFilesProcessed = false;
    createStdData();
    return avroTypeInference.getOutputDataType();
  }

  protected boolean containsNullValuedNonNullableArgument(Object[] arguments) {
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i] == null && !_nullableArguments[i]) {
        return true;
      }
    }
    return false;
  }

  protected StdData wrap(Object avroObject, StdData stdData) {
    if (avroObject != null) {
      ((PlatformData) stdData).setUnderlyingData(avroObject);
      return stdData;
    } else {
      return null;
    }
  }

  protected abstract List<? extends StdUDF> getStdUdfImplementations();

  protected abstract Class<? extends TopLevelStdUDF> getTopLevelUdfClass();

  protected void createStdData() {
    _args = new StdData[_inputSchemas.length];
    for (int i = 0; i < _inputSchemas.length; i++) {
      _args[i] = AvroWrapper.createStdData(null, _inputSchemas[i]);
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
      default:
        throw new UnsupportedOperationException("eval not yet supported for StdUDF" + args.length);
    }
    return result == null ? null : ((PlatformData) result).getUnderlyingData();
  }
}
