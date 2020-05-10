/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.PlatformData;
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
import com.linkedin.transport.avro.typesystem.AvroTypeInference;
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
  private Object[] _args;

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

  protected Object wrap(Object avroObject, Schema inputSchema, Object stdData) {
    switch (inputSchema.getType()) {
      case INT:
      case LONG:
      case BOOLEAN:
        return avroObject;
      case STRING:
        return avroObject == null? null : avroObject.toString();
      case ARRAY:
      case MAP:
      case RECORD:
        if (avroObject != null) {
          ((PlatformData) stdData).setUnderlyingData(avroObject);
          return stdData;
        } else {
          return null;
        }
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + inputSchema.getClass());
    }
  }

  protected abstract List<? extends StdUDF> getStdUdfImplementations();

  protected abstract Class<? extends TopLevelStdUDF> getTopLevelUdfClass();

  protected void createStdData() {
    _args = new Object[_inputSchemas.length];
    for (int i = 0; i < _inputSchemas.length; i++) {
      _args[i] = AvroWrapper.createStdData(null, _inputSchemas[i]);
    }
  }

  private Object[] wrapArguments(Object[] arguments) {
    return IntStream.range(0, _args.length).mapToObj(
        i -> wrap(arguments[i], _inputSchemas[i], _args[i])
    ).toArray(Object[]::new);
  }

  public Object evaluate(Object[] arguments) {
    if (containsNullValuedNonNullableArgument(arguments)) {
      return null;
    }
    Object[] args = wrapArguments(arguments);
    Object result;
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
    return AvroWrapper.getPlatformData(result);
  }
}
