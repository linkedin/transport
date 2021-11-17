/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

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
import com.linkedin.transport.avro.typesystem.AvroTypeInference;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.avro.Schema;


/**
 * Base class for all Avro Standard UDFs. It provides a standard way of type validation, binding, and output type
 * inference through its initialize() method.
 */
public abstract class AvroUDF {

  protected Schema[] _inputSchemas;
  protected UDF _udf;
  protected boolean _requiredFilesProcessed;
  protected TypeFactory _typeFactory;
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
    avroTypeInference.compile(arguments, getUdfImplementations(), getTopLevelUdfClass());
    _inputSchemas = avroTypeInference.getInputDataTypes();
    _typeFactory = avroTypeInference.getTypeFactory();
    _udf = avroTypeInference.getUdf();
    _nullableArguments = _udf.getAndCheckNullableArguments();
    _udf.init(_typeFactory);
    _requiredFilesProcessed = false;
    createTransportData();
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

  protected Object wrap(Object avroObject, Schema inputSchema, Object data) {
    switch (inputSchema.getType()) {
      case INT:
      case LONG:
      case BOOLEAN:
        return avroObject;
      case STRING:
        return avroObject == null ? null : avroObject.toString();
      case ARRAY:
      case MAP:
      case RECORD:
        if (avroObject != null) {
          ((PlatformData) data).setUnderlyingData(avroObject);
          return data;
        } else {
          return null;
        }
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + inputSchema.getClass());
    }
  }

  protected abstract List<? extends UDF> getUdfImplementations();

  protected abstract Class<? extends TopLevelUDF> getTopLevelUdfClass();

  protected void createTransportData() {
    _args = new Object[_inputSchemas.length];
    for (int i = 0; i < _inputSchemas.length; i++) {
      _args[i] = AvroConverters.toTransportData(null, _inputSchemas[i]);
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
    return AvroConverters.toPlatformData(result);
  }
}
