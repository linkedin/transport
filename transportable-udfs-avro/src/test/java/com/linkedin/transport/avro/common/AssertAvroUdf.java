/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.common;

import com.linkedin.transport.avro.AvroUDF;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;

import static org.apache.avro.Schema.Type.*;


public class AssertAvroUdf {
  private AssertAvroUdf() {
  }

  public static void assertFunction(AvroUDF udf, Schema[] schemas, Object[] arguments, Object expected) {
    udf.initialize(schemas);
    Object result = udf.evaluate(IntStream.range(0, arguments.length)
        .mapToObj(i -> getAvroObject(arguments[i], schemas[i]))
        .toArray(Object[]::new));
    Assert.assertEquals(getJavaObject(result), expected);
  }

  private static Object getAvroObject(Object javaObject, Schema schema) {
    if (javaObject == null) {
      return null;
    }
    Object avroObject;
    if (schema.getType() == STRING) {
      avroObject = new Utf8((String) javaObject);
    } else if (schema.getType() == ARRAY) {
      avroObject = new GenericData.Array(((Collection<Object>) javaObject).size(), schema);
      for (Object arrayElement : (Collection<Object>) javaObject) {
        ((GenericArray) avroObject).add(getAvroObject(arrayElement, schema.getElementType()));
      }
    } else if (schema.getType() == MAP) {
      avroObject = new LinkedHashMap<>();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) javaObject).entrySet()) {
        ((Map<Object, Object>) avroObject).put(getAvroObject(entry.getKey(), Schema.create(STRING)),
            getAvroObject(entry.getValue(), schema.getValueType()));
      }
    } else if (schema.getType() == RECORD) {
      avroObject = new GenericData.Record(schema);
      for (int j = 0; j < schema.getFields().size(); j++) {
        ((GenericRecord) avroObject).put(j,
            getAvroObject(((Object[]) javaObject)[j], schema.getFields().get(j).schema()));
      }
    } else {
      avroObject = javaObject;
    }
    return avroObject;
  }

  private static Object getJavaObject(Object avroObject) {
    Object javaObject;
    if (avroObject instanceof Utf8) {
      javaObject = avroObject.toString();
    } else if (avroObject instanceof GenericArray) {
      javaObject = ((GenericArray<Object>) avroObject).stream()
          .map(element -> getJavaObject(element))
          .collect(Collectors.toList());
    } else if (avroObject instanceof Map) {
      javaObject = new LinkedHashMap<>();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) avroObject).entrySet()) {
        ((Map<Object, Object>) javaObject).put(getJavaObject(entry.getKey()), getJavaObject(entry.getValue()));
      }
    } else if (avroObject instanceof GenericRecord) {
      int numberOfFields = ((GenericRecord) avroObject).getSchema().getFields().size();
      javaObject = new ArrayList(numberOfFields);
      for (int i = 0; i < numberOfFields; i++) {
        ((ArrayList) javaObject).add(getJavaObject(((GenericRecord) avroObject).get(i)));
      }
    } else {
      javaObject = avroObject;
    }
    return javaObject;
  }
}
