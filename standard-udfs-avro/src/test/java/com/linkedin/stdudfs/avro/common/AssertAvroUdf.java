/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.avro.common;

import com.linkedin.stdudfs.avro.StdUdfWrapper;
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

  public static void assertFunction(StdUdfWrapper udf, Schema[] schemas, Object[] arguments, Object expected) {
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
