/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArray;
import com.linkedin.transport.avro.data.AvroBinary;
import com.linkedin.transport.avro.data.AvroBoolean;
import com.linkedin.transport.avro.data.AvroDouble;
import com.linkedin.transport.avro.data.AvroFloat;
import com.linkedin.transport.avro.data.AvroInteger;
import com.linkedin.transport.avro.data.AvroLong;
import com.linkedin.transport.avro.data.AvroMap;
import com.linkedin.transport.avro.data.AvroString;
import com.linkedin.transport.avro.data.AvroStruct;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroBinaryType;
import com.linkedin.transport.avro.types.AvroBooleanType;
import com.linkedin.transport.avro.types.AvroDoubleType;
import com.linkedin.transport.avro.types.AvroFloatType;
import com.linkedin.transport.avro.types.AvroIntegerType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroStringType;
import com.linkedin.transport.avro.types.AvroStructType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestAvroWrapper {

  private Schema createSchema(String typeName) {
    return createSchema("testField", typeName);
  }

  private Schema createSchema(String fieldName, String typeName) {
    return new Schema.Parser().parse(
        String.format("{\"name\": \"%s\",\"type\": %s}", fieldName, typeName));
  }

  private void testSimpleType(String typeName, Class<? extends StdType> expectedAvroTypeClass,
      Object testData, Class<? extends StdData> expectedDataClass) {
    Schema avroSchema = createSchema(String.format("\"%s\"", typeName));

    StdType stdType = AvroWrapper.createStdType(avroSchema);
    assertTrue(expectedAvroTypeClass.isAssignableFrom(stdType.getClass()));
    assertEquals(avroSchema, stdType.underlyingType());

    StdData stdData = AvroWrapper.createStdData(testData, avroSchema);
    assertNotNull(stdData);
    assertTrue(expectedDataClass.isAssignableFrom(stdData.getClass()));
    if ("string".equals(typeName)) {
      // Use String values for equality assertion as we support both Utf8 and String input types
      assertEquals(testData.toString(), ((PlatformData) stdData).getUnderlyingData().toString());
    } else {
      assertEquals(testData, ((PlatformData) stdData).getUnderlyingData());
    }
  }

  @Test
  public void testBooleanType() {
    testSimpleType("boolean", AvroBooleanType.class, true, AvroBoolean.class);
  }

  @Test
  public void testIntegerType() {
    testSimpleType("int", AvroIntegerType.class, 1, AvroInteger.class);
  }

  @Test
  public void testLongType() {
    testSimpleType("long", AvroLongType.class, 1L, AvroLong.class);
  }

  @Test
  public void testFloatType() {
    testSimpleType("float", AvroFloatType.class, 1.0f, AvroFloat.class);
  }

  @Test
  public void testDoubleType() {
    testSimpleType("double", AvroDoubleType.class, 1.0, AvroDouble.class);
  }

  @Test
  public void testStringType() {
    testSimpleType("string", AvroStringType.class, new Utf8("foo"), AvroString.class);
    testSimpleType("string", AvroStringType.class, "foo", AvroString.class);
  }

  @Test
  public void testBinaryType() {
    testSimpleType("bytes", AvroBinaryType.class, ByteBuffer.wrap("bar".getBytes()), AvroBinary.class);
  }

  @Test
  public void testArrayType() {
    Schema elementType = createSchema("\"int\"");
    Schema arraySchema = Schema.createArray(elementType);

    StdType stdArrayType = AvroWrapper.createStdType(arraySchema);
    assertTrue(stdArrayType instanceof AvroArrayType);
    assertEquals(arraySchema, stdArrayType.underlyingType());
    assertEquals(elementType, ((AvroArrayType) stdArrayType).elementType().underlyingType());

    GenericArray<Integer> value = new GenericData.Array<>(arraySchema, Arrays.asList(1, 2));
    StdData stdArrayData = AvroWrapper.createStdData(value, arraySchema);
    assertTrue(stdArrayData instanceof AvroArray);
    assertEquals(2, ((AvroArray) stdArrayData).size());
    assertEquals(value, ((AvroArray) stdArrayData).getUnderlyingData());
  }

  @Test
  public void testMapType() {
    Schema valueType = createSchema("\"long\"");
    Schema mapSchema = Schema.createMap(valueType);

    StdType stdMapType = AvroWrapper.createStdType(mapSchema);
    assertTrue(stdMapType instanceof AvroMapType);
    assertEquals(mapSchema, stdMapType.underlyingType());
    assertEquals(valueType, ((AvroMapType) stdMapType).valueType().underlyingType());

    Map<String, Long> value = ImmutableMap.of("foo", 1L, "bar", 2L);
    StdData stdMapData = AvroWrapper.createStdData(value, mapSchema);
    assertTrue(stdMapData instanceof AvroMap);
    assertEquals(2, ((AvroMap) stdMapData).size());
    assertEquals(value, ((AvroMap) stdMapData).getUnderlyingData());
  }

  @Test
  public void testRecordType() {
    Schema field1 = createSchema("field1", "\"int\"");
    Schema field2 = createSchema("field2", "\"double\"");
    Schema structSchema = Schema.createRecord(ImmutableList.of(
        new Schema.Field("field1", field1, null, null),
        new Schema.Field("field2", field2, null, null)
    ));

    StdType stdStructType = AvroWrapper.createStdType(structSchema);
    assertTrue(stdStructType instanceof AvroStructType);
    assertEquals(structSchema, stdStructType.underlyingType());
    assertEquals(field1, ((AvroStructType) stdStructType).fieldTypes().get(0).underlyingType());
    assertEquals(field2, ((AvroStructType) stdStructType).fieldTypes().get(1).underlyingType());

    GenericRecord value = new GenericData.Record(structSchema);
    value.put("field1", 1);
    value.put("field2", 2.0);
    StdData stdStructData = AvroWrapper.createStdData(value, structSchema);
    assertTrue(stdStructData instanceof AvroStruct);
    AvroStruct avroStruct = (AvroStruct) stdStructData;
    assertEquals(2, avroStruct.fields().size());
    assertEquals(value, avroStruct.getUnderlyingData());
    assertEquals(1, ((PlatformData) avroStruct.getField("field1")).getUnderlyingData());
    assertEquals(2.0, ((PlatformData) avroStruct.getField("field2")).getUnderlyingData());
  }

  @Test
  public void testValidUnionType() {
    Schema nonNullType = createSchema("\"long\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType, Schema.create(Schema.Type.NULL)));

    StdType stdLongType = AvroWrapper.createStdType(unionSchema);
    assertTrue(stdLongType instanceof AvroLongType);
    assertEquals(nonNullType, stdLongType.underlyingType());

    StdData stdLongData = AvroWrapper.createStdData(1L, unionSchema);
    assertTrue(stdLongData instanceof AvroLong);
    assertEquals(1L, ((AvroLong) stdLongData).get());

    StdData stdNullData = AvroWrapper.createStdData(null, unionSchema);
    assertNull(stdNullData);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidUnionType1() {
    Schema nonNullType = createSchema("\"long\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType));
    AvroWrapper.createStdType(unionSchema);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidUnionType2() {
    Schema nonNullType1 = createSchema("\"long\"");
    Schema nonNullType2 = createSchema("\"int\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType1, nonNullType2));
    AvroWrapper.createStdData(1L, unionSchema);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUnsupportedType1() {
    AvroWrapper.createStdData("test", Schema.create(Schema.Type.ENUM));
  }

  @Test
  public void testStructWithSimpleUnionField() {
    Schema field1 = createSchema("field1", "\"int\"");
    Schema nonNullableField2 = createSchema("field2", "\"double\"");
    Schema field2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), nonNullableField2));

    Schema structSchema = Schema.createRecord(ImmutableList.of(
        new Schema.Field("field1", field1, null, null),
        new Schema.Field("field2", field2, null, null)
    ));

    GenericRecord record1 = new GenericData.Record(structSchema);
    record1.put("field1", 1);
    record1.put("field2", 3.0);
    AvroStruct avroStruct1 = (AvroStruct) AvroWrapper.createStdData(record1, structSchema);
    assertEquals(2, avroStruct1.fields().size());
    assertEquals(3.0, ((PlatformData) avroStruct1.getField("field2")).getUnderlyingData());

    GenericRecord record2 = new GenericData.Record(structSchema);
    record2.put("field1", 1);
    record2.put("field2", null);
    AvroStruct avroStruct2 = (AvroStruct) AvroWrapper.createStdData(record2, structSchema);
    assertEquals(2, avroStruct2.fields().size());
    assertNull(avroStruct2.getField("field2"));
    assertNull(avroStruct2.fields().get(1));

    GenericRecord record3 = new GenericData.Record(structSchema);
    record3.put("field1", 1);
    AvroStruct avroStruct3 = (AvroStruct) AvroWrapper.createStdData(record3, structSchema);
    assertEquals(2, avroStruct3.fields().size());
    assertNull(avroStruct3.getField("field2"));
    assertNull(avroStruct3.fields().get(1));
  }
}
