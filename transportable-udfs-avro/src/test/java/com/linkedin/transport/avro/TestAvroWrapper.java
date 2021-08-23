/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArrayData;
import com.linkedin.transport.avro.data.AvroMapData;
import com.linkedin.transport.avro.data.AvroRowData;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroRowType;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

  @Test
  public void testEnumType() {
    Schema field1 = createSchema("field1", ""
        + "\"enum\","
        + "\"name\":\"SampleEnum\","
        + "\"doc\":\"\","
        + "\"symbols\":[\"A\",\"B\"]");
    Schema structSchema = Schema.createRecord(ImmutableList.of(
        new Schema.Field("field1", field1, null, null)
    ));

    GenericRecord record1 = new GenericData.Record(structSchema);
    record1.put("field1", "A");
    Object stdEnumData1 = AvroWrapper.createStdData(record1.get("field1"),
        Schema.createEnum("SampleEnum", "", "", Arrays.asList("A", "B")));
    assertTrue(stdEnumData1 instanceof String);
    assertEquals("A", ((String) stdEnumData1));

    GenericRecord record2 = new GenericData.Record(structSchema);
    record1.put("field1", new GenericData.EnumSymbol(field1, "A"));
    Object stdEnumData2 = AvroWrapper.createStdData(record1.get("field1"),
        Schema.createEnum("SampleEnum", "", "", Arrays.asList("A", "B")));
    assertTrue(stdEnumData2 instanceof String);
    assertEquals("A", ((String) stdEnumData2));
  }

  @Test
  public void testArrayType() {
    Schema elementType = createSchema("\"int\"");
    Schema arraySchema = Schema.createArray(elementType);

    StdType stdArrayType = AvroWrapper.createStdType(arraySchema);
    assertTrue(stdArrayType instanceof AvroArrayType);
    assertEquals(arraySchema, ((AvroArrayType) stdArrayType).underlyingType());
    assertEquals(elementType, ((AvroArrayType) stdArrayType).elementType().underlyingType());

    GenericArray<Integer> value = new GenericData.Array<>(arraySchema, Arrays.asList(1, 2));
    Object stdArrayData = AvroWrapper.createStdData(value, arraySchema);
    assertTrue(stdArrayData instanceof AvroArrayData);
    assertEquals(2, ((AvroArrayData) stdArrayData).size());
    assertEquals(value, ((AvroArrayData) stdArrayData).getUnderlyingData());
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
    Object stdMapData = AvroWrapper.createStdData(value, mapSchema);
    assertTrue(stdMapData instanceof AvroMapData);
    assertEquals(2, ((AvroMapData) stdMapData).size());
    assertEquals(value, ((AvroMapData) stdMapData).getUnderlyingData());
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
    assertTrue(stdStructType instanceof AvroRowType);
    assertEquals(structSchema, stdStructType.underlyingType());
    assertEquals(field1, ((AvroRowType) stdStructType).fieldTypes().get(0).underlyingType());
    assertEquals(field2, ((AvroRowType) stdStructType).fieldTypes().get(1).underlyingType());

    GenericRecord value = new GenericData.Record(structSchema);
    value.put("field1", 1);
    value.put("field2", 2.0);
    Object stdStructData = AvroWrapper.createStdData(value, structSchema);
    assertTrue(stdStructData instanceof AvroRowData);
    AvroRowData avroStruct = (AvroRowData) stdStructData;
    assertEquals(2, avroStruct.fields().size());
    assertEquals(value, avroStruct.getUnderlyingData());
    assertEquals(1,  avroStruct.getField("field1"));
    assertEquals(2.0, avroStruct.getField("field2"));
  }

  @Test
  public void testValidUnionType() {
    Schema nonNullType = createSchema("\"long\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType, Schema.create(Schema.Type.NULL)));

    StdType stdLongType = AvroWrapper.createStdType(unionSchema);
    assertTrue(stdLongType instanceof AvroLongType);
    assertEquals(nonNullType, stdLongType.underlyingType());

    Object stdLongData = AvroWrapper.createStdData(1L, unionSchema);
    assertTrue(stdLongData instanceof Long);
    assertEquals(1L, stdLongData);

    Object stdNullData = AvroWrapper.createStdData(null, unionSchema);
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
    AvroRowData avroStruct1 = (AvroRowData) AvroWrapper.createStdData(record1, structSchema);
    assertEquals(2, avroStruct1.fields().size());
    assertEquals(3.0,  avroStruct1.getField("field2"));

    GenericRecord record2 = new GenericData.Record(structSchema);
    record2.put("field1", 1);
    record2.put("field2", null);
    AvroRowData avroStruct2 = (AvroRowData) AvroWrapper.createStdData(record2, structSchema);
    assertEquals(2, avroStruct2.fields().size());
    assertNull(avroStruct2.getField("field2"));
    assertNull(avroStruct2.fields().get(1));

    GenericRecord record3 = new GenericData.Record(structSchema);
    record3.put("field1", 1);
    AvroRowData avroStruct3 = (AvroRowData) AvroWrapper.createStdData(record3, structSchema);
    assertEquals(2, avroStruct3.fields().size());
    assertNull(avroStruct3.getField("field2"));
    assertNull(avroStruct3.fields().get(1));
  }
}
