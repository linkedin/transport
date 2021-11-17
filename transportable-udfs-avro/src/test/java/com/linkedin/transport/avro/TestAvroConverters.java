/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.types.DataType;
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


public class TestAvroConverters {

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
    Object enumData1 = AvroConverters.toTransportData(record1.get("field1"),
        Schema.createEnum("SampleEnum", "", "", Arrays.asList("A", "B")));
    assertTrue(enumData1 instanceof String);
    assertEquals("A", ((String) enumData1));

    GenericRecord record2 = new GenericData.Record(structSchema);
    record1.put("field1", new GenericData.EnumSymbol(field1, "A"));
    Object enumData2 = AvroConverters.toTransportData(record1.get("field1"),
        Schema.createEnum("SampleEnum", "", "", Arrays.asList("A", "B")));
    assertTrue(enumData2 instanceof String);
    assertEquals("A", ((String) enumData2));
  }

  @Test
  public void testArrayType() {
    Schema elementType = createSchema("\"int\"");
    Schema arraySchema = Schema.createArray(elementType);

    DataType arrayType = AvroConverters.toTransportType(arraySchema);
    assertTrue(arrayType instanceof AvroArrayType);
    assertEquals(arraySchema, ((AvroArrayType) arrayType).underlyingType());
    assertEquals(elementType, ((AvroArrayType) arrayType).elementType().underlyingType());

    GenericArray<Integer> value = new GenericData.Array<>(arraySchema, Arrays.asList(1, 2));
    Object arrayData = AvroConverters.toTransportData(value, arraySchema);
    assertTrue(arrayData instanceof AvroArrayData);
    assertEquals(2, ((AvroArrayData) arrayData).size());
    assertEquals(value, ((AvroArrayData) arrayData).getUnderlyingData());
  }

  @Test
  public void testMapType() {
    Schema valueType = createSchema("\"long\"");
    Schema mapSchema = Schema.createMap(valueType);

    DataType mapType = AvroConverters.toTransportType(mapSchema);
    assertTrue(mapType instanceof AvroMapType);
    assertEquals(mapSchema, mapType.underlyingType());
    assertEquals(valueType, ((AvroMapType) mapType).valueType().underlyingType());

    Map<String, Long> value = ImmutableMap.of("foo", 1L, "bar", 2L);
    Object mapData = AvroConverters.toTransportData(value, mapSchema);
    assertTrue(mapData instanceof AvroMapData);
    assertEquals(2, ((AvroMapData) mapData).size());
    assertEquals(value, ((AvroMapData) mapData).getUnderlyingData());
  }

  @Test
  public void testRecordType() {
    Schema field1 = createSchema("field1", "\"int\"");
    Schema field2 = createSchema("field2", "\"double\"");
    Schema structSchema = Schema.createRecord(ImmutableList.of(
        new Schema.Field("field1", field1, null, null),
        new Schema.Field("field2", field2, null, null)
    ));

    DataType rowType = AvroConverters.toTransportType(structSchema);
    assertTrue(rowType instanceof AvroRowType);
    assertEquals(structSchema, rowType.underlyingType());
    assertEquals(field1, ((AvroRowType) rowType).fieldTypes().get(0).underlyingType());
    assertEquals(field2, ((AvroRowType) rowType).fieldTypes().get(1).underlyingType());

    GenericRecord value = new GenericData.Record(structSchema);
    value.put("field1", 1);
    value.put("field2", 2.0);
    Object rowData = AvroConverters.toTransportData(value, structSchema);
    assertTrue(rowData instanceof AvroRowData);
    AvroRowData avroStruct = (AvroRowData) rowData;
    assertEquals(2, avroStruct.fields().size());
    assertEquals(value, avroStruct.getUnderlyingData());
    assertEquals(1,  avroStruct.getField("field1"));
    assertEquals(2.0, avroStruct.getField("field2"));
  }

  @Test
  public void testValidUnionType() {
    Schema nonNullType = createSchema("\"long\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType, Schema.create(Schema.Type.NULL)));

    DataType longType = AvroConverters.toTransportType(unionSchema);
    assertTrue(longType instanceof AvroLongType);
    assertEquals(nonNullType, longType.underlyingType());

    Object longData = AvroConverters.toTransportData(1L, unionSchema);
    assertTrue(longData instanceof Long);
    assertEquals(1L, longData);

    Object nullData = AvroConverters.toTransportData(null, unionSchema);
    assertNull(nullData);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidUnionType1() {
    Schema nonNullType = createSchema("\"long\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType));
    AvroConverters.toTransportType(unionSchema);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidUnionType2() {
    Schema nonNullType1 = createSchema("\"long\"");
    Schema nonNullType2 = createSchema("\"int\"");
    Schema unionSchema = Schema.createUnion(Arrays.asList(nonNullType1, nonNullType2));
    AvroConverters.toTransportData(1L, unionSchema);
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
    AvroRowData avroStruct1 = (AvroRowData) AvroConverters.toTransportData(record1, structSchema);
    assertEquals(2, avroStruct1.fields().size());
    assertEquals(3.0,  avroStruct1.getField("field2"));

    GenericRecord record2 = new GenericData.Record(structSchema);
    record2.put("field1", 1);
    record2.put("field2", null);
    AvroRowData avroStruct2 = (AvroRowData) AvroConverters.toTransportData(record2, structSchema);
    assertEquals(2, avroStruct2.fields().size());
    assertNull(avroStruct2.getField("field2"));
    assertNull(avroStruct2.fields().get(1));

    GenericRecord record3 = new GenericData.Record(structSchema);
    record3.put("field1", 1);
    AvroRowData avroStruct3 = (AvroRowData) AvroConverters.toTransportData(record3, structSchema);
    assertEquals(2, avroStruct3.fields().size());
    assertNull(avroStruct3.getField("field2"));
    assertNull(avroStruct3.fields().get(1));
  }
}
