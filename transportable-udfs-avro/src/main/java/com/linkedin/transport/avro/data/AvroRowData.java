/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.avro.AvroWrapper;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;


public class AvroRowData implements RowData, PlatformData {

  private final Schema _recordSchema;
  private GenericRecord _genericRecord;

  public AvroRowData(GenericRecord genericRecord, Schema recordSchema) {
    _genericRecord = genericRecord;
    _recordSchema = recordSchema;
  }

  public AvroRowData(Schema recordSchema) {
    _genericRecord = new Record(recordSchema);
    _recordSchema = recordSchema;
  }

  @Override
  public Object getUnderlyingData() {
    return _genericRecord;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _genericRecord = (GenericRecord) value;
  }

  @Override
  public Object getField(int index) {
    return AvroWrapper.createStdData(_genericRecord.get(index), _recordSchema.getFields().get(index).schema());
  }

  @Override
  public Object getField(String name) {
    return AvroWrapper.createStdData(_genericRecord.get(name), _recordSchema.getField(name).schema());
  }

  @Override
  public void setField(int index, Object value) {
    _genericRecord.put(index, AvroWrapper.getPlatformData(value));
  }

  @Override
  public void setField(String name, Object value) {
    _genericRecord.put(name, AvroWrapper.getPlatformData(value));
  }

  @Override
  public List<Object> fields() {
    return IntStream.range(0, _recordSchema.getFields().size()).mapToObj(i -> getField(i)).collect(Collectors.toList());
  }
}
