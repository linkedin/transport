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
package com.linkedin.stdudfs.avro.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.avro.AvroWrapper;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;


public class AvroStruct implements StdStruct, PlatformData {

  private final Schema _recordSchema;
  private GenericRecord _genericRecord;

  public AvroStruct(GenericRecord genericRecord, Schema recordSchema) {
    _genericRecord = genericRecord;
    _recordSchema = recordSchema;
  }

  public AvroStruct(Schema recordSchema) {
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
  public StdData getField(int index) {
    return AvroWrapper.createStdData(_genericRecord.get(index), _recordSchema.getFields().get(index).schema());
  }

  @Override
  public StdData getField(String name) {
    return AvroWrapper.createStdData(_genericRecord.get(name), _recordSchema.getField(name).schema());
  }

  @Override
  public void setField(int index, StdData value) {
    _genericRecord.put(index, ((PlatformData) value).getUnderlyingData());
  }

  @Override
  public void setField(String name, StdData value) {
    _genericRecord.put(name, ((PlatformData) value).getUnderlyingData());
  }

  @Override
  public List<StdData> fields() {
    return IntStream.range(0, _recordSchema.getFields().size()).mapToObj(i -> getField(i)).collect(Collectors.toList());
  }
}
