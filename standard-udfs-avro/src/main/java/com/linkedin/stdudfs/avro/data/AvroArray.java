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
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.avro.AvroWrapper;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;


public class AvroArray implements StdArray, PlatformData {
  private final Schema _elementSchema;
  private GenericArray<Object> _genericArray;

  public AvroArray(GenericArray<Object> genericArray, Schema arraySchema) {
    _genericArray = genericArray;
    _elementSchema = arraySchema.getElementType();
  }

  public AvroArray(Schema arraySchema, int size) {
    _elementSchema = arraySchema.getElementType();
    _genericArray = new GenericData.Array(size, arraySchema);
  }

  @Override
  public int size() {
    return _genericArray.size();
  }

  @Override
  public StdData get(int idx) {
    return AvroWrapper.createStdData(_genericArray.get(idx), _elementSchema);
  }

  @Override
  public void add(StdData e) {
    _genericArray.add(((PlatformData) e).getUnderlyingData());
  }

  @Override
  public Iterator<StdData> iterator() {
    return new Iterator<StdData>() {
      private final Iterator<Object> _iterator = _genericArray.iterator();

      @Override
      public boolean hasNext() {
        return _iterator.hasNext();
      }

      @Override
      public StdData next() {
        return AvroWrapper.createStdData(_iterator.next(), _elementSchema);
      }
    };
  }

  @Override
  public Object getUnderlyingData() {
    return _genericArray;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _genericArray = (GenericArray<Object>) value;
  }
}
