/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
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
