/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.avro.AvroWrapper;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;


public class AvroArrayData<E> implements ArrayData<E>, PlatformData {
  private final Schema _elementSchema;
  private GenericArray<Object> _genericArray;

  public AvroArrayData(GenericArray<Object> genericArray, Schema arraySchema) {
    _genericArray = genericArray;
    _elementSchema = arraySchema.getElementType();
  }

  public AvroArrayData(Schema arraySchema, int size) {
    _elementSchema = arraySchema.getElementType();
    _genericArray = new GenericData.Array(size, arraySchema);
  }

  @Override
  public int size() {
    return _genericArray.size();
  }

  @Override
  public E get(int idx) {
    return (E) AvroWrapper.createStdData(_genericArray.get(idx), _elementSchema);
  }

  @Override
  public void add(E e) {
    _genericArray.add(AvroWrapper.getPlatformData(e));
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      private final Iterator<Object> _iterator = _genericArray.iterator();

      @Override
      public boolean hasNext() {
        return _iterator.hasNext();
      }

      @Override
      public E next() {
        return (E) AvroWrapper.createStdData(_iterator.next(), _elementSchema);
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
