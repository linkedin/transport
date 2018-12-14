/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.test.generic.GenericWrapper;
import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.TestType;
import java.util.Iterator;
import java.util.List;


public class GenericArray implements StdArray, PlatformData {

  private List<Object> _array;
  private TestType _elementType;

  public GenericArray(List<Object> data, TestType type) {
    _array = data;
    _elementType = ((ArrayTestType) type).getElementType();
  }

  @Override
  public int size() {
    return _array.size();
  }

  @Override
  public StdData get(int idx) {
    return GenericWrapper.createStdData(_array.get(idx), _elementType);
  }

  @Override
  public void add(StdData e) {
    _array.add(((PlatformData) e).getUnderlyingData());
  }

  @Override
  public Iterator<StdData> iterator() {
    return new Iterator<StdData>() {
      private final Iterator<Object> _iterator = _array.iterator();

      @Override
      public boolean hasNext() {
        return _iterator.hasNext();
      }

      @Override
      public StdData next() {
        return GenericWrapper.createStdData(_iterator.next(), _elementType);
      }
    };
  }

  @Override
  public Object getUnderlyingData() {
    return _array;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _array = (List<Object>) value;
  }
}
