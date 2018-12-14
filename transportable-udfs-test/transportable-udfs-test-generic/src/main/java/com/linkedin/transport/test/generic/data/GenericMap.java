/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.test.generic.GenericWrapper;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.TestType;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class GenericMap implements StdMap, PlatformData {

  private Map<Object, Object> _map;
  private final TestType _keyType;
  private final TestType _valueType;

  public GenericMap(Map<Object, Object> map, TestType type) {
    _map = map;
    _keyType = ((MapTestType) type).getKeyType();
    _valueType = ((MapTestType) type).getValueType();
  }

  public GenericMap(TestType type) {
    this(new LinkedHashMap<>(), type);
  }

  @Override
  public Object getUnderlyingData() {
    return _map;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _map = (Map<Object, Object>) value;
  }

  @Override
  public int size() {
    return _map.size();
  }

  @Override
  public StdData get(StdData key) {
    return GenericWrapper.createStdData(_map.get(((PlatformData) key).getUnderlyingData()), _valueType);
  }

  @Override
  public void put(StdData key, StdData value) {
    _map.put(((PlatformData) key).getUnderlyingData(), ((PlatformData) value).getUnderlyingData());
  }

  @Override
  public Set<StdData> keySet() {
    return new AbstractSet<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          Iterator<Object> keySet = _map.keySet().iterator();

          @Override
          public boolean hasNext() {
            return keySet.hasNext();
          }

          @Override
          public StdData next() {
            return GenericWrapper.createStdData(keySet.next(), _keyType);
          }
        };
      }

      @Override
      public int size() {
        return _map.size();
      }
    };
  }

  @Override
  public Collection<StdData> values() {
    return _map.values().stream().map(v -> GenericWrapper.createStdData(v, _valueType)).collect(Collectors.toList());
  }

  @Override
  public boolean containsKey(StdData key) {
    return _map.containsKey(((PlatformData) key).getUnderlyingData());
  }
}
