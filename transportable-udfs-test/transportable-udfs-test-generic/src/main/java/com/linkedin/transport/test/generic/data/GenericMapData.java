/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.test.generic.GenericConverters;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.TestType;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class GenericMapData<K, V> implements MapData<K, V>, PlatformData {

  private Map<Object, Object> _map;
  private final TestType _keyType;
  private final TestType _valueType;

  public GenericMapData(Map<Object, Object> map, TestType type) {
    _map = map;
    _keyType = ((MapTestType) type).getKeyType();
    _valueType = ((MapTestType) type).getValueType();
  }

  public GenericMapData(TestType type) {
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
  public V get(K key) {
    return (V) GenericConverters.toTransportData(_map.get(GenericConverters.toPlatformData(key)), _valueType);
  }

  @Override
  public void put(K key, V value) {
    _map.put(GenericConverters.toPlatformData(key), GenericConverters.toPlatformData(value));
  }

  @Override
  public Set<K> keySet() {
    return new AbstractSet<K>() {
      @Override
      public Iterator<K> iterator() {
        return new Iterator<K>() {
          Iterator<Object> keySet = _map.keySet().iterator();

          @Override
          public boolean hasNext() {
            return keySet.hasNext();
          }

          @Override
          public K next() {
            return (K) GenericConverters.toTransportData(keySet.next(), _keyType);
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
  public Collection<V> values() {
    return _map.values().stream().map(v -> (V) GenericConverters.toTransportData(v, _valueType)).collect(Collectors.toList());
  }

  @Override
  public boolean containsKey(K key) {
    return _map.containsKey(GenericConverters.toPlatformData(key));
  }
}
