/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.avro.AvroConverters;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.*;


public class AvroMapData<K, V> implements MapData<K, V>, PlatformData {
  private Map<Object, Object> _map;
  private final Schema _keySchema;
  private final Schema _valueSchema;

  public AvroMapData(Map<Object, Object> map, Schema mapSchema) {
    _map = map;
    _keySchema = Schema.create(STRING);
    _valueSchema = mapSchema.getValueType();
  }

  public AvroMapData(Schema mapSchema) {
    _map = new LinkedHashMap<>();
    _keySchema = Schema.create(STRING);
    _valueSchema = mapSchema.getValueType();
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
    return (V) AvroConverters.toTransportData(_map.get(AvroConverters.toPlatformData(key)), _valueSchema);
  }

  @Override
  public void put(K key, V value) {
    _map.put(AvroConverters.toPlatformData(key), AvroConverters.toPlatformData(value));
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
            return (K) AvroConverters.toTransportData(keySet.next(), _keySchema);
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
    return _map.values().stream().map(v -> (V) AvroConverters.toTransportData(v, _valueSchema)).collect(Collectors.toList());
  }

  @Override
  public boolean containsKey(K key) {
    return _map.containsKey(AvroConverters.toPlatformData(key));
  }
}
