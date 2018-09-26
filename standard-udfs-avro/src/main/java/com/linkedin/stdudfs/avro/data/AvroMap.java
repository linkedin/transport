/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.avro.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.avro.AvroWrapper;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.*;


public class AvroMap implements StdMap, PlatformData {
  private Map<Object, Object> _map;
  private final Schema _keySchema;
  private final Schema _valueSchema;

  public AvroMap(Map<Object, Object> map, Schema mapSchema) {
    _map = map;
    _keySchema = Schema.create(STRING);
    _valueSchema = mapSchema.getValueType();
  }

  public AvroMap(Schema mapSchema) {
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
  public StdData get(StdData key) {
    return AvroWrapper.createStdData(_map.get(((PlatformData) key).getUnderlyingData()), _valueSchema);
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
            return AvroWrapper.createStdData(keySet.next(), _keySchema);
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
    return _map.values().stream().map(v -> AvroWrapper.createStdData(v, _valueSchema)).collect(Collectors.toList());
  }

  @Override
  public boolean containsKey(StdData key) {
    return _map.containsKey(((PlatformData) key).getUnderlyingData());
  }
}
