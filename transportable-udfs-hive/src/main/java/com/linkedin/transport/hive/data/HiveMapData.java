/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.hive.HiveWrapper;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;


public class HiveMapData<K, V> extends HiveData implements MapData<K, V> {

  final MapObjectInspector _mapObjectInspector;
  final ObjectInspector _keyObjectInspector;
  final ObjectInspector _valueObjectInspector;

  public HiveMapData(Object object, ObjectInspector objectInspector, TypeFactory typeFactory) {
    super(typeFactory);
    _object = object;
    _mapObjectInspector = (MapObjectInspector) objectInspector;
    _keyObjectInspector = _mapObjectInspector.getMapKeyObjectInspector();
    _valueObjectInspector = _mapObjectInspector.getMapValueObjectInspector();
  }

  @Override
  public int size() {
    return _mapObjectInspector.getMapSize(_object);
  }

  @Override
  public V get(K key) {
    MapObjectInspector mapOI = _mapObjectInspector;
    Object mapObj = _object;
    Object keyObj;
    try {
      keyObj = HiveWrapper.getPlatformDataForObjectInspector(key, _keyObjectInspector);
    } catch (RuntimeException e) {
      // Cannot convert key argument to Map's KeyOI. So convert both the map and the key arg to
      // objects having standard OIs
      mapOI = (MapObjectInspector) getStandardObjectInspector();
      mapObj = HiveWrapper.getStandardObject(this);
      keyObj = HiveWrapper.getStandardObject(key);
    }

    return (V) HiveWrapper.createStdData(
        mapOI.getMapValueElement(mapObj, keyObj),
        mapOI.getMapValueObjectInspector(), _typeFactory);
  }

  @Override
  public void put(K key, V value) {
    if (_mapObjectInspector instanceof SettableMapObjectInspector) {
      Object keyObj = HiveWrapper.getPlatformDataForObjectInspector(key, _keyObjectInspector);
      Object valueObj = HiveWrapper.getPlatformDataForObjectInspector(value, _valueObjectInspector);

      ((SettableMapObjectInspector) _mapObjectInspector).put(
          _object,
          keyObj,
          valueObj
      );
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _mapObjectInspector.getClass());
    }
  }

  //TODO: Cache the result of .getMap(_object) below for subsequent calls.
  @Override
  public Set<K> keySet() {
    return new AbstractSet<K>() {
      @Override
      public Iterator<K> iterator() {
        return new Iterator<K>() {
          Iterator mapKeyIterator = _mapObjectInspector.getMap(_object).keySet().iterator();

          @Override
          public boolean hasNext() {
            return mapKeyIterator.hasNext();
          }

          @Override
          public K next() {
            return (K) HiveWrapper.createStdData(mapKeyIterator.next(), _keyObjectInspector, _typeFactory);
          }
        };
      }

      @Override
      public int size() {
        return HiveMapData.this.size();
      }
    };
  }

  //TODO: Cache the result of .getMap(_object) below for subsequent calls.
  @Override
  public Collection<V> values() {
    return new AbstractCollection<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {
          Iterator mapValueIterator = _mapObjectInspector.getMap(_object).values().iterator();

          @Override
          public boolean hasNext() {
            return mapValueIterator.hasNext();
          }

          @Override
          public V next() {
            return (V) HiveWrapper.createStdData(mapValueIterator.next(), _valueObjectInspector, _typeFactory);
          }
        };
      }

      @Override
      public int size() {
        return HiveMapData.this.size();
      }
    };
  }

  @Override
  public boolean containsKey(K key) {
    Object mapObj = _object;
    Object keyObj;
    try {
      keyObj = HiveWrapper.getPlatformDataForObjectInspector(key, _keyObjectInspector);
    } catch (RuntimeException e) {
      // Cannot convert key argument to Map's KeyOI. So convertboth the map and the key arg to
      // objects having standard OIs
      mapObj = HiveWrapper.getStandardObject(this);
      keyObj = HiveWrapper.getStandardObject(key);
    }

    return ((Map) mapObj).containsKey(keyObj);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _mapObjectInspector;
  }
}
