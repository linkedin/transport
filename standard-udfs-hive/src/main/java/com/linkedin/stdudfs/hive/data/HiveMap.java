package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.hive.HiveWrapper;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;


public class HiveMap extends HiveData implements StdMap {

  final MapObjectInspector _mapObjectInspector;
  final ObjectInspector _keyObjectInspector;
  final ObjectInspector _valueObjectInspector;

  public HiveMap(Object object, ObjectInspector objectInspector, StdFactory stdFactory) {
    super(stdFactory);
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
  public StdData get(StdData key) {
    MapObjectInspector mapOI = _mapObjectInspector;
    Object mapObj = _object;
    Object keyObj;
    try {
      keyObj = ((HiveData) key).getUnderlyingDataForObjectInspector(_keyObjectInspector);
    } catch (RuntimeException e) {
      // Cannot convert key argument to Map's KeyOI. So convert both the map and the key arg to
      // objects having standard OIs
      mapOI = (MapObjectInspector) getStandardObjectInspector();
      mapObj = getStandardObject();
      keyObj = ((HiveData) key).getStandardObject();
    }

    return HiveWrapper.createStdData(
        mapOI.getMapValueElement(mapObj, keyObj),
        mapOI.getMapValueObjectInspector(), _stdFactory);
  }

  @Override
  public void put(StdData key, StdData value) {
    if (_mapObjectInspector instanceof SettableMapObjectInspector) {
      Object keyObj = ((HiveData) key).getUnderlyingDataForObjectInspector(_keyObjectInspector);
      Object valueObj = ((HiveData) value).getUnderlyingDataForObjectInspector(_valueObjectInspector);

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
  public Set<StdData> keySet() {
    return new AbstractSet<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          Iterator mapKeyIterator = _mapObjectInspector.getMap(_object).keySet().iterator();

          @Override
          public boolean hasNext() {
            return mapKeyIterator.hasNext();
          }

          @Override
          public StdData next() {
            return HiveWrapper.createStdData(mapKeyIterator.next(), _keyObjectInspector, _stdFactory);
          }
        };
      }

      @Override
      public int size() {
        return HiveMap.this.size();
      }
    };
  }

  //TODO: Cache the result of .getMap(_object) below for subsequent calls.
  @Override
  public Collection<StdData> values() {
    return new AbstractCollection<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          Iterator mapValueIterator = _mapObjectInspector.getMap(_object).values().iterator();

          @Override
          public boolean hasNext() {
            return mapValueIterator.hasNext();
          }

          @Override
          public StdData next() {
            return HiveWrapper.createStdData(mapValueIterator.next(), _valueObjectInspector, _stdFactory);
          }
        };
      }

      @Override
      public int size() {
        return HiveMap.this.size();
      }
    };
  }

  @Override
  public boolean containsKey(StdData key) {
    Object mapObj = _object;
    Object keyObj;
    try {
      keyObj = ((HiveData) key).getUnderlyingDataForObjectInspector(_keyObjectInspector);
    } catch (RuntimeException e) {
      // Cannot convert key argument to Map's KeyOI. So convertboth the map and the key arg to
      // objects having standard OIs
      mapObj = getStandardObject();
      keyObj = ((HiveData) key).getStandardObject();
    }

    return ((Map) mapObj).containsKey(keyObj);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _mapObjectInspector;
  }
}
