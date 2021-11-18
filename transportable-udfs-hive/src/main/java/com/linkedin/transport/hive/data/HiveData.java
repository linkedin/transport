/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.hive.HiveTypeFactory;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;


public abstract class HiveData implements PlatformData {

  protected Object _object;
  protected TypeFactory _typeFactory;
  protected boolean _isObjectModified;
  private ConcurrentHashMap<ObjectInspector, Object> _cachedObjectsForObjectInspectors;

  public HiveData(TypeFactory typeFactory) {
    _typeFactory = typeFactory;
    _isObjectModified = false;
    _cachedObjectsForObjectInspectors = new ConcurrentHashMap<>();
  }

  @Override
  public Object getUnderlyingData() {
    return _object;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _isObjectModified = true;
    _object = value;
  }

  public abstract ObjectInspector getUnderlyingObjectInspector();

  public Object getUnderlyingDataForObjectInspector(ObjectInspector oi) {
    if (oi.equals(getUnderlyingObjectInspector())) {
      return getUnderlyingData();
    }

    Object result = getObjectFromCache(oi);
    if (result == null) {
      Converter c = ((HiveTypeFactory) _typeFactory).getConverter(getUnderlyingObjectInspector(), oi);
      result = c.convert(getUnderlyingData());
      _cachedObjectsForObjectInspectors.putIfAbsent(oi, result);
    }
    return result;
  }

  public ObjectInspector getStandardObjectInspector() {
    return ObjectInspectorUtils.getStandardObjectInspector(
        getUnderlyingObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
  }

  private Object getObjectFromCache(ObjectInspector oi) {
    if (_isObjectModified) {
      _cachedObjectsForObjectInspectors.clear();
      _isObjectModified = false;
      return null;
    } else {
      return _cachedObjectsForObjectInspectors.get(oi);
    }
  }
}
