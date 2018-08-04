package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.hive.HiveFactory;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;


public abstract class HiveData implements PlatformData {

  protected Object _object;
  protected StdFactory _stdFactory;
  protected boolean _isObjectModified;
  private ConcurrentHashMap<ObjectInspector, Object> _cachedObjectsForObjectInspectors;

  public HiveData(StdFactory stdFactory) {
    _stdFactory = stdFactory;
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
      Converter c = ((HiveFactory) _stdFactory).getConverter(getUnderlyingObjectInspector(), oi);
      result = c.convert(getUnderlyingData());
      _cachedObjectsForObjectInspectors.putIfAbsent(oi, result);
    }
    return result;
  }

  public ObjectInspector getStandardObjectInspector() {
    return ObjectInspectorUtils.getStandardObjectInspector(
        getUnderlyingObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
  }

  public Object getStandardObject() {
    return getUnderlyingDataForObjectInspector(getStandardObjectInspector());
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
