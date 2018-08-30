/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
