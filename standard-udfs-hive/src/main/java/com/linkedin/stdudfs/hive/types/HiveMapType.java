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
package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdMapType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.hive.HiveWrapper;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;


public class HiveMapType implements StdMapType {

  final MapObjectInspector _mapObjectInspector;

  public HiveMapType(MapObjectInspector mapObjectInspector) {
    _mapObjectInspector = mapObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _mapObjectInspector;
  }

  @Override
  public StdType keyType() {
    return HiveWrapper.createStdType(_mapObjectInspector.getMapKeyObjectInspector());
  }

  @Override
  public StdType valueType() {
    return HiveWrapper.createStdType(_mapObjectInspector.getMapValueObjectInspector());
  }
}
