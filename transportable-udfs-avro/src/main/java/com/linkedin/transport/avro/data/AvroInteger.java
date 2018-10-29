/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdInteger;


public class AvroInteger implements StdInteger, PlatformData {
  private Integer _integer;

  public AvroInteger(Integer integer) {
    _integer = integer;
  }

  @Override
  public int get() {
    return _integer;
  }

  @Override
  public Object getUnderlyingData() {
    return _integer;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _integer = (Integer) value;
  }
}
