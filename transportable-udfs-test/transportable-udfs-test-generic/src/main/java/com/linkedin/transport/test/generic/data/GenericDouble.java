/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdDouble;


public class GenericDouble implements StdDouble, PlatformData {
  private Double _double;

  public GenericDouble(Double aDouble) {
    _double = aDouble;
  }

  @Override
  public double get() {
    return _double;
  }

  @Override
  public Object getUnderlyingData() {
    return _double;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _double = (Double) value;
  }
}
