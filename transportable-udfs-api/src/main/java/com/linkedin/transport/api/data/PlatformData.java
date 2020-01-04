/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

/** An interface to handle platform-specific container types. */
public interface PlatformData {

  /** Returns the underlying platform-specific object holding the data. */
  Object getUnderlyingData();

  /** Sets the underlying platform-specific object holding the data. */
  void setUnderlyingData(Object value);
}
