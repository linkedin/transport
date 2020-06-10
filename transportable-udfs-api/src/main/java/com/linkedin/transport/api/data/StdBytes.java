/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

import java.nio.ByteBuffer;

/** A Standard UDF data type for representing binary objects. */
public interface StdBytes extends StdData {

  /** Returns the underlying {@link ByteBuffer} value. */
  ByteBuffer get();
}
