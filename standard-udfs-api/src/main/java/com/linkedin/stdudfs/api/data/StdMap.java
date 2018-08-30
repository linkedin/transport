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
package com.linkedin.stdudfs.api.data;

import java.util.Collection;
import java.util.Set;


/** A Standard UDF data type for representing maps. */
public interface StdMap extends StdData {

  /** Returns the number of key-value pairs in the map. */
  int size();

  /**
   * Returns the value for the given key from the map, null if key is not found.
   *
   * @param key  the key whose value is to be returned
   */
  StdData get(StdData key);

  /**
   * Adds the given value to the map against the given key.
   *
   * @param key  the key to which the value is to be associated
   * @param value  the value to be associated with the key
   */
  void put(StdData key, StdData value);

  /** Returns a {@link Set} of all the keys in the map. */
  Set<StdData> keySet();

  /** Returns a {@link Collection} of all the values in the map. */
  Collection<StdData> values();

  /**
   * Returns true if the map contains the given key, false otherwise.
   *
   * @param key  the key to be checked
   */
  boolean containsKey(StdData key);
}
