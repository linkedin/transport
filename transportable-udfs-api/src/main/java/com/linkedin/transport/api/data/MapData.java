/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

import java.util.Collection;
import java.util.Set;


/** A Standard UDF data type for representing maps. */
public interface MapData<K, V> {

  /** Returns the number of key-value pairs in the map. */
  int size();

  /**
   * Returns the value for the given key from the map, null if key is not found.
   *
   * @param key  the key whose value is to be returned
   */
  V get(K key);

  /**
   * Adds the given value to the map against the given key.
   *
   * @param key  the key to which the value is to be associated
   * @param value  the value to be associated with the key
   */
  void put(K key, V value);

  /** Returns a {@link Set} of all the keys in the map. */
  Set<K> keySet();

  /** Returns a {@link Collection} of all the values in the map. */
  Collection<V> values();

  /**
   * Returns true if the map contains the given key, false otherwise.
   *
   * @param key  the key to be checked
   */
  boolean containsKey(K key);
}
