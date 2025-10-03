/*
 * Copyright 2018 LinkedIn Corporation.
 * Licensed under the BSD-2 Clause license.
 */
package com.linkedin.transport.trino.data;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapHashTables.HashBuildMode;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.readNativeValue;

public class TrinoMap extends TrinoData implements StdMap {
  private final Type _keyType;
  private final Type _valueType;
  private final MapType _mapType;
  private final MethodHandle _keyEqualsMethod;
  private final StdFactory _stdFactory;

  private SqlMap _map;

  public TrinoMap(MapType mapType, StdFactory stdFactory) {
    _mapType = mapType;
    _keyType = mapType.getKeyType();
    _valueType = mapType.getValueType();
    _stdFactory = stdFactory;
    _keyEqualsMethod = ((TrinoFactory) stdFactory).getOperatorHandle(
        OperatorType.EQUAL,
        ImmutableList.of(_keyType, _keyType),
        simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));

    // Start with an empty SqlMap
    Block emptyKeys = _keyType.createBlockBuilder(null, 0).build();
    Block emptyValues = _valueType.createBlockBuilder(null, 0).build();
    _map = new SqlMap(mapType, HashBuildMode.STRICT_EQUALS, emptyKeys, emptyValues);
  }

  public TrinoMap(SqlMap map, MapType mapType, StdFactory stdFactory) {
    this(mapType, stdFactory);
    _map = map;
  }

  @Override
  public int size() {
    return keyBlock().getPositionCount();
  }

  @Override
  public StdData get(StdData key) {
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int idx = seekKeyIndex(trinoKey);
    if (idx == -1) {
      return null;
    }
    Object value = readNativeValue(_valueType, valueBlock(), idx);
    return TrinoWrapper.createStdData(value, _valueType, _stdFactory);
  }

  @Override
  public void put(StdData key, StdData value) {
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int existingIndex = seekKeyIndex(trinoKey);

    int n = size();
    int newSize = (existingIndex == -1) ? n + 1 : n;

    BlockBuilder keyBuilder = _keyType.createBlockBuilder(null, newSize);
    BlockBuilder valueBuilder = _valueType.createBlockBuilder(null, newSize);

    // copy existing entries, replacing value if key matches
    for (int i = 0; i < n; i++) {
      _keyType.appendTo(keyBlock(), i, keyBuilder);
      if (i == existingIndex) {
        ((TrinoData) value).writeToBlock(valueBuilder);
      } else {
        _valueType.appendTo(valueBlock(), i, valueBuilder);
      }
    }

    // append new entry if key not present
    if (existingIndex == -1) {
      ((TrinoData) key).writeToBlock(keyBuilder);
      ((TrinoData) value).writeToBlock(valueBuilder);
    }

    _map = new SqlMap(_mapType, HashBuildMode.STRICT_EQUALS, keyBuilder.build(), valueBuilder.build());
  }

  @Override
  public boolean containsKey(StdData key) {
    return get(key) != null;
  }

  public Set<StdData> keySet() {
    return new AbstractSet<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          int i = -1;
          @Override public boolean hasNext() {
            return i + 1 < size();
          }
          @Override public StdData next() {
            i++;
            Object k = readNativeValue(_keyType, keyBlock(), i);
            return TrinoWrapper.createStdData(k, _keyType, _stdFactory);
          }
        };
      }
      @Override
      public int size() {
        return TrinoMap.this.size();
      }
    };
  }

  @Override
  public Collection<StdData> values() {
    return new AbstractCollection<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          int i = -1;
          @Override public boolean hasNext() {
            return i + 1 < size();
          }
          @Override public StdData next() {
            i++;
            Object v = readNativeValue(_valueType, valueBlock(), i);
            return TrinoWrapper.createStdData(v, _valueType, _stdFactory);
          }
        };
      }
      @Override
      public int size() {
        return TrinoMap.this.size();
      }
    };
  }

  @Override
  public Object getUnderlyingData() {
    return _map;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _map = (SqlMap) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _mapType.writeObject(blockBuilder, _map);
  }

  private int seekKeyIndex(Object key) {
    Block keys = keyBlock();
    for (int i = 0; i < keys.getPositionCount(); i++) {
      try {
        if ((boolean) _keyEqualsMethod.invoke(readNativeValue(_keyType, keys, i), key)) {
          return i;
        }
      } catch (Throwable t) {
        Throwables.propagateIfInstanceOf(t, Error.class);
        Throwables.propagateIfInstanceOf(t, TrinoException.class);
        throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
      }
    }
    return -1;
  }

  private Block keyBlock() {
    return getBlock(_map, /*key=*/true);
  }

  private Block valueBlock() {
    return getBlock(_map, /*key=*/false);
  }

  /**
   * SqlMap getters differ slightly across Trino versions (getKeyBlock()/getValueBlock() vs keyBlock()/valueBlock()).
   * Use a tiny reflective shim so this class compiles against either.
   */
  private static Block getBlock(SqlMap map, boolean key) {
    try {
      Method m;
      try {
        m = SqlMap.class.getMethod(key ? "getRawKeyBlock" : "getRawValueBlock");
      } catch (NoSuchMethodException e) {
        m = SqlMap.class.getMethod(key ? "keyBlock" : "valueBlock");
      }
      return (Block) m.invoke(map);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Unable to access SqlMap blocks", e);
    }
  }
}
