/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
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
import io.trino.spi.block.MapBlock;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import java.lang.invoke.MethodHandle;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.*;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeUtils.*;


public class TrinoMap extends TrinoData implements StdMap {

  final Type _keyType;
  final Type _valueType;
  final Type _mapType;
  final MethodHandle _keyEqualsMethod;
  final StdFactory _stdFactory;
  Block _block;

  public TrinoMap(Type mapType, StdFactory stdFactory) {
    MapType typedMap = (MapType) mapType;

    // Create empty key/value blocks
    Block emptyKeyBlock = typedMap.getKeyType().createBlockBuilder(null, 0).build();
    Block emptyValueBlock = typedMap.getValueType().createBlockBuilder(null, 0).build();
    int[] offsets = new int[] {0, 0};  // 1 map entry, 0 elements

    Block emptyMapBlock = MapBlock.fromKeyValueBlock(
        Optional.empty(),
        offsets,
        emptyKeyBlock,
        emptyValueBlock,
        typedMap
    );

    BlockBuilder mutable = mapType.createBlockBuilder(null, 1);
    typedMap.writeObject(mutable, emptyMapBlock);
    _block = mutable.build();

    _keyType = typedMap.getKeyType();
    _valueType = typedMap.getValueType();
    _mapType = typedMap;

    _stdFactory = stdFactory;
    _keyEqualsMethod = ((TrinoFactory) stdFactory).getOperatorHandle(
        OperatorType.EQUAL,
        ImmutableList.of(_keyType, _keyType),
        simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)
    );
  }

  public TrinoMap(Block block, Type mapType, StdFactory stdFactory) {
    this(mapType, stdFactory);
    _block = block;
  }

  @Override
  public int size() {
    return _block.getPositionCount() / 2;
  }

  @Override
  public StdData get(StdData key) {
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int i = seekKey(trinoKey);
    if (i != -1) {
      Object value = readNativeValue(_valueType, _block, i);
      StdData stdValue = TrinoWrapper.createStdData(value, _valueType, _stdFactory);
      return stdValue;
    } else {
      return null;
    }
  }

  // TODO: Do not copy the _mutable BlockBuilder on every update. As long as updates are append-only or for fixed-size
  // types, we can skip copying.
  @Override
  public void put(StdData key, StdData value) {
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int valuePosition = seekKey(trinoKey);

    int entryCount = _block.getPositionCount() / 2;
    BlockBuilder keyBuilder = _keyType.createBlockBuilder(null, entryCount + 1);
    BlockBuilder valueBuilder = _valueType.createBlockBuilder(null, entryCount + 1);

    for (int i = 0; i < _block.getPositionCount(); i += 2) {
      _keyType.appendTo(_block, i, keyBuilder);

      if (i == valuePosition - 1) {
        ((TrinoData) value).writeToBlock(valueBuilder);
      } else {
        _valueType.appendTo(_block, i + 1, valueBuilder);
      }
    }

    if (valuePosition == -1) {
      ((TrinoData) key).writeToBlock(keyBuilder);
      ((TrinoData) value).writeToBlock(valueBuilder);
    }

    int[] offsets = new int[] {0, keyBuilder.getPositionCount()};
    Block mapBlock = MapBlock.fromKeyValueBlock(
        Optional.empty(),
        offsets,
        keyBuilder.build(),
        valueBuilder.build(),
        (MapType) _mapType
    );

    BlockBuilder parent = _mapType.createBlockBuilder(null, 1);
    _mapType.writeObject(parent, mapBlock);
    _block = parent.build();
  }

  public Set<StdData> keySet() {
    return new AbstractSet<StdData>() {
      @Override
      public Iterator<StdData> iterator() {
        return new Iterator<StdData>() {
          int i = -2;

          @Override
          public boolean hasNext() {
            return !(i + 2 == size() * 2);
          }

          @Override
          public StdData next() {
            i += 2;
            return TrinoWrapper.createStdData(readNativeValue(_keyType, _block, i), _keyType, _stdFactory);
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
          int i = -2;

          @Override
          public boolean hasNext() {
            return !(i + 2 == size() * 2);
          }

          @Override
          public StdData next() {
            i += 2;
            return TrinoWrapper.createStdData(readNativeValue(_valueType, _block, i + 1), _valueType, _stdFactory);
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
  public boolean containsKey(StdData key) {
    return get(key) != null;
  }

  @Override
  public Object getUnderlyingData() {
    return _block;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _block = (Block) value;
  }

  private int seekKey(Object key) {
    for (int i = 0; i < _block.getPositionCount(); i += 2) {
      try {
        if ((boolean) _keyEqualsMethod.invoke(readNativeValue(_keyType, _block, i), key)) {
          return i + 1;
        }
      } catch (Throwable t) {
        Throwables.propagateIfInstanceOf(t, Error.class);
        Throwables.propagateIfInstanceOf(t, TrinoException.class);
        throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
      }
    }
    return -1;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _mapType.writeObject(blockBuilder, _block);
  }
}
