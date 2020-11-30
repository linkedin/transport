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
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
=======
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.presto.PrestoFactory;
import com.linkedin.transport.presto.PrestoWrapper;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
import java.lang.invoke.MethodHandle;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.*;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeUtils.*;


<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
public class TrinoMap extends TrinoData implements StdMap {
=======
public class PrestoMapData<K, V> extends PrestoData implements MapData<K, V> {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java

  final Type _keyType;
  final Type _valueType;
  final Type _mapType;
  final MethodHandle _keyEqualsMethod;
  final StdFactory _stdFactory;
  Block _block;

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
  public TrinoMap(Type mapType, StdFactory stdFactory) {
=======
  public PrestoMapData(Type mapType, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
    BlockBuilder mutable = mapType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    mutable.beginBlockEntry();
    mutable.closeEntry();
    _block = ((MapType) mapType).getObject(mutable.build(), 0);

    _keyType = ((MapType) mapType).getKeyType();
    _valueType = ((MapType) mapType).getValueType();
    _mapType = mapType;

    _stdFactory = stdFactory;
    _keyEqualsMethod = ((TrinoFactory) stdFactory).getOperatorHandle(
        OperatorType.EQUAL, ImmutableList.of(_keyType, _keyType), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
  }

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
  public TrinoMap(Block block, Type mapType, StdFactory stdFactory) {
=======
  public PrestoMapData(Block block, Type mapType, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
    this(mapType, stdFactory);
    _block = block;
  }

  @Override
  public int size() {
    return _block.getPositionCount() / 2;
  }

  @Override
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
  public StdData get(StdData key) {
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int i = seekKey(trinoKey);
    if (i != -1) {
      Object value = readNativeValue(_valueType, _block, i);
      StdData stdValue = TrinoWrapper.createStdData(value, _valueType, _stdFactory);
      return stdValue;
=======
  public V get(K key) {
    Object prestoKey = PrestoWrapper.getPlatformData(key);
    int i = seekKey(prestoKey);
    if (i != -1) {
      Object value = readNativeValue(_valueType, _block, i);
      return (V) PrestoWrapper.createStdData(value, _valueType, _stdFactory);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
    } else {
      return null;
    }
  }

  // TODO: Do not copy the _mutable BlockBuilder on every update. As long as updates are append-only or for fixed-size
  // types, we can skip copying.
  @Override
  public void put(K key, V value) {
    BlockBuilder mutable = _mapType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    BlockBuilder entryBuilder = mutable.beginBlockEntry();
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
    Object trinoKey = ((PlatformData) key).getUnderlyingData();
    int valuePosition = seekKey(trinoKey);
=======
    Object prestoKey = PrestoWrapper.getPlatformData(key);
    int valuePosition = seekKey(prestoKey);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
    for (int i = 0; i < _block.getPositionCount(); i += 2) {
      // Write the current key to the map
      _keyType.appendTo(_block, i, entryBuilder);
      // Find out if we need to change the corresponding value
      if (i == valuePosition - 1) {
        // Use the user-supplied value
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
        ((TrinoData) value).writeToBlock(entryBuilder);
=======
        PrestoWrapper.writeToBlock(value, entryBuilder);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
      } else {
        // Use the existing value in original _block
        _valueType.appendTo(_block, i + 1, entryBuilder);
      }
    }
    if (valuePosition == -1) {
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
      ((TrinoData) key).writeToBlock(entryBuilder);
      ((TrinoData) value).writeToBlock(entryBuilder);
=======
      PrestoWrapper.writeToBlock(key, entryBuilder);
      PrestoWrapper.writeToBlock(value, entryBuilder);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
    }

    mutable.closeEntry();
    _block = ((MapType) _mapType).getObject(mutable.build(), 0);
  }

  public Set<K> keySet() {
    return new AbstractSet<K>() {
      @Override
      public Iterator<K> iterator() {
        return new Iterator<K>() {
          int i = -2;

          @Override
          public boolean hasNext() {
            return !(i + 2 == size() * 2);
          }

          @Override
          public K next() {
            i += 2;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
            return TrinoWrapper.createStdData(readNativeValue(_keyType, _block, i), _keyType, _stdFactory);
=======
            return (K) PrestoWrapper.createStdData(readNativeValue(_keyType, _block, i), _keyType, _stdFactory);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
          }
        };
      }

      @Override
      public int size() {
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
        return TrinoMap.this.size();
=======
        return PrestoMapData.this.size();
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
      }
    };
  }

  @Override
  public Collection<V> values() {
    return new AbstractCollection<V>() {

      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {
          int i = -2;

          @Override
          public boolean hasNext() {
            return !(i + 2 == size() * 2);
          }

          @Override
          public V next() {
            i += 2;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
            return TrinoWrapper.createStdData(readNativeValue(_valueType, _block, i + 1), _valueType, _stdFactory);
=======
            return
                (V) PrestoWrapper.createStdData(
                    readNativeValue(_valueType, _block, i + 1), _valueType, _stdFactory
                );
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
          }
        };
      }

      @Override
      public int size() {
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoMap.java
        return TrinoMap.this.size();
=======
        return PrestoMapData.this.size();
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoMapData.java
      }
    };
  }

  @Override
  public boolean containsKey(K key) {
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
