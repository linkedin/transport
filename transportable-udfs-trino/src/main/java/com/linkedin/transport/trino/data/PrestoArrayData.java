/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.StdFactory;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
=======
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.presto.PrestoWrapper;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
import java.util.Iterator;

import static io.trino.spi.type.TypeUtils.*;


<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
public class TrinoArray extends TrinoData implements StdArray {
=======
public class PrestoArrayData<E> extends PrestoData implements ArrayData<E> {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java

  private final StdFactory _stdFactory;
  private final ArrayType _arrayType;
  private final Type _elementType;

  private Block _block;
  private BlockBuilder _mutable;

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
  public TrinoArray(Block block, ArrayType arrayType, StdFactory stdFactory) {
=======
  public PrestoArrayData(Block block, ArrayType arrayType, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
    _block = block;
    _arrayType = arrayType;
    _elementType = arrayType.getElementType();
    _stdFactory = stdFactory;
  }

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
  public TrinoArray(ArrayType arrayType, int expectedEntries, StdFactory stdFactory) {
=======
  public PrestoArrayData(ArrayType arrayType, int expectedEntries, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
    _block = null;
    _elementType = arrayType.getElementType();
    _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), expectedEntries);
    _stdFactory = stdFactory;
    _arrayType = arrayType;
  }

  @Override
  public int size() {
    return _mutable == null ? _block.getPositionCount() : _mutable.getPositionCount();
  }

  @Override
  public E get(int idx) {
    Block sourceBlock = _mutable == null ? _block : _mutable;
    int position = TrinoWrapper.checkedIndexToBlockPosition(sourceBlock, idx);
    Object element = readNativeValue(_elementType, sourceBlock, position);
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
    return TrinoWrapper.createStdData(element, _elementType, _stdFactory);
=======
    return (E) PrestoWrapper.createStdData(element, _elementType, _stdFactory);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
  }

  @Override
  public void add(E e) {
    if (_mutable == null) {
      _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    }
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
    ((TrinoData) e).writeToBlock(_mutable);
=======
    PrestoWrapper.writeToBlock(e, _mutable);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
  }

  @Override
  public Object getUnderlyingData() {
    return _mutable == null ? _block : _mutable.build();
  }

  @Override
  public void setUnderlyingData(Object value) {
    _block = (Block) value;
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      Block sourceBlock = _mutable == null ? _block : _mutable;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
      int size = TrinoArray.this.size();
=======
      int size = PrestoArrayData.this.size();
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
      int position = 0;

      @Override
      public boolean hasNext() {
        return position != size;
      }

      @Override
      public E next() {
        Object element = readNativeValue(_elementType, sourceBlock, position);
        position++;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoArray.java
        return TrinoWrapper.createStdData(element, _elementType, _stdFactory);
=======
        return (E) PrestoWrapper.createStdData(element, _elementType, _stdFactory);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoArrayData.java
      }
    };
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _arrayType.writeObject(blockBuilder, getUnderlyingData());
  }
}
