/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.StdFactory;
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
=======
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.presto.PrestoWrapper;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.TypeUtils.*;


<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
public class TrinoStruct extends TrinoData implements StdStruct {
=======
public class PrestoRowData extends PrestoData implements RowData {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java

  final RowType _rowType;
  final StdFactory _stdFactory;
  Block _block;

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
  public TrinoStruct(Type rowType, StdFactory stdFactory) {
=======
  public PrestoRowData(Type rowType, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
    _rowType = (RowType) rowType;
    _stdFactory = stdFactory;
  }

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
  public TrinoStruct(Block block, Type rowType, StdFactory stdFactory) {
=======
  public PrestoRowData(Block block, Type rowType, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
    this(rowType, stdFactory);
    _block = block;
  }

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
  public TrinoStruct(List<Type> fieldTypes, StdFactory stdFactory) {
=======
  public PrestoRowData(List<Type> fieldTypes, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
    _stdFactory = stdFactory;
    _rowType = RowType.anonymous(fieldTypes);
  }

<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
  public TrinoStruct(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
=======
  public PrestoRowData(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
    _stdFactory = stdFactory;
    List<RowType.Field> fields = IntStream.range(0, fieldNames.size())
        .mapToObj(i -> new RowType.Field(Optional.ofNullable(fieldNames.get(i)), fieldTypes.get(i)))
        .collect(Collectors.toList());
    _rowType = RowType.from(fields);
  }

  @Override
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
  public StdData getField(int index) {
    int position = TrinoWrapper.checkedIndexToBlockPosition(_block, index);
=======
  public Object getField(int index) {
    int position = PrestoWrapper.checkedIndexToBlockPosition(_block, index);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
    if (position == -1) {
      return null;
    }
    Type elementType = _rowType.getFields().get(position).getType();
    Object element = readNativeValue(elementType, _block, position);
    return TrinoWrapper.createStdData(element, elementType, _stdFactory);
  }

  @Override
  public Object getField(String name) {
    int index = -1;
    Type elementType = null;
    int i = 0;
    for (RowType.Field field : _rowType.getFields()) {
      if (field.getName().isPresent() && name.equals(field.getName().get())) {
        index = i;
        elementType = field.getType();
        break;
      }
      i++;
    }
    if (index == -1) {
      return null;
    }
    Object element = readNativeValue(elementType, _block, index);
    return TrinoWrapper.createStdData(element, elementType, _stdFactory);
  }

  @Override
  public void setField(int index, Object value) {
    // TODO: This is not the right way to get this object. The status should be passed in from the invocation of the
    // function and propagated to here. See PRESTO-1359 for more details.
    BlockBuilderStatus blockBuilderStatus = new PageBuilderStatus().createBlockBuilderStatus();
    BlockBuilder mutable = _rowType.createBlockBuilder(blockBuilderStatus, 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.Field field : _rowType.getFields()) {
      if (i == index) {
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
        ((TrinoData) value).writeToBlock(rowBlockBuilder);
=======
        PrestoWrapper.writeToBlock(value, rowBlockBuilder);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
      } else {
        if (_block == null) {
          rowBlockBuilder.appendNull();
        } else {
          field.getType().appendTo(_block, i, rowBlockBuilder);
        }
      }
      i++;
    }
    mutable.closeEntry();
    _block = _rowType.getObject(mutable.build(), 0);
  }

  @Override
  public void setField(String name, Object value) {
    BlockBuilder mutable = _rowType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.Field field : _rowType.getFields()) {
      if (field.getName().isPresent() && name.equals(field.getName().get())) {
<<<<<<< HEAD:transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/TrinoStruct.java
        ((TrinoData) value).writeToBlock(rowBlockBuilder);
=======
        PrestoWrapper.writeToBlock(value, rowBlockBuilder);
>>>>>>> 757697e (WIP: Rebase on master branch):transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/data/PrestoRowData.java
      } else {
        if (_block == null) {
          rowBlockBuilder.appendNull();
        } else {
          field.getType().appendTo(_block, i, rowBlockBuilder);
        }
      }
      i++;
    }
    mutable.closeEntry();
    _block = _rowType.getObject(mutable.build(), 0);
  }

  @Override
  public List<Object> fields() {
    ArrayList<Object> fields = new ArrayList<>();
    for (int i = 0; i < _block.getPositionCount(); i++) {
      Type elementType = _rowType.getFields().get(i).getType();
      Object element = readNativeValue(elementType, _block, i);
      fields.add(TrinoWrapper.createStdData(element, elementType, _stdFactory));
    }
    return fields;
  }

  @Override
  public Object getUnderlyingData() {
    return _block;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _block = (Block) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _rowType.writeObject(blockBuilder, getUnderlyingData());
  }
}
