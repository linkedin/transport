/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.TypeUtils.*;


public class TrinoStruct extends TrinoData implements StdStruct {

  final RowType _rowType;
  final StdFactory _stdFactory;
  Block _block;

  public TrinoStruct(Type rowType, StdFactory stdFactory) {
    _rowType = (RowType) rowType;
    _stdFactory = stdFactory;
  }

  public TrinoStruct(Block block, Type rowType, StdFactory stdFactory) {
    this(rowType, stdFactory);
    _block = block;
  }

  public TrinoStruct(List<Type> fieldTypes, StdFactory stdFactory) {
    _stdFactory = stdFactory;
    _rowType = RowType.anonymous(fieldTypes);
  }

  public TrinoStruct(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
    _stdFactory = stdFactory;
    List<RowType.Field> fields = IntStream.range(0, fieldNames.size())
        .mapToObj(i -> new RowType.Field(Optional.ofNullable(fieldNames.get(i)), fieldTypes.get(i)))
        .collect(Collectors.toList());
    _rowType = RowType.from(fields);
  }

  @Override
  public StdData getField(int index) {
    int position = TrinoWrapper.checkedIndexToBlockPosition(_block, index);
    if (position == -1) {
      return null;
    }
    Type elementType = _rowType.getFields().get(position).getType();
    Object element = readNativeValue(elementType, _block, position);
    return TrinoWrapper.createStdData(element, elementType, _stdFactory);
  }

  @Override
  public StdData getField(String name) {
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
  public void setField(int index, StdData value) {
    // TODO: This is not the right way to get this object. The status should be passed in from the invocation of the
    // function and propagated to here. See PRESTO-1359 for more details.
    BlockBuilderStatus blockBuilderStatus = new PageBuilderStatus().createBlockBuilderStatus();
    BlockBuilder mutable = _rowType.createBlockBuilder(blockBuilderStatus, 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.Field field : _rowType.getFields()) {
      if (i == index) {
        ((TrinoData) value).writeToBlock(rowBlockBuilder);
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
  public void setField(String name, StdData value) {
    BlockBuilder mutable = _rowType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.Field field : _rowType.getFields()) {
      if (field.getName().isPresent() && name.equals(field.getName().get())) {
        ((TrinoData) value).writeToBlock(rowBlockBuilder);
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
  public List<StdData> fields() {
    ArrayList<StdData> fields = new ArrayList<>();
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
