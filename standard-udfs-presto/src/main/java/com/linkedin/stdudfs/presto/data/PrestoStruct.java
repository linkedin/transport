/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.presto.PrestoWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;


public class PrestoStruct extends PrestoData implements StdStruct {

  final RowType _rowType;
  final StdFactory _stdFactory;
  Block _block;

  public PrestoStruct(Type rowType, StdFactory stdFactory) {
    _rowType = (RowType) rowType;
    _stdFactory = stdFactory;
  }

  public PrestoStruct(Block block, Type rowType, StdFactory stdFactory) {
    this(rowType, stdFactory);
    _block = block;
  }

  public PrestoStruct(List<Type> fieldTypes, StdFactory stdFactory) {
    _stdFactory = stdFactory;
    _rowType = RowType.anonymous(fieldTypes);
  }

  public PrestoStruct(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
    _stdFactory = stdFactory;
    List<RowType.Field> fields = IntStream.range(0, fieldNames.size())
        .mapToObj(i -> new RowType.Field(Optional.ofNullable(fieldNames.get(i)), fieldTypes.get(i)))
        .collect(Collectors.toList());
    _rowType = RowType.from(fields);
  }

  @Override
  public StdData getField(int index) {
    int position = PrestoWrapper.checkedIndexToBlockPosition(_block, index);
    if (position == -1) {
      return null;
    }
    Type elementType = _rowType.getFields().get(position).getType();
    Object element = readNativeValue(elementType, _block, position);
    return PrestoWrapper.createStdData(element, elementType, _stdFactory);
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
    return PrestoWrapper.createStdData(element, elementType, _stdFactory);
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
        ((PrestoData) value).writeToBlock(rowBlockBuilder);
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
        ((PrestoData) value).writeToBlock(rowBlockBuilder);
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
      fields.add(PrestoWrapper.createStdData(element, elementType, _stdFactory));
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
