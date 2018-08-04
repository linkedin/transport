package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.RowType;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.presto.PrestoWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TypeUtils.*;


public class PrestoStruct implements StdStruct, PlatformData {

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

  private PrestoStruct(Optional<List<String>> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
    this(new RowType(fieldTypes, fieldNames), stdFactory);
  }

  public PrestoStruct(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
    this(Optional.of(fieldNames), fieldTypes, stdFactory);
  }

  public PrestoStruct(List<Type> fieldTypes, StdFactory stdFactory) {
    this(Optional.empty(), fieldTypes, stdFactory);
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
    for (RowType.RowField field : _rowType.getFields()) {
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

    BlockBuilder mutable = _rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.RowField field : _rowType.getFields()) {
      if (i == index) {
        PrestoWrapper.writeStdDataToBlock(value, rowBlockBuilder);
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
    BlockBuilder mutable = _rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
    BlockBuilder rowBlockBuilder = mutable.beginBlockEntry();
    int i = 0;
    for (RowType.RowField field : _rowType.getFields()) {
      if (field.getName().isPresent() && name.equals(field.getName().get())) {
        PrestoWrapper.writeStdDataToBlock(value, rowBlockBuilder);
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

  @Override  public Object getUnderlyingData() {
    return _block;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _block = (Block) value;
  }

  private InterleavedBlockBuilder createInterleavedBlockBuilder() {
    return new InterleavedBlockBuilder(
        _rowType.getFields().stream().map(field -> field.getType()).collect(Collectors.toList()),
        new BlockBuilderStatus(), _rowType.getFields().size());
  }
}
