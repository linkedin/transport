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
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.TypeUtils.readNativeValue;

public class TrinoStruct extends TrinoData implements StdStruct {

  private final RowType rowType;
  private final StdFactory stdFactory;

  // Trino v446+: ROW values are represented as SqlRow
  private SqlRow rowData;

  public TrinoStruct(Type rowType, StdFactory stdFactory) {
    this.rowType = (RowType) rowType;
    this.stdFactory = stdFactory;
  }

  /** Prefer using this ctor if you already have a SqlRow from Trino. */
  public TrinoStruct(SqlRow sqlRow, Type rowType, StdFactory stdFactory) {
    this(rowType, stdFactory);
    this.rowData = sqlRow;
  }

  public TrinoStruct(List<Type> fieldTypes, StdFactory stdFactory) {
    this.stdFactory = stdFactory;
    this.rowType = RowType.anonymous(fieldTypes);
  }

  public TrinoStruct(List<String> fieldNames, List<Type> fieldTypes, StdFactory stdFactory) {
    this.stdFactory = stdFactory;
    List<RowType.Field> fields = IntStream.range(0, fieldNames.size())
        .mapToObj(i -> new RowType.Field(Optional.ofNullable(fieldNames.get(i)), fieldTypes.get(i)))
        .collect(Collectors.toList());
    this.rowType = RowType.from(fields);
  }

  @Override
  public StdData getField(int index) {
    if (rowData == null) {
      return null;
    }
    int offset = rowData.getRawIndex();
    Type fieldType = rowType.getFields().get(index).getType();
    Block fieldBlock = rowData.getRawFieldBlock(index);
    Object element = readNativeValue(fieldType, fieldBlock, offset);
    return TrinoWrapper.createStdData(element, fieldType, stdFactory);
  }

  @Override
  public StdData getField(String name) {
    if (rowData == null) {
      return null;
    }
    int idx = -1;
    Type t = null;
    for (int i = 0; i < rowType.getFields().size(); i++) {
      var f = rowType.getFields().get(i);
      if (f.getName().isPresent() && name.equals(f.getName().get())) {
        idx = i;
        t = f.getType();
        break;
      }
    }
    if (idx == -1) {
      return null;
    }
    int offset = rowData.getRawIndex();
    Block fieldBlock = rowData.getRawFieldBlock(idx);
    Object element = readNativeValue(t, fieldBlock, offset);
    return TrinoWrapper.createStdData(element, t, stdFactory);
  }

  @Override
  public void setField(int index, StdData value) {
    int fieldCount = rowType.getFields().size();
    List<Type> fieldTypes = rowType.getTypeParameters();
    Block[] fieldBlocks = new Block[fieldCount];

    int existingOffset = (rowData == null) ? 0 : rowData.getRawIndex();

    for (int i = 0; i < fieldCount; i++) {
      Type ft = fieldTypes.get(i);
      BlockBuilder bb = ft.createBlockBuilder(null, 1);

      if (i == index) {
        ((TrinoData) value).writeToBlock(bb);
      } else {
        if (rowData == null) {
          bb.appendNull();
        } else {
          // copy existing value at this row's offset
          ft.appendTo(rowData.getRawFieldBlock(i), existingOffset, bb);
        }
      }
      fieldBlocks[i] = bb.build();
    }

    // Build a single-row SqlRow at offset 0
    this.rowData = new SqlRow(0, fieldBlocks);
  }

  @Override
  public void setField(String name, StdData value) {
    int fieldCount = rowType.getFields().size();
    List<Type> fieldTypes = rowType.getTypeParameters();
    Block[] fieldBlocks = new Block[fieldCount];

    int targetIndex = -1;
    for (int i = 0; i < fieldCount; i++) {
      var f = rowType.getFields().get(i);
      if (f.getName().isPresent() && name.equals(f.getName().get())) {
        targetIndex = i;
        break;
      }
    }
    if (targetIndex == -1) {
      // Unknown field name; treat as no-op
      return;
    }

    int existingOffset = (rowData == null) ? 0 : rowData.getRawIndex();

    for (int i = 0; i < fieldCount; i++) {
      Type ft = fieldTypes.get(i);
      BlockBuilder bb = ft.createBlockBuilder(null, 1);

      if (i == targetIndex) {
        ((TrinoData) value).writeToBlock(bb);
      } else {
        if (rowData == null) {
          bb.appendNull();
        } else {
          ft.appendTo(rowData.getRawFieldBlock(i), existingOffset, bb);
        }
      }
      fieldBlocks[i] = bb.build();
    }

    this.rowData = new SqlRow(0, fieldBlocks);
  }

  @Override
  public List<StdData> fields() {
    ArrayList<StdData> out = new ArrayList<>();
    if (rowData == null) {
      return out;
    }
    int offset = rowData.getRawIndex();
    int count = rowType.getFields().size();
    for (int i = 0; i < count; i++) {
      Type t = rowType.getFields().get(i).getType();
      Block fieldBlock = rowData.getRawFieldBlock(i);
      Object element = readNativeValue(t, fieldBlock, offset);
      out.add(TrinoWrapper.createStdData(element, t, stdFactory));
    }
    return out;
  }

  @Override
  public Object getUnderlyingData() {
    return rowData;
  }

  @Override
  public void setUnderlyingData(Object value) {
    this.rowData = (SqlRow) value;
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    rowType.writeObject(blockBuilder, rowData);
  }
}
