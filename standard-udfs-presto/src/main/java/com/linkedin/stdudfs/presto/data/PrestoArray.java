/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.presto.PrestoWrapper;
import java.util.Iterator;

import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;


public class PrestoArray extends PrestoData implements StdArray {

  private final StdFactory _stdFactory;
  private final ArrayType _arrayType;
  private final Type _elementType;

  private Block _block;
  private BlockBuilder _mutable;

  public PrestoArray(Block block, ArrayType arrayType, StdFactory stdFactory) {
    _block = block;
    _arrayType = arrayType;
    _elementType = arrayType.getElementType();
    _stdFactory = stdFactory;
  }

  public PrestoArray(ArrayType arrayType, int expectedEntries, StdFactory stdFactory) {
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
  public StdData get(int idx) {
    Block sourceBlock = _mutable == null ? _block : _mutable;
    int position = PrestoWrapper.checkedIndexToBlockPosition(sourceBlock, idx);
    Object element = readNativeValue(_elementType, sourceBlock, position);
    return PrestoWrapper.createStdData(element, _elementType, _stdFactory);
  }

  @Override
  public void add(StdData e) {
    if (_mutable == null) {
      _mutable = _elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
    }
    ((PrestoData) e).writeToBlock(_mutable);
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
  public Iterator<StdData> iterator() {
    return new Iterator<StdData>() {
      Block sourceBlock = _mutable == null ? _block : _mutable;
      int size = PrestoArray.this.size();
      int position = 0;

      @Override
      public boolean hasNext() {
        return position != size;
      }

      @Override
      public StdData next() {
        Object element = readNativeValue(_elementType, sourceBlock, position);
        position++;
        return PrestoWrapper.createStdData(element, _elementType, _stdFactory);
      }
    };
  }

  @Override
  public void writeToBlock(BlockBuilder blockBuilder) {
    _arrayType.writeObject(blockBuilder, getUnderlyingData());
  }
}
