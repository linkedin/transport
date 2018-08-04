package com.linkedin.stdudfs.presto.data;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.presto.PrestoWrapper;
import java.util.Iterator;

import static com.facebook.presto.spi.type.TypeUtils.*;


public class PrestoArray implements StdArray, PlatformData {

  final StdFactory _stdFactory;
  Block _block;
  BlockBuilder _mutable;
  Type _elementType;

  public PrestoArray(Block block, Type elementType, StdFactory stdFactory) {
    _block = block;
    _elementType = elementType;
    _stdFactory = stdFactory;
  }

  public PrestoArray(Type elementType, int expectedEntries, StdFactory stdFactory) {
    _block = null;
    _mutable = elementType.createBlockBuilder(new BlockBuilderStatus(), expectedEntries);
    _elementType = elementType;
    _stdFactory = stdFactory;
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
      _mutable = _elementType.createBlockBuilder(new BlockBuilderStatus(), 1);
    }
    PrestoWrapper.writeStdDataToBlock(e, _mutable);
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
}
