/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.trino.data.TrinoData;
import com.linkedin.transport.trino.data.TrinoArrayData;
import com.linkedin.transport.trino.data.TrinoRowData;
import com.linkedin.transport.trino.data.TrinoMapData;
import com.linkedin.transport.trino.types.TrinoArrayType;
import com.linkedin.transport.trino.types.TrinoBooleanType;
import com.linkedin.transport.trino.types.TrinoBinaryType;
import com.linkedin.transport.trino.types.TrinoDoubleType;
import com.linkedin.transport.trino.types.TrinoFloatType;
import com.linkedin.transport.trino.types.TrinoIntegerType;
import com.linkedin.transport.trino.types.TrinoLongType;
import com.linkedin.transport.trino.types.TrinoMapType;
import com.linkedin.transport.trino.types.TrinoStringType;
import com.linkedin.transport.trino.types.TrinoRowType;
import com.linkedin.transport.trino.types.TrinoUnknownType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.UnknownType;

import static io.trino.spi.type.BigintType.*;
import static io.trino.spi.type.BooleanType.*;
import static io.trino.spi.type.DoubleType.*;
import static io.trino.spi.type.IntegerType.*;
import static io.trino.spi.type.VarbinaryType.*;
import static io.trino.spi.type.VarcharType.*;
import static io.trino.spi.StandardErrorCode.*;
import static java.lang.Float.*;
import static java.lang.Math.*;
import static java.lang.String.*;
import java.nio.ByteBuffer;

public final class TrinoWrapper {

  private TrinoWrapper() {
  }

  public static Object createStdData(Object trinoData, Type trinoType, StdFactory stdFactory) {
    if (trinoData == null) {
      return null;
    }
    if (trinoType instanceof IntegerType) {
      // Trino represents SQL Integers (i.e., corresponding to IntegerType above) as long or Long
      // Therefore, we first cast trinoData to Long, then extract the int value.
      return ((Long) trinoData).intValue();
    } else if (trinoType instanceof BigintType || trinoType.getJavaType() == boolean.class
        || trinoType instanceof DoubleType) {
      return trinoData;
    } else if (trinoType instanceof VarcharType) {
      return ((Slice) trinoData).toStringUtf8();
    } else if (trinoType instanceof RealType) {
      // Trino represents SQL Reals (i.e., corresponding to RealType above) as long or Long
      // Therefore, to pass it to the TrinoFloat class, we first cast it to Long, extract
      // the int value and convert it the int bits to float.
      long value = (long) trinoData;
      int floatValue;
      try {
        floatValue = toIntExact(value);
      } catch (ArithmeticException e) {
        throw new TrinoException(GENERIC_INTERNAL_ERROR,
            format("Value (%sb) is not a valid single-precision float", Long.toBinaryString(value)));
      }
      return intBitsToFloat(floatValue);
    } else if (trinoType instanceof VarbinaryType) {
      return ((Slice) trinoData).toByteBuffer();
    } else if (trinoType instanceof ArrayType) {
      return new TrinoArrayData((Block) trinoData, (ArrayType) trinoType, stdFactory);
    } else if (trinoType instanceof MapType) {
      return new TrinoMapData((Block) trinoData, trinoType, stdFactory);
    } else if (trinoType instanceof RowType) {
      return new TrinoRowData((Block) trinoData, trinoType, stdFactory);
    }
    assert false : "Unrecognized Trino Type: " + trinoType.getClass();
    return null;
  }

  public static Object getPlatformData(Object transportData) {
    if (transportData == null) {
      return null;
    }
    if (transportData instanceof Integer) {
      return ((Number) transportData).longValue();
    } else if (transportData instanceof Long) {
      return ((Long) transportData).longValue();
    } else if (transportData instanceof Float) {
      return (long) floatToIntBits((Float) transportData);
    } else if (transportData instanceof Double) {
      return ((Double) transportData).doubleValue();
    } else if (transportData instanceof Boolean) {
      return ((Boolean) transportData).booleanValue();
    } else if (transportData instanceof String) {
      return Slices.utf8Slice((String) transportData);
    } else if (transportData instanceof ByteBuffer) {
      return Slices.wrappedBuffer(((ByteBuffer) transportData).array());
    } else {
      return ((PlatformData) transportData).getUnderlyingData();
    }
  }

  public static void writeToBlock(Object transportData, BlockBuilder blockBuilder) {
    if (transportData == null) {
      blockBuilder.appendNull();
    } else {
      if (transportData instanceof Integer) {
        // This looks a bit strange, but the call to writeLong is correct here. INTEGER does not have a writeInt method for
        // some reason. It uses BlockBuilder.writeInt internally.
        INTEGER.writeLong(blockBuilder, (Integer) transportData);
      } else if (transportData instanceof Long) {
        BIGINT.writeLong(blockBuilder, (Long) transportData);
      } else if (transportData instanceof Float) {
        INTEGER.writeLong(blockBuilder, floatToIntBits((Float) transportData));
      } else if (transportData instanceof Double) {
        DOUBLE.writeDouble(blockBuilder, (Double) transportData);
      } else if (transportData instanceof Boolean) {
        BOOLEAN.writeBoolean(blockBuilder, (Boolean) transportData);
      } else if (transportData instanceof String) {
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice((String) transportData));
      } else if (transportData instanceof ByteBuffer) {
        VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer((ByteBuffer) transportData));
      } else {
        ((TrinoData) transportData).writeToBlock(blockBuilder);
      }
    }
  }

  public static StdType createStdType(Object trinoType) {
    if (trinoType instanceof IntegerType) {
      return new TrinoIntegerType((IntegerType) trinoType);
    } else if (trinoType instanceof BigintType) {
      return new TrinoLongType((BigintType) trinoType);
    } else if (trinoType instanceof BooleanType) {
      return new TrinoBooleanType((BooleanType) trinoType);
    } else if (trinoType instanceof VarcharType) {
      return new TrinoStringType((VarcharType) trinoType);
    } else if (trinoType instanceof RealType) {
      return new TrinoFloatType((RealType) trinoType);
    } else if (trinoType instanceof DoubleType) {
      return new TrinoDoubleType((DoubleType) trinoType);
    } else if (trinoType instanceof VarbinaryType) {
      return new TrinoBinaryType((VarbinaryType) trinoType);
    } else if (trinoType instanceof ArrayType) {
      return new TrinoArrayType((ArrayType) trinoType);
    } else if (trinoType instanceof MapType) {
      return new TrinoMapType((MapType) trinoType);
    } else if (trinoType instanceof RowType) {
      return new TrinoRowType(((RowType) trinoType));
    } else if (trinoType instanceof UnknownType) {
      return new TrinoUnknownType(((UnknownType) trinoType));
    }
    assert false : "Unrecognized Trino Type: " + trinoType.getClass();
    return null;
  }

  /**
   * @return index if the index is in range, -1 otherwise.
   */
  public static int checkedIndexToBlockPosition(Block block, long index) {
    int blockLength = block.getPositionCount();
    if (index >= 0 && index < blockLength) {
      return toIntExact(index);
    }
    return -1; // -1 indicates that the element is out of range and the calling function should return null
  }
}