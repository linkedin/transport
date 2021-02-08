/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArrayData;
import com.linkedin.transport.presto.data.PrestoData;
import com.linkedin.transport.presto.data.PrestoMapData;
import com.linkedin.transport.presto.data.PrestoRowData;
import com.linkedin.transport.presto.types.PrestoArrayType;
import com.linkedin.transport.presto.types.PrestoBooleanType;
import com.linkedin.transport.presto.types.PrestoBinaryType;
import com.linkedin.transport.presto.types.PrestoDoubleType;
import com.linkedin.transport.presto.types.PrestoFloatType;
import com.linkedin.transport.presto.types.PrestoIntegerType;
import com.linkedin.transport.presto.types.PrestoLongType;
import com.linkedin.transport.presto.types.PrestoMapType;
import com.linkedin.transport.presto.types.PrestoStringType;
import com.linkedin.transport.presto.types.PrestoRowType;
import com.linkedin.transport.presto.types.PrestoUnknownType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.type.UnknownType;
import java.nio.ByteBuffer;

import static io.prestosql.spi.type.BigintType.*;
import static io.prestosql.spi.type.BooleanType.*;
import static io.prestosql.spi.type.DoubleType.*;
import static io.prestosql.spi.type.IntegerType.*;
import static io.prestosql.spi.type.VarbinaryType.*;
import static io.prestosql.spi.type.VarcharType.*;
import static io.prestosql.spi.StandardErrorCode.*;
import static java.lang.Float.*;
import static java.lang.Math.*;
import static java.lang.String.*;


public final class PrestoWrapper {

  private PrestoWrapper() {
  }

  public static Object createStdData(Object prestoData, Type prestoType, StdFactory stdFactory) {
    if (prestoData == null) {
      return null;
    }
    if (prestoType instanceof IntegerType) {
      // Presto represents SQL Integers (i.e., corresponding to IntegerType above) as long or Long
      // Therefore, we first cast prestoData to Long, then extract the int value.
      return ((Long) prestoData).intValue();
    } else if (prestoType instanceof BigintType || prestoType.getJavaType() == boolean.class
        || prestoType instanceof DoubleType) {
      return prestoData;
    } else if (prestoType instanceof VarcharType) {
      return ((Slice) prestoData).toStringUtf8();
    } else if (prestoType instanceof RealType) {
      // Presto represents SQL Reals (i.e., corresponding to RealType above) as long or Long
      // Therefore, to pass it to the PrestoFloat class, we first cast it to Long, extract
      // the int value and convert it the int bits to float.
      long value = (long) prestoData;
      int floatValue;
      try {
        floatValue = toIntExact(value);
      } catch (ArithmeticException e) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR,
            format("Value (%sb) is not a valid single-precision float", Long.toBinaryString(value)));
      }
      return intBitsToFloat(floatValue);
    } else if (prestoType instanceof VarbinaryType) {
      return ((Slice) prestoData).toByteBuffer();
    } else if (prestoType instanceof ArrayType) {
      return new PrestoArrayData((Block) prestoData, (ArrayType) prestoType, stdFactory);
    } else if (prestoType instanceof MapType) {
      return new PrestoMapData((Block) prestoData, prestoType, stdFactory);
    } else if (prestoType instanceof RowType) {
      return new PrestoRowData((Block) prestoData, prestoType, stdFactory);
    }
    assert false : "Unrecognized Presto Type: " + prestoType.getClass();
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
        ((PrestoData) transportData).writeToBlock(blockBuilder);
      }
    }
  }

  public static StdType createStdType(Object prestoType) {
    if (prestoType instanceof IntegerType) {
      return new PrestoIntegerType((IntegerType) prestoType);
    } else if (prestoType instanceof BigintType) {
      return new PrestoLongType((BigintType) prestoType);
    } else if (prestoType instanceof BooleanType) {
      return new PrestoBooleanType((BooleanType) prestoType);
    } else if (prestoType instanceof VarcharType) {
      return new PrestoStringType((VarcharType) prestoType);
    } else if (prestoType instanceof RealType) {
      return new PrestoFloatType((RealType) prestoType);
    } else if (prestoType instanceof DoubleType) {
      return new PrestoDoubleType((DoubleType) prestoType);
    } else if (prestoType instanceof VarbinaryType) {
      return new PrestoBinaryType((VarbinaryType) prestoType);
    } else if (prestoType instanceof ArrayType) {
      return new PrestoArrayType((ArrayType) prestoType);
    } else if (prestoType instanceof MapType) {
      return new PrestoMapType((MapType) prestoType);
    } else if (prestoType instanceof RowType) {
      return new PrestoRowType(((RowType) prestoType));
    } else if (prestoType instanceof UnknownType) {
      return new PrestoUnknownType(((UnknownType) prestoType));
    }
    assert false : "Unrecognized Presto Type: " + prestoType.getClass();
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
