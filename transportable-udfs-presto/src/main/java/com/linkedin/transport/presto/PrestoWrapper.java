/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArray;
import com.linkedin.transport.presto.data.PrestoBoolean;
import com.linkedin.transport.presto.data.PrestoBinary;
import com.linkedin.transport.presto.data.PrestoDouble;
import com.linkedin.transport.presto.data.PrestoFloat;
import com.linkedin.transport.presto.data.PrestoInteger;
import com.linkedin.transport.presto.data.PrestoLong;
import com.linkedin.transport.presto.data.PrestoMap;
import com.linkedin.transport.presto.data.PrestoString;
import com.linkedin.transport.presto.data.PrestoStruct;
import com.linkedin.transport.presto.types.PrestoArrayType;
import com.linkedin.transport.presto.types.PrestoBooleanType;
import com.linkedin.transport.presto.types.PrestoBinaryType;
import com.linkedin.transport.presto.types.PrestoDoubleType;
import com.linkedin.transport.presto.types.PrestoFloatType;
import com.linkedin.transport.presto.types.PrestoIntegerType;
import com.linkedin.transport.presto.types.PrestoLongType;
import com.linkedin.transport.presto.types.PrestoMapType;
import com.linkedin.transport.presto.types.PrestoStringType;
import com.linkedin.transport.presto.types.PrestoStructType;
import com.linkedin.transport.presto.types.PrestoUnknownType;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
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

import static java.lang.Math.*;


public final class PrestoWrapper {

  private PrestoWrapper() {
  }

  public static StdData createStdData(Object prestoData, Type prestoType, StdFactory stdFactory) {
    if (prestoData == null) {
      return null;
    }
    if (prestoType instanceof IntegerType) {
      // Presto represents SQL Integers (i.e., corresponding to IntegerType above) as long or Long
      // Therefore, to pass it to the PrestoInteger class, we first cast it to Long, then extract
      // the int value.
      return new PrestoInteger(((Long) prestoData).intValue());
    } else if (prestoType instanceof BigintType) {
      return new PrestoLong((long) prestoData);
    } else if (prestoType instanceof BooleanType) {
      return new PrestoBoolean((boolean) prestoData);
    } else if (prestoType instanceof VarcharType) {
      return new PrestoString((Slice) prestoData);
    } else if (prestoType instanceof RealType) {
      return new PrestoFloat((float) prestoData);
    } else if (prestoType instanceof DoubleType) {
      return new PrestoDouble((double) prestoData);
    } else if (prestoType instanceof VarbinaryType) {
      return new PrestoBinary((Slice) prestoData);
    } else if (prestoType instanceof ArrayType) {
      return new PrestoArray((Block) prestoData, (ArrayType) prestoType, stdFactory);
    } else if (prestoType instanceof MapType) {
      return new PrestoMap((Block) prestoData, prestoType, stdFactory);
    } else if (prestoType instanceof RowType) {
      return new PrestoStruct((Block) prestoData, prestoType, stdFactory);
    }
    assert false : "Unrecognized Presto Type: " + prestoType.getClass();
    return null;
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
      return new PrestoStructType(((RowType) prestoType));
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
