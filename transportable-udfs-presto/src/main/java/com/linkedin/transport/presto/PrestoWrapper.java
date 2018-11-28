/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArray;
import com.linkedin.transport.presto.data.PrestoBoolean;
import com.linkedin.transport.presto.data.PrestoInteger;
import com.linkedin.transport.presto.data.PrestoLong;
import com.linkedin.transport.presto.data.PrestoMap;
import com.linkedin.transport.presto.data.PrestoString;
import com.linkedin.transport.presto.data.PrestoStruct;
import com.linkedin.transport.presto.types.PrestoArrayType;
import com.linkedin.transport.presto.types.PrestoBooleanType;
import com.linkedin.transport.presto.types.PrestoIntegerType;
import com.linkedin.transport.presto.types.PrestoLongType;
import com.linkedin.transport.presto.types.PrestoMapType;
import com.linkedin.transport.presto.types.PrestoStringType;
import com.linkedin.transport.presto.types.PrestoStructType;
import io.airlift.slice.Slice;

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
    } else if (prestoType.getJavaType() == boolean.class) {
      return new PrestoBoolean((boolean) prestoData);
    } else if (prestoType.getJavaType() == Slice.class) {
      return new PrestoString((Slice) prestoData);
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
    } else if (prestoType instanceof ArrayType) {
      return new PrestoArrayType((ArrayType) prestoType);
    } else if (prestoType instanceof MapType) {
      return new PrestoMapType((MapType) prestoType);
    } else if (prestoType instanceof RowType) {
      return new PrestoStructType(((RowType) prestoType));
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
