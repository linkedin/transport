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
package com.linkedin.stdudfs.presto;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.data.PrestoArray;
import com.linkedin.stdudfs.presto.data.PrestoBoolean;
import com.linkedin.stdudfs.presto.data.PrestoInteger;
import com.linkedin.stdudfs.presto.data.PrestoLong;
import com.linkedin.stdudfs.presto.data.PrestoMap;
import com.linkedin.stdudfs.presto.data.PrestoString;
import com.linkedin.stdudfs.presto.data.PrestoStruct;
import com.linkedin.stdudfs.presto.types.PrestoArrayType;
import com.linkedin.stdudfs.presto.types.PrestoBooleanType;
import com.linkedin.stdudfs.presto.types.PrestoIntegerType;
import com.linkedin.stdudfs.presto.types.PrestoLongType;
import com.linkedin.stdudfs.presto.types.PrestoMapType;
import com.linkedin.stdudfs.presto.types.PrestoStringType;
import com.linkedin.stdudfs.presto.types.PrestoStructType;
import io.airlift.slice.Slice;

import static java.lang.Math.toIntExact;


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
