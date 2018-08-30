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
package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.*;
import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestArrayFillFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new ArrayFillFunctionWrapper());
  }

  @Test
  public void testArrayFill() {

    assertFunction("array_fill(1, 5)", new ArrayType(INTEGER), ImmutableList.of(1, 1, 1, 1, 1));

    assertFunction("array_fill('1', 5)", new ArrayType(createVarcharType(1)),
        ImmutableList.of("1", "1", "1", "1", "1"));
    assertFunction("array_fill(true, 5)", new ArrayType(BOOLEAN), ImmutableList.of(true, true, true, true, true));

    assertFunction("array_fill(array[1], 5)", new ArrayType(new ArrayType(INTEGER)),
        ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1),
            ImmutableList.of(1)));

    assertFunction("array_fill(1, 0)", new ArrayType(INTEGER), ImmutableList.of());

    assertFunction("array_fill(1, null)", new ArrayType(INTEGER), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testArrayFillUnkownNonNullable() {
    assertFunction("array_fill(null, 1)", new ArrayType(UNKNOWN), null);
  }
}
