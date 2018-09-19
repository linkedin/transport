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
package com.linkedin.stdudfs.examples.presto;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestStructCreateByIndexFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new StructCreateByIndexFunctionWrapper());
  }

  @Test
  public void testStructCreateByIndexFunction() {

    assertFunction("cast(struct_create_by_index('x', 'y') as row(a varchar, b varchar)).a", VARCHAR, "x");

    assertFunction("cast(struct_create_by_index('x', 'y') as row(a varchar, b varchar)).b", VARCHAR, "y");

    assertFunction("cast(struct_create_by_index(1, array[1]) as row(a integer, b array(integer))).a", INTEGER, 1);

    assertFunction("cast(struct_create_by_index(1, array[1]) as row(a integer, b array(integer))).b",
        new ArrayType(INTEGER), ImmutableList.of(1));

    assertFunction(
        "cast(struct_create_by_index(cast(null as integer), cast(null as integer)) as row(A integer, B integer))",
        RowType.from(ImmutableList.of(new RowType.Field(Optional.of("a"), INTEGER),
            new RowType.Field(Optional.of("b"), INTEGER))), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByIndexFunctionError1() {
    assertFunction("struct_create_by_index(null, null)", RowType.from(
        ImmutableList.of(new RowType.Field(Optional.of("field0"), UNKNOWN),
            new RowType.Field(Optional.of("field1"), UNKNOWN))), null);
  }
}
