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
package com.linkedin.stdudfs.examples.avro;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.avro.common.AssertAvroUdf.*;
import static com.linkedin.stdudfs.avro.common.SchemaFactory.*;


public class TestArrayFillFunctionWrapper {
  @Test
  public void testArrayFill() {
    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{INTEGER, LONG}, new Object[]{1, 5L},
        Arrays.asList(1, 1, 1, 1, 1));

    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{STRING, LONG}, new Object[]{"1", 5L},
        Arrays.asList("1", "1", "1", "1", "1"));

    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{array(INTEGER), LONG},
        new Object[]{Arrays.asList(1), 5L},
        Arrays.asList(Arrays.asList(1), Arrays.asList(1), Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)));

    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{INTEGER, LONG}, new Object[]{1, 0L}, Arrays.asList());

    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{INTEGER, LONG}, new Object[]{1, null}, null);

    assertFunction(new ArrayFillFunctionWrapper(), new Schema[]{INTEGER, LONG}, new Object[]{null, 5}, null);
  }
}
