/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.avro.common.AssertAvroUdf.*;
import static com.linkedin.stdudfs.avro.common.SchemaFactory.*;


public class TestMapKeySetFunctionWrapper {
  @Test
  public void testMapKeySet() {
    assertFunction(new MapKeySetFunctionWrapper(), new Schema[]{map(STRING, INTEGER)},
        new Object[]{ImmutableMap.of("1", 4, "2", 5, "3", 6)}, ImmutableList.of("1", "2", "3"));

    assertFunction(new MapKeySetFunctionWrapper(), new Schema[]{map(STRING, STRING)},
        new Object[]{ImmutableMap.of("1", "4", "2", "5", "3", "6")}, ImmutableList.of("1", "2", "3"));

    assertFunction(new MapKeySetFunctionWrapper(), new Schema[]{map(STRING, STRING)}, new Object[]{null}, null);
  }
}
