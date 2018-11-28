/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.transport.avro.common.AssertAvroUdf.*;
import static com.linkedin.transport.avro.common.SchemaFactory.*;


public class TestMapFromTwoArraysFunctionWrapper {
  @Test
  public void testMapFromTwoArrays() {
    assertFunction(new MapFromTwoArraysFunctionWrapper(), new Schema[]{array(STRING), array(STRING)},
        new Object[]{ImmutableList.of("1", "2"), ImmutableList.of("a", "b")}, ImmutableMap.of("1", "a", "2", "b"));

    assertFunction(new MapFromTwoArraysFunctionWrapper(), new Schema[]{array(STRING), array(array(STRING))},
        new Object[]{null, ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("b"))}, null);

    assertFunction(new MapFromTwoArraysFunctionWrapper(), new Schema[]{array(STRING), array(array(STRING))},
        new Object[]{ImmutableList.of("1", "2"), null}, null);
  }
}
