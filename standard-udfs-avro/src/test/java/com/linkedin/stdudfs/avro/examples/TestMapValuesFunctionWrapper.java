package com.linkedin.stdudfs.avro.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.avro.common.AssertAvroUdf.*;
import static com.linkedin.stdudfs.avro.common.SchemaFactory.*;


public class TestMapValuesFunctionWrapper {
  @Test
  public void testMapValues() {
    assertFunction(new MapValuesFunctionWrapper(), new Schema[]{map(STRING, INTEGER)},
        new Object[]{ImmutableMap.of("1", 4, "2", 5, "3", 6)}, ImmutableList.of(4, 5, 6));

    assertFunction(new MapValuesFunctionWrapper(), new Schema[]{map(STRING, STRING)},
        new Object[]{ImmutableMap.of("1", "4", "2", "5", "3", "6")}, ImmutableList.of("4", "5", "6"));

    assertFunction(new MapValuesFunctionWrapper(), new Schema[]{map(STRING, STRING)}, new Object[]{null}, null);
  }
}
