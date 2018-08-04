package com.linkedin.stdudfs.avro.examples;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.avro.common.AssertAvroUdf.*;
import static com.linkedin.stdudfs.avro.common.SchemaFactory.*;


public class TestStructCreateByIndexFunctionWrapper {
  @Test
  public void testStructCreateByIndex() {
    assertFunction(new StructCreateByIndexFunctionWrapper(), new Schema[]{STRING, STRING}, new Object[]{"x", "y"},
        ImmutableList.of("x", "y"));

    assertFunction(new StructCreateByIndexFunctionWrapper(), new Schema[]{STRING, STRING}, new Object[]{null, "y"},
        null);

    assertFunction(new StructCreateByIndexFunctionWrapper(), new Schema[]{STRING, STRING}, new Object[]{"x", null},
        null);
  }
}
