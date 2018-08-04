package com.linkedin.stdudfs.avro.examples;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.avro.common.AssertAvroUdf.*;
import static com.linkedin.stdudfs.avro.common.SchemaFactory.*;


public class TestArrayElementAtFunctionWrapper {
  @Test
  public void testArrayElementAt() {
    assertFunction(new ArrayElementAtFunctionWrapper(), new Schema[]{array(STRING), INTEGER},
        new Object[]{Arrays.asList("1", "2"), 1}, "2");

    assertFunction(new ArrayElementAtFunctionWrapper(), new Schema[]{array(INTEGER), INTEGER},
        new Object[]{Arrays.asList(1, 2), 1}, 2);

    assertFunction(new ArrayElementAtFunctionWrapper(), new Schema[]{array(array(STRING)), INTEGER},
        new Object[]{Arrays.asList(Arrays.asList("1"), Arrays.asList("2")), 1}, Arrays.asList("2"));

    assertFunction(new ArrayElementAtFunctionWrapper(), new Schema[]{array(INTEGER), INTEGER}, new Object[]{null, 1},
        null);

    assertFunction(new ArrayElementAtFunctionWrapper(), new Schema[]{array(INTEGER), INTEGER},
        new Object[]{Arrays.asList(1, 2), null}, null);
  }
}
