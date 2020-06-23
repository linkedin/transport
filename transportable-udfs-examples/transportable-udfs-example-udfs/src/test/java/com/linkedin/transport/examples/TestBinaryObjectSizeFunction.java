/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.StdTester;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestBinaryObjectSizeFunction extends AbstractStdUDFTest {
  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(BinaryObjectSizeFunction.class, ImmutableList.of(BinaryObjectSizeFunction.class));
  }

  @Test
  public void tesBinaryObjectSize() {
    StdTester tester = getTester();
    ByteBuffer argTest1 = ByteBuffer.wrap("foo".getBytes());
    ByteBuffer argTest2 = ByteBuffer.wrap("".getBytes());
    ByteBuffer argTest3 = ByteBuffer.wrap("fooBar".getBytes());
    tester.check(functionCall("binary_size", argTest1), 3, "integer");
    tester.check(functionCall("binary_size", argTest2), 0, "integer");
    tester.check(functionCall("binary_size", argTest3), 6, "integer");
  }
}
