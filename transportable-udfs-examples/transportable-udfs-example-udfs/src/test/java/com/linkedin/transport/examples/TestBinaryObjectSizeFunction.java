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

// Temporarily disable the tests for Trino. As the test infrastructure from Trino named QueryAssertions is used to
// run these test for Trino, QueryAssertions mandatory execute the function with the query in two formats: one with
// is the normal query (e.g. SELECT "binary_size"(a0) FROM (VALUES ROW(from_base64('Zm9v'))) t(a0);), the other
// is with "where RAND()>0" clause (e.g. SELECT "binary_size"(a0) FROM (VALUES ROW(from_base64('Zm9v'))) t(a0) where RAND()>0;)
// QueryAssertions verifies the output from both queries are equal otherwise the test fail.
// However, the execution of the query with where clause triggers the code of VariableWidthBlockBuilder.writeByte() to create
// the input byte array in Slice with an initial 32 byes capacity, while the execution of the query without where clause does not trigger
// the code of VariableWidthBlockBuilder.writeByte() and create the input byte array in Slice with the actual capacity of the content.
// Therefore, the outputs from both queries are different.
// TODO: https://github.com/linkedin/transport/issues/131
public class TestBinaryObjectSizeFunction extends AbstractStdUDFTest {
  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(BinaryObjectSizeFunction.class, ImmutableList.of(BinaryObjectSizeFunction.class));
  }

  @Test
  public void tesBinaryObjectSize() {
    if (!isTrinoTest()) {
      StdTester tester = getTester();
      ByteBuffer argTest1 = ByteBuffer.wrap("foo".getBytes());
      ByteBuffer argTest2 = ByteBuffer.wrap("".getBytes());
      ByteBuffer argTest3 = ByteBuffer.wrap("fooBar".getBytes());
      tester.check(functionCall("binary_size", argTest1), 3, "integer");
      tester.check(functionCall("binary_size", argTest2), 0, "integer");
      tester.check(functionCall("binary_size", argTest3), 6, "integer");
    }
  }
}
