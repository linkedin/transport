/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import org.testng.annotations.Test;


public class TestPrestoWrapperGenerator extends AbstractTestWrapperGenerator {

  @Override
  WrapperGenerator getWrapperGenerator() {
    return new PrestoWrapperGenerator();
  }

  @Test
  public void testPrestoWrapperGenerator() {
    testWrapperGenerator("inputs/sample-udf-metadata.json", "outputs/sample-udf-metadata/presto/sources",
        "outputs/sample-udf-metadata/presto/resources");
  }
}
