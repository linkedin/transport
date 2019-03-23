/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import org.testng.annotations.Test;


public class TestHiveWrapperGenerator extends AbstractTestWrapperGenerator {

  @Override
  WrapperGenerator getWrapperGenerator() {
    return new HiveWrapperGenerator();
  }

  @Test
  public void testHiveWrapperGenerator() {
    testWrapperGenerator("inputs/sample-udf-properties.json", "outputs/sample-udf-properties/hive/sources");
  }
}
