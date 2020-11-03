/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.typesystem;

import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.AbstractTestBoundVariables;
import com.linkedin.transport.typesystem.AbstractTypeSystem;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


@Test
public class TestAvroBoundVariables extends AbstractTestBoundVariables<Schema> {

  @Override
  protected AbstractTypeSystem<Schema> getTypeSystem() {
    return new AvroTypeSystem();
  }

  @Override
  protected AbstractBoundVariables<Schema> createBoundVariables() {
    return new AvroBoundVariables();
  }
}
