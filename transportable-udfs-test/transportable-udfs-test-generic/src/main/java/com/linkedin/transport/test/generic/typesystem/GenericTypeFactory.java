/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.typesystem;

import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.typesystem.AbstractTypeFactory;
import com.linkedin.transport.typesystem.AbstractTypeSystem;


public class GenericTypeFactory extends AbstractTypeFactory<TestType> {
  @Override
  protected AbstractTypeSystem<TestType> getTypeSystem() {
    return new GenericTypeSystem();
  }
}
