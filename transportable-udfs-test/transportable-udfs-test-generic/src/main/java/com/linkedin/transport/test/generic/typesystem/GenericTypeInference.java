/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.typesystem;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.test.generic.GenericFactory;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.AbstractTypeFactory;
import com.linkedin.transport.typesystem.AbstractTypeInference;
import com.linkedin.transport.typesystem.AbstractTypeSystem;


public class GenericTypeInference extends AbstractTypeInference<TestType> {
  @Override
  protected AbstractTypeSystem<TestType> getTypeSystem() {
    return new GenericTypeSystem();
  }

  @Override
  protected AbstractBoundVariables<TestType> createBoundVariables() {
    return new GenericBoundVariables();
  }

  @Override
  protected StdFactory createStdFactory(AbstractBoundVariables<TestType> boundVariables) {
    return new GenericFactory(boundVariables);
  }

  @Override
  protected AbstractTypeFactory<TestType> getTypeFactory() {
    return new GenericTypeFactory();
  }
}
