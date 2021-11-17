/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.typesystem;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.avro.AvroFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.AbstractTypeFactory;
import com.linkedin.transport.typesystem.AbstractTypeInference;
import com.linkedin.transport.typesystem.AbstractTypeSystem;
import org.apache.avro.Schema;


public class AvroTypeInference extends AbstractTypeInference<Schema> {
  @Override
  protected AbstractTypeSystem<Schema> getTypeSystem() {
    return new AvroTypeSystem();
  }

  @Override
  protected AbstractBoundVariables<Schema> createBoundVariables() {
    return new AvroBoundVariables();
  }

  @Override
  protected TypeFactory createStdFactory(AbstractBoundVariables<Schema> boundVariables) {
    return new AvroFactory(boundVariables);
  }

  @Override
  protected AbstractTypeFactory<Schema> getTypeFactory() {
    return new AvroTypeFactory();
  }
}
