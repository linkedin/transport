/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.avro.typesystem;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.avro.AvroFactory;
import com.linkedin.stdudfs.typesystem.AbstractBoundVariables;
import com.linkedin.stdudfs.typesystem.AbstractTypeFactory;
import com.linkedin.stdudfs.typesystem.AbstractTypeInference;
import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
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
  protected StdFactory createStdFactory(AbstractBoundVariables<Schema> boundVariables) {
    return new AvroFactory(boundVariables);
  }

  @Override
  protected AbstractTypeFactory<Schema> getTypeFactory() {
    return new AvroTypeFactory();
  }
}
