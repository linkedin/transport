/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.typesystem;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.hive.HiveFactory;
import com.linkedin.stdudfs.typesystem.AbstractBoundVariables;
import com.linkedin.stdudfs.typesystem.AbstractTypeFactory;
import com.linkedin.stdudfs.typesystem.AbstractTypeInference;
import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class HiveTypeInference extends AbstractTypeInference<ObjectInspector> {

  @Override
  protected AbstractTypeSystem<ObjectInspector> getTypeSystem() {
    return new HiveTypeSystem();
  }

  @Override
  protected AbstractBoundVariables<ObjectInspector> createBoundVariables() {
    return new HiveBoundVariables();
  }

  @Override
  protected StdFactory createStdFactory(AbstractBoundVariables<ObjectInspector> boundVariables) {
    return new HiveFactory(boundVariables);
  }

  @Override
  protected AbstractTypeFactory<ObjectInspector> getTypeFactory() {
    return new HiveTypeFactory();
  }
}
