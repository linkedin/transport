/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.typesystem;

import com.linkedin.stdudfs.typesystem.AbstractBoundVariables;
import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class HiveBoundVariables extends AbstractBoundVariables<ObjectInspector> {
  @Override
  protected AbstractTypeSystem<ObjectInspector> getTypeSystem() {
    return new HiveTypeSystem();
  }
}
