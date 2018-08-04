package com.linkedin.stdudfs.avro.typesystem;

import com.linkedin.stdudfs.typesystem.AbstractTypeFactory;
import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
import org.apache.avro.Schema;


public class AvroTypeFactory extends AbstractTypeFactory<Schema> {
  @Override
  protected AbstractTypeSystem<Schema> getTypeSystem() {
    return new AvroTypeSystem();
  }
}
