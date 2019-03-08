/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public class Constants {
  public static final String UDF_RESOURCE_FILE_PATH = "META-INF/transport-udfs/udf-properties.json";

  public static final String UDF_OVERLOADING_RULE = String.format(
      "A UDF class should implement %s either directly or indirectly through an interface only. Implementing %s through"
          + " a superclass is not supported as of now. If the requirement is to reuse another UDF, please use"
          + " composition instead of inheritance.",
      TopLevelStdUDF.class.getSimpleName(), TopLevelStdUDF.class.getSimpleName()
  );

  public static final String INTERFACE_NOT_IMPLEMENTED_ERROR =
      String.format(
          "Transport UDF should implement %s interface either directly or indirectly through an interface only.",
          TopLevelStdUDF.class.getSimpleName());

  public static final String SUPERCLASS_IMPLEMENTS_INTERFACE_ERROR = String.format(
      "The superclass of the UDF implements %s. %s",
      TopLevelStdUDF.class.getSimpleName(), UDF_OVERLOADING_RULE);

  public static final String MULTIPLE_INTERFACES_ERROR = String.format(
      "More than one interface implements %s. %s",
      TopLevelStdUDF.class.getSimpleName(), UDF_OVERLOADING_RULE);

  public static final String CLASS_SHOULD_NOT_OVERRIDE_INTERFACE_METHODS_ERROR =
      String.format("UDF class should not override %s methods implemented by a parent interface.",
          TopLevelStdUDF.class.getSimpleName());

  private Constants() {
  }
}
