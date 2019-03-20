/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public class Constants {
  public static final String UDF_RESOURCE_FILE_PATH = "META-INF/transport-udfs/udf-properties.json";

  public static final String INTERFACE_NOT_IMPLEMENTED_ERROR =
      String.format(
          "A Transport UDF should implement %s interface.",
          TopLevelStdUDF.class.getSimpleName());

  public static final String MORE_THAN_ONE_TYPE_OVERRIDING_ERROR =
      String.format(
          "%s methods should be overriden in only one class/interface in the type hierarchy.",
          TopLevelStdUDF.class.getSimpleName());

  private Constants() {
  }
}
