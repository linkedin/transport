/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.TopLevelStdUDF;


public abstract class AbstractUDFImplementingInterface extends StdUDF0<String> implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "abstract_udf_implementing_interface";
  }

  @Override
  public String getFunctionDescription() {
    return "Abstract UDF implementing interface";
  }
}