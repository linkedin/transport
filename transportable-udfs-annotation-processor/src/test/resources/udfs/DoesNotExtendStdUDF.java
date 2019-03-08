/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public class DoesNotExtendStdUDF implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "does_not_extend_std_udf";
  }

  @Override
  public String getFunctionDescription() {
    return "Does not extend StdUDF";
  }
}