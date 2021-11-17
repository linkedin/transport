/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.linkedin.transport.api.udf.TopLevelUDF;


public interface OverloadedUDF2 extends TopLevelUDF {

  @Override
  default String getFunctionName() {
    return "overloaded_udf_2";
  }

  @Override
  default String getFunctionDescription() {
    return "Overload UDF 2";
  }
}