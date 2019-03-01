/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public interface UDFInterface2 extends TopLevelStdUDF {

  @Override
  default String getFunctionName() {
    return "";
  }

  @Override
  default String getFunctionDescription() {
    return "";
  }
}