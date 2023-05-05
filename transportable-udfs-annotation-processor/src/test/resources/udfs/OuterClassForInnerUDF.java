/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class OuterClassForInnerUDF {
  public class InnerUDF extends StdUDF0<String> implements TopLevelStdUDF {

    @Override
    public String getFunctionName() {
      return "inner_udf";
    }

    @Override
    public String getFunctionDescription() {
      return "Inner UDF";
    }

    @Override
    public List<String> getInputParameterSignatures() {
      return ImmutableList.of();
    }

    @Override
    public String getOutputParameterSignature() {
      return "varchar";
    }

    @Override
    public String eval() {
      return null;
    }
  }
}