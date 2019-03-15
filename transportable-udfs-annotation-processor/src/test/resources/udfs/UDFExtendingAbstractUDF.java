/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class UDFExtendingAbstractUDF extends AbstractUDF implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "udf_extending_abstract_udf";
  }

  @Override
  public String getFunctionDescription() {
    return "UDF extending Abstract UDF";
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
  public StdString eval() {
    return null;
  }
}