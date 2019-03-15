/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdString;
import java.util.List;


public class UDFWithMultipleInterfaces2 extends AbstractUDFImplementingInterface implements OverloadedUDF1 {

  @Override
  public String getFunctionName() {
    return "udf_with_multiple_interfaces_2";
  }

  @Override
  public String getFunctionDescription() {
    return "UDF with multiple interfaces 2";
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of();
  }

  @Override
  public String getOutputParameterSignature() {
    return "boolean";
  }

  @Override
  public StdString eval() {
    return null;
  }
}