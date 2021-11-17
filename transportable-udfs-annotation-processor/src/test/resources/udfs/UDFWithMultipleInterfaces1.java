/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.UDF0;
import java.util.List;


public class UDFWithMultipleInterfaces1 extends UDF0<Boolean> implements OverloadedUDF1, OverloadedUDF2 {

  @Override
  public String getFunctionName() {
    return "udf_with_multiple_interfaces_1";
  }

  @Override
  public String getFunctionDescription() {
    return "UDF with multiple interfaces 1";
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
  public Boolean eval() {
    return null;
  }
}