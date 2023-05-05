/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF0;
import java.util.List;


public class UDFOverridingInterfaceMethod extends StdUDF0<Boolean> implements OverloadedUDF1 {

  @Override
  public String getFunctionName() {
    return "udf_overriding_interface_method";
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