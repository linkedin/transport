/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.google.common.collect.ImmutableList;
import java.util.List;


public class UDFExtendingAbstractUDFImplementingInterface extends AbstractUDFImplementingInterface {

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