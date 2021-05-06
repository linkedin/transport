/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF;
import io.trino.metadata.TypeVariableConstraint;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.trino.metadata.Signature.*;


public class TestGetTypeVariableConstraints {

  private void assertGetTypeVariableConstraints(List<String> inputParameterSignatures, String outputParameterSignature,
      List<TypeVariableConstraint> expectedTypeVariableConstraints) {
    ExampleStdUDF exampleStdUDF = new ExampleStdUDF(inputParameterSignatures, outputParameterSignature);
    Assert.assertEqualsNoOrder(StdUdfWrapper.getTypeVariableConstraintsForStdUdf(exampleStdUDF).toArray(),
        expectedTypeVariableConstraints.toArray());
  }

  @Test
  public void testGetTypeVariableConstraints() {
    assertGetTypeVariableConstraints(ImmutableList.of("array(varchar)"), "varchar", ImmutableList.of());

    assertGetTypeVariableConstraints(ImmutableList.of("K", "V"), "map(K,V)",
        ImmutableList.of(typeVariable("K"), typeVariable("V")));

    assertGetTypeVariableConstraints(ImmutableList.of("K", "K"), "bigint", ImmutableList.of(typeVariable("K")));

    assertGetTypeVariableConstraints(ImmutableList.of("bigint", "bigint"), "K", ImmutableList.of(typeVariable("K")));

    assertGetTypeVariableConstraints(ImmutableList.of("array(map(K,varchar))", "row(V,integer)"), "map(integer,S)",
        ImmutableList.of(typeVariable("K"), typeVariable("S"), typeVariable("V")));
  }

  private class ExampleStdUDF extends StdUDF {

    private List<String> _inputParameterSignatures;
    private String _outputParameterSignature;

    public ExampleStdUDF(List<String> inputParameterSignatures, String outputParameterSignature) {
      _inputParameterSignatures = inputParameterSignatures;
      _outputParameterSignature = outputParameterSignature;
    }

    @Override
    public List<String> getInputParameterSignatures() {
      return _inputParameterSignatures;
    }

    @Override
    public String getOutputParameterSignature() {
      return _outputParameterSignature;
    }

    @Override
    public int numberOfArguments() {
      return 0;
    }
  }
}
