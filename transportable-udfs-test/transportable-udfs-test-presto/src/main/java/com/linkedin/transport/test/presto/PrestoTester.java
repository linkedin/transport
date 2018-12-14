/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.presto;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.presto.PrestoFactory;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import java.util.List;
import java.util.Map;


public class PrestoTester extends AbstractTestFunctions implements SqlStdTester {

  private StdFactory _stdFactory;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _toPlatformTestOutputConverter;

  public PrestoTester() {
    _stdFactory = null;
    _sqlFunctionCallGenerator = new PrestoSqlFunctionCallGenerator();
    _toPlatformTestOutputConverter = new ToPrestoTestOutputConverter();
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {
    // Refresh Presto state during every setup call
    initTestFunctions();
    for (List<Class<? extends StdUDF>> stdUDFImplementations : topLevelStdUDFClassesAndImplementations.values()) {
      for (Class<? extends StdUDF> stdUDF : stdUDFImplementations) {
        registerScalarFunction(new PrestoTestStdUDFWrapper(stdUDF));
      }
    }
  }

  @Override
  public StdFactory getStdFactory() {
    if (_stdFactory == null) {
      _stdFactory = new PrestoFactory(new BoundVariables(ImmutableMap.of(), ImmutableMap.of()),
          this.functionAssertions.getMetadata().getTypeManager(),
          this.functionAssertions.getMetadata().getFunctionRegistry());
    }
    return _stdFactory;
  }

  @Override
  public SqlFunctionCallGenerator getSqlFunctionCallGenerator() {
    return _sqlFunctionCallGenerator;
  }

  @Override
  public ToPlatformTestOutputConverter getToPlatformTestOutputConverter() {
    return _toPlatformTestOutputConverter;
  }

  @Override
  public void assertFunctionCall(String functionCallString, Object expectedOutputData, Object expectedOutputType) {
    assertFunction(functionCallString, (Type) expectedOutputType, expectedOutputData);
  }
}
