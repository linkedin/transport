/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import io.trino.metadata.BoundVariables;
import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import java.util.List;
import java.util.Map;


public class TrinoTester extends AbstractTestFunctions implements SqlStdTester {

  private StdFactory _stdFactory;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _toPlatformTestOutputConverter;

  public TrinoTester() {
    _stdFactory = null;
    _sqlFunctionCallGenerator = new TrinoSqlFunctionCallGenerator();
    _toPlatformTestOutputConverter = new ToTrinoTestOutputConverter();
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {
    // Refresh Trino state during every setup call
    initTestFunctions();
    for (List<Class<? extends StdUDF>> stdUDFImplementations : topLevelStdUDFClassesAndImplementations.values()) {
      for (Class<? extends StdUDF> stdUDF : stdUDFImplementations) {
        registerScalarFunction(new TrinoTestStdUDFWrapper(stdUDF));
      }
    }
  }

  @Override
  public StdFactory getStdFactory() {
    if (_stdFactory == null) {
      _stdFactory = new TrinoFactory(new BoundVariables(ImmutableMap.of(), ImmutableMap.of()),
          this.functionAssertions.getMetadata());
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
