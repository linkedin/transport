/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.test.spi.SqlTester;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionId;
import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.Type;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.trino.TrinoTypeFactory;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import java.util.List;
import java.util.Map;

import static io.trino.type.UnknownType.UNKNOWN;


public class TrinoTester extends AbstractTestFunctions implements SqlTester {

  private TypeFactory _typeFactory;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _toPlatformTestOutputConverter;

  public TrinoTester() {
    _typeFactory = null;
    _sqlFunctionCallGenerator = new TrinoSqlFunctionCallGenerator();
    _toPlatformTestOutputConverter = new ToTrinoTestOutputConverter();
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> topLevelUDFClassesAndImplementations) {
    // Refresh Trino state during every setup call
    initTestFunctions();
    for (List<Class<? extends UDF>> udfImplementations : topLevelUDFClassesAndImplementations.values()) {
      for (Class<? extends UDF> udf : udfImplementations) {
        registerScalarFunction(new TrinoTestTrinoUDF(udf));
      }
    }
  }

  @Override
  public TypeFactory getTypeFactory() {
    if (_typeFactory == null) {
      FunctionBinding functionBinding = new FunctionBinding(
          new FunctionId("test"),
          new BoundSignature("test", UNKNOWN, ImmutableList.of()),
          ImmutableMap.of(),
          ImmutableMap.of());
      _typeFactory = new TrinoTypeFactory(
          functionBinding,
          this.functionAssertions.getMetadata());
    }
    return _typeFactory;
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
