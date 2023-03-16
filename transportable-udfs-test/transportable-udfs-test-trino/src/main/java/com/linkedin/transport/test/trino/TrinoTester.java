/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.test.spi.TestCase;
import com.linkedin.transport.test.spi.types.TestType;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SessionTestUtils;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.SqlFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.spi.function.FunctionId;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.LocalQueryRunner;
import io.trino.type.InternalTypeManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.*;


public class TrinoTester implements SqlStdTester {

  private StdFactory _stdFactory;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _toPlatformTestOutputConverter;
  private Session _session;
  private FeaturesConfig  _featuresConfig;
  private LocalQueryRunner _runner;
  private QueryAssertions _queryAssertions;

  public TrinoTester() {
    _stdFactory = null;
    _sqlFunctionCallGenerator = new TrinoSqlFunctionCallGenerator();
    _toPlatformTestOutputConverter = new ToTrinoTestOutputConverter();
    _session = SessionTestUtils.TEST_SESSION;
    _featuresConfig = new FeaturesConfig();
    _runner = LocalQueryRunner.builder(_session).withFeaturesConfig(_featuresConfig).build();
    _queryAssertions = new QueryAssertions(_runner);
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {
    // Refresh Trino state during every setup call
    for (List<Class<? extends StdUDF>> stdUDFImplementations : topLevelStdUDFClassesAndImplementations.values()) {
      for (Class<? extends StdUDF> stdUDF : stdUDFImplementations) {
        _runner.addFunctions(new InternalFunctionBundle(new SqlFunction[]{new TrinoTestStdUDFWrapper(stdUDF)}));
      }
    }
  }

  @Override
  public StdFactory getStdFactory() {
    if (_stdFactory == null) {
      FunctionBinding functionBinding = new FunctionBinding(
          new FunctionId("test"),
          new BoundSignature("test", UNKNOWN, ImmutableList.of()),
          ImmutableMap.of(),
          ImmutableMap.of());
      _stdFactory = new TrinoFactory(
          functionBinding,
          _runner.getMetadata(),
          _runner.getFunctionManager(),
          _session, InternalTypeManager.TESTING_TYPE_MANAGER);
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
  public void check(TestCase testCase) {
    String functionName = testCase.getFunctionCall().getFunctionName();
    List<Object> parameters = testCase.getFunctionCall().getParameters();
    List<TestType> testTypes = testCase.getFunctionCall().getInferredParameterTypes();
    List<String> functionArguments = new ArrayList<>();
    for (int i = 0; i < parameters.size(); ++i) {
      functionArguments.add(_sqlFunctionCallGenerator.getFunctionCallArgumentString(parameters.get(i), testTypes.get(i)));
    }
    Object expectedOutputType = getPlatformType(testCase.getExpectedOutputType());
    QueryAssertions.ExpressionAssertProvider expressionAssertProvider = _queryAssertions.function(functionName, functionArguments);
    assertThat(expressionAssertProvider).hasType((Type) expectedOutputType).isEqualTo(testCase.getExpectedOutput());
  }
}
