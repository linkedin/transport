/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.TestCase;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.trino.StdUdfWrapper;
import com.linkedin.transport.trino.TransportConnector;
import com.linkedin.transport.trino.TransportConnectorMetadata;
import com.linkedin.transport.trino.TransportFunctionProvider;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.spi.Plugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.type.Type;
import io.trino.sql.SqlPath;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import io.trino.type.InternalTypeManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.*;


public class TrinoTester implements SqlStdTester {

  private StdFactory _stdFactory;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _toPlatformTestOutputConverter;
  private Session _session;
  private DistributedQueryRunner _runner;
  private QueryAssertions _queryAssertions;

  public TrinoTester() throws Exception {
    _stdFactory = null;
    _sqlFunctionCallGenerator = new TrinoSqlFunctionCallGenerator();
    _toPlatformTestOutputConverter = new ToTrinoTestOutputConverter();
    SqlPath sqlPath = new SqlPath(List.of(new CatalogSchemaName("LINKEDIN", "TRANSPORT")), "LINKEDIN.TRANSPORT");
    _session = TestingSession.testSessionBuilder().setPath(sqlPath).setClientCapabilities((Set) Arrays.stream(
        ClientCapabilities.values()).map(Enum::toString).collect(ImmutableSet.toImmutableSet())).build();
    _runner = DistributedQueryRunner.builder(_session).build();
    _queryAssertions = new QueryAssertions(_runner);
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {
    Map<FunctionId, StdUdfWrapper> functions = new HashMap<>();
    // Refresh Trino state during every setup call
    for (List<Class<? extends StdUDF>> stdUDFImplementations : topLevelStdUDFClassesAndImplementations.values()) {
      for (Class<? extends StdUDF> stdUDF : stdUDFImplementations) {
        StdUdfWrapper function = new TrinoTestStdUDFWrapper(stdUDF);
        functions.put(function.getFunctionMetadata().getFunctionId(), function);
      }
    }
    FunctionProvider functionProvider = new TransportFunctionProvider(functions);
    ConnectorMetadata connectorMetadata = new TransportConnectorMetadata(functions);
    Connector connector = new TransportConnector(connectorMetadata, functionProvider);
    ConnectorFactory connectorFactory = new ConnectorFactory() {
      @Override
      public String getName() {
        return "TRANSPORT";
      }
      @Override
      public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        return connector;
      }
    };

    _runner.installPlugin(new Plugin() {
      @Override
      public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(connectorFactory);
      }
    });
    _runner.createCatalog("LINKEDIN", "TRANSPORT", Collections.emptyMap());
  }

  @Override
  public StdFactory getStdFactory() {
    if (_stdFactory == null) {
      FunctionBinding functionBinding = new FunctionBinding(
          new FunctionId("test"),
          new BoundSignature(new CatalogSchemaFunctionName("LINKEDIN", "TRANSPORT", "test"), UNKNOWN, ImmutableList.of()),
          ImmutableMap.of(),
          ImmutableMap.of());
      _stdFactory = new TrinoFactory(functionBinding, new TrinoTestFunctionDependencies(InternalTypeManager.TESTING_TYPE_MANAGER, _runner));
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
    Object expectedOutput = testCase.getExpectedOutput();
    if (expectedOutput instanceof Row) {
      expectedOutput = ((Row) expectedOutput).getFields();
    }
    QueryAssertions.ExpressionAssertProvider expressionAssertProvider = _queryAssertions.function(functionName, functionArguments);
    assertThat(expressionAssertProvider).hasType((Type) expectedOutputType).isEqualTo(expectedOutput);
  }
}