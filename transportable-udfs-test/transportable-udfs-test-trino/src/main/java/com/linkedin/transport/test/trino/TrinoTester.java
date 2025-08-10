/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.TestCase;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.trino.StdUdfWrapper;
import com.linkedin.transport.trino.TrinoFactory;
import com.linkedin.transport.trino.TransportConnector;
import com.linkedin.transport.trino.TransportConnectorMetadata;
import com.linkedin.transport.trino.TransportFunctionProvider;
import io.trino.client.ClientCapabilities;
import io.trino.metadata.FunctionBinding;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.type.Type;
import io.trino.sql.SqlPath;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import io.trino.type.InternalTypeManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;

import static io.trino.type.UnknownType.UNKNOWN;

public class TrinoTester implements SqlStdTester, AutoCloseable {
  private final DistributedQueryRunner queryRunner;

  private final io.trino.Session session;

  private StdFactory stdFactory;

  private final SqlFunctionCallGenerator sqlFunctionCallGenerator;
  private final ToPlatformTestOutputConverter toPlatformTestOutputConverter;

  public TrinoTester() throws Exception {
    sqlFunctionCallGenerator = new TrinoSqlFunctionCallGenerator();
    toPlatformTestOutputConverter = new ToTrinoTestOutputConverter();

    SqlPath sqlPath = new SqlPath(List.of(new CatalogSchemaName("LINKEDIN", "TRANSPORT")), "LINKEDIN_TRANSPORT");
    Set<String> caps = Arrays.stream(ClientCapabilities.values())
        .map(Enum::toString)
        .collect(ImmutableSet.toImmutableSet());

    session = TestingSession.testSessionBuilder()
        .setPath(sqlPath)
        .setClientCapabilities(caps)
        .build();

    queryRunner = DistributedQueryRunner.builder(session).build();
  }

  @Override
  public void setup(Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> udfMap) {
    Map<FunctionId, StdUdfWrapper> wrappers = new HashMap<>();
    for (List<Class<? extends StdUDF>> impls : udfMap.values()) {
      for (Class<? extends StdUDF> impl : impls) {
        StdUdfWrapper wrapper = new TrinoTestStdUDFWrapper(impl);
        wrappers.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
      }
    }

    FunctionProvider fp = new TransportFunctionProvider(wrappers);
    ConnectorMetadata cm = new TransportConnectorMetadata(wrappers);
    Connector connector = new TransportConnector(cm, fp);

    ConnectorFactory factory = new ConnectorFactory() {
      @Override public String getName() {
        return "transport";
      }
      @Override public Connector create(String catalog, Map<String, String> cfg, ConnectorContext ctx) {
        return connector;
      }
    };

    queryRunner.createCatalog(
        "LINKEDIN",
        "transport",
        ImmutableMap.of("connector.name", "transport",
            "transport.udf.repo", Path.of(".").toUri().toString()));
  }

  @Override
  public StdFactory getStdFactory() {
    if (stdFactory == null) {
      FunctionBinding binding = new FunctionBinding(
          new FunctionId("test"),
          new BoundSignature(
              new CatalogSchemaFunctionName("linkedin", "transport", "test"),
              UNKNOWN,
              ImmutableList.of()),
          ImmutableMap.of(),
          ImmutableMap.of());

      stdFactory = new TrinoFactory(
          binding,
          new TrinoTestFunctionDependencies(InternalTypeManager.TESTING_TYPE_MANAGER, queryRunner));
    }
    return stdFactory;
  }

  @Override public SqlFunctionCallGenerator getSqlFunctionCallGenerator() {
    return sqlFunctionCallGenerator;
  }
  @Override public ToPlatformTestOutputConverter getToPlatformTestOutputConverter() {
    return toPlatformTestOutputConverter;
  }

  @Override
  public void check(TestCase testCase) {
    String fnName = testCase.getFunctionCall().getFunctionName();
    List<Object> params = testCase.getFunctionCall().getParameters();
    List<TestType> paramTypes = testCase.getFunctionCall().getInferredParameterTypes();

    List<String> argSql = new ArrayList<>();
    for (int i = 0; i < params.size(); i++) {
      argSql.add(sqlFunctionCallGenerator.getFunctionCallArgumentString(params.get(i), paramTypes.get(i)));
    }

    String querySql = "SELECT linkedin.transport." + fnName + "(" + String.join(", ", argSql) + ")";
    MaterializedResult result = queryRunner.execute(session, querySql);

    Object expectedOutput = testCase.getExpectedOutput();
    if (expectedOutput instanceof Row) {
      expectedOutput = ((Row) expectedOutput).getFields();
    }
    Type expectedType = (Type) getPlatformType(testCase.getExpectedOutputType());

    Assertions.assertThat(result.getRowCount()).isEqualTo(1);
    Assertions.assertThat(result.getTypes().get(0)).isEqualTo(expectedType);
    Assertions.assertThat(result.getOnlyValue()).isEqualTo(expectedOutput);
  }

  @Override
  public void close() throws Exception {
    if (queryRunner != null) {
      queryRunner.close();
    }
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    }));
  }
}
