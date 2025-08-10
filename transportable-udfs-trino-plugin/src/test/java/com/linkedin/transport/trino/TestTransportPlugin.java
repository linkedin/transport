/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.SqlPath;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTransportPlugin {
  private static final String UDF_REPO = TestTransportPlugin.class.getClassLoader()
      .getResource("transport-udf-repo")
      .getPath();

  private DistributedQueryRunner queryRunner;
  private Session session;

  @BeforeClass
  public void setUp() throws Exception {
    SqlPath sqlPath = new SqlPath(List.of(new CatalogSchemaName("linkedin", "transport")), "linkedin_transport");
    session = TestingSession.testSessionBuilder()
        .setPath(sqlPath)
        .setClientCapabilities((Set<String>) Arrays.stream(ClientCapabilities.values())
            .map(Enum::toString)
            .collect(ImmutableSet.toImmutableSet()))
        .build();

    queryRunner = DistributedQueryRunner.builder(session)
        .build();

    queryRunner.installPlugin(new TransportPlugin());
    queryRunner.installPlugin(new TpchPlugin());

    queryRunner.createCatalog(
        "linkedin",
        "transport",
        ImmutableMap.of("transport.udf.repo", UDF_REPO));

    queryRunner.createCatalog(
        "tpch",
        "tpch",
        Map.of());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (queryRunner != null) {
      queryRunner.close();
      queryRunner = null;
    }
  }

  @Test
  public void testTransportUdfIsAccessible() {
    MaterializedResult result = queryRunner.execute(
        session,
        "SELECT linkedin.transport.array_element_at(ARRAY[1,2,3], 2)");
    Assert.assertEquals(result.getOnlyValue(), 3);
  }

  @Test
  public void testTransportUdfAppearsInShowFunctions() {
    MaterializedResult rows = queryRunner.execute(
        session,
        "SHOW FUNCTIONS FROM linkedin.transport LIKE 'array_element_at'");
    Assert.assertEquals(rows.getRowCount(), 1);
    Assert.assertEquals(rows.getMaterializedRows()
            .get(0)
            .getField(0),
        "array_element_at");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEachUdfGetsSeparateClassLoader() {
    TransportConfig cfg = new TransportConfig().setTransportUdfRepo(UDF_REPO);
    TransportConnector connector = new TransportConnector(cfg);
    TransportFunctionProvider provider =
        (TransportFunctionProvider) connector.getFunctionProvider().get();

    ClassLoader wrapperParent = StdUdfWrapper.class.getClassLoader();
    Set<ClassLoader> udfLoaders = new HashSet<>();

    provider.getFunctions().values().forEach(wrapper -> {
      ClassLoader cl = wrapper.getClass().getClassLoader();
      udfLoaders.add(cl);
      Assert.assertTrue(cl instanceof TransportUDFClassLoader,
          "UDF should be loaded by TransportUDFClassLoader");
      Assert.assertSame(wrapper.getClass()
              .getSuperclass()
              .getClassLoader(),
          wrapperParent,
          "StdUdfWrapper itself must be loaded by parent loader");
    });

    // two UDF JARs are being loaded, so we expect two classloaders
    Assert.assertEquals(udfLoaders.size(), 2);
  }
}
