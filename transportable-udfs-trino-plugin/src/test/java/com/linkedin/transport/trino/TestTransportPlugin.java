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
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.SqlPath;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestTransportPlugin {
  private final String udfRepoDir = TestTransportPlugin.class.getClassLoader()
      .getResource("transport-udf-repo")
      .getPath();
  private DistributedQueryRunner queryRunner;

  @BeforeClass
  public void setUp() throws Exception {
    SqlPath sqlPath = new SqlPath(List.of(new CatalogSchemaName("linkedin", "transport")), "linkedin_transport");
    Session session = TestingSession.testSessionBuilder().setPath(sqlPath).setClientCapabilities((Set) Arrays.stream(
        ClientCapabilities.values()).map(Enum::toString).collect(ImmutableSet.toImmutableSet())).build();
    queryRunner = DistributedQueryRunner.builder(session).build();
    queryRunner.installPlugin(new TransportPlugin());
    queryRunner.createCatalog("linkedin", "transport", ImmutableMap.of("transport.udf.repo", udfRepoDir));
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
    String query = "SELECT linkedin.transport.array_element_at(array[1,2,3], 2)";
    MaterializedResult result = queryRunner.execute(query);
    Assert.assertEquals(result.getRowCount(), 1);
    Assert.assertEquals(((int) result.getMaterializedRows().get(0).getField(0)), 3);

    String camelCaseQuery = "SELECT linkedin.transport.Array_Element_At(array[1,2,3], 2)";
    MaterializedResult camelCaseResult = queryRunner.execute(camelCaseQuery);
    Assert.assertEquals(camelCaseResult.getRowCount(), 1);
    Assert.assertEquals(((int) camelCaseResult.getMaterializedRows().get(0).getField(0)), 3);
  }

  @Test
  public void testTransportUdfInShowFunctions() {
    String showFunctionQuery = "SHOW FUNCTIONS FROM linkedin.transport LIKE 'array_element_at'";
    MaterializedResult showFunctionResult = queryRunner.execute(showFunctionQuery);
    Assert.assertEquals(showFunctionResult.getRowCount(), 1);
    Assert.assertEquals(((String) showFunctionResult.getMaterializedRows().get(0).getField(0)), "array_element_at");
  }

  @Test
  public void testTransportUDFClassLoader() {
    TransportConfig config = new TransportConfig();
    config.setTransportUdfRepo(udfRepoDir);
    TransportConnector connector = new TransportConnector(config);
    TransportFunctionProvider fnProvider = (TransportFunctionProvider) connector.getFunctionProvider().get();

    ClassLoader parentStdUdfWrapperClassloader = StdUdfWrapper.class.getClassLoader();
    Set<ClassLoader> classLoaders = new HashSet<>();
    for (StdUdfWrapper udfWrapper : fnProvider.getFunctions().values()) {
      classLoaders.add(udfWrapper.getClass().getClassLoader());
      assertTrue(udfWrapper.getClass().getClassLoader() instanceof TransportUDFClassLoader);
      // the classloader of the superclass (StdUdfWrapper) should always be the same parent loader
      assertEquals(udfWrapper.getClass().getSuperclass().getClassLoader(), parentStdUdfWrapperClassloader);
    }
    // two UDF JARs are being loaded, so we expect two classloaders
    assertEquals(classLoaders.size(), 2);
  }
}