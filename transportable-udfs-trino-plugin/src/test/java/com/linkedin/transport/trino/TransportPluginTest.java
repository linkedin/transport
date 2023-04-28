/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.sql.SqlPath;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TransportPluginTest {
  private static final String TRANSPORT_UDF_REPO_DIR =  "transport-udf-repo";

  @Test
  public void testTransportPluginInitialization() {
    String udfRepoDir = getClass().getClassLoader().getResource(TRANSPORT_UDF_REPO_DIR).getPath();
    SqlPath sqlPath = new SqlPath("LINKEDIN.TRANSPORT");
    FeaturesConfig featuresConfig = new FeaturesConfig();
    Session session = TestingSession.testSessionBuilder().setPath(sqlPath).setClientCapabilities((Set) Arrays.stream(
        ClientCapabilities.values()).map(Enum::toString).collect(ImmutableSet.toImmutableSet())).build();
    LocalQueryRunner queryRunner = LocalQueryRunner.builder(session).withFeaturesConfig(featuresConfig).build();
    queryRunner.installPlugin(new TransportPlugin());
    queryRunner.createCatalog("LINKEDIN", "TRANSPORT", ImmutableMap.of("transport.udf.repo", udfRepoDir));
    String query = "SELECT array_element_at(array[1,2,3], 2)";
    MaterializedResult result = queryRunner.execute(query);
    Assert.assertEquals(result.getRowCount(), 1);
    Assert.assertEquals(((int) result.getMaterializedRows().get(0).getField(0)), 3);
  }

  @Test
  public void testTransportUDFClassLoader() {
    String udfRepoDir = getClass().getClassLoader().getResource(TRANSPORT_UDF_REPO_DIR).getPath();
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
