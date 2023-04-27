/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableSet;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.sql.SqlPath;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TransportPluginTest {
  private static final String TRANSPORT_UDF_REPO_DIR =  "transport-udf-repo";

  @BeforeTest
  public void setup() throws IOException {
    File source = new File(getClass().getClassLoader().getResource(TRANSPORT_UDF_REPO_DIR).getFile());
    File destination = Paths.get(Paths.get("").toAbsolutePath().toString(), TRANSPORT_UDF_REPO_DIR).toFile();
    FileUtils.copyDirectory(source, destination);
  }

  @AfterTest
  void clean() throws IOException {
    FileUtils.deleteDirectory(Paths.get(Paths.get("").toAbsolutePath().toString(), TRANSPORT_UDF_REPO_DIR).toFile());
  }

  @Test
  public void testTransportPluginInitialization() {
    SqlPath sqlPath = new SqlPath("LINKEDIN.TRANSPORT");
    FeaturesConfig featuresConfig = new FeaturesConfig();
    Session session = TestingSession.testSessionBuilder().setPath(sqlPath).setClientCapabilities((Set) Arrays.stream(
        ClientCapabilities.values()).map(Enum::toString).collect(ImmutableSet.toImmutableSet())).build();
    LocalQueryRunner queryRunner = LocalQueryRunner.builder(session).withFeaturesConfig(featuresConfig).build();
    queryRunner.installPlugin(new TransportPlugin());
    queryRunner.createCatalog("LINKEDIN", "TRANSPORT", Collections.emptyMap());
    String query = "SELECT array_element_at(array[1,2,3], 2)";
    MaterializedResult result = queryRunner.execute(query);
    Assert.assertEquals(result.getRowCount(), 1);
    Assert.assertEquals(((int) result.getMaterializedRows().get(0).getField(0)), 3);
  }
}
