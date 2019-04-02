/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;


public abstract class AbstractTestWrapperGenerator {

  private static File _sourcesOutputDir;
  private static File _resourcesOutputDir;

  @BeforeClass
  void setup() {
    _sourcesOutputDir = Files.createTempDir();
    _resourcesOutputDir = Files.createTempDir();
  }

  @AfterMethod
  void cleanOutputDirectories() {
    try {
      FileUtils.cleanDirectory(_sourcesOutputDir);
      FileUtils.cleanDirectory(_resourcesOutputDir);
    } catch (IOException e) {
      throw new RuntimeException("Could not clean test output directory", e);
    }
  }

  @AfterClass
  void teardown() {
    _sourcesOutputDir.delete();
    _resourcesOutputDir.delete();
  }

  void testWrapperGenerator(String udfMetadataFileResource, String expectedSourcesOutputFolderResource) {
    testWrapperGenerator(udfMetadataFileResource, expectedSourcesOutputFolderResource, null);
  }

  void testWrapperGenerator(String udfMetadataFileResource, String expectedSourcesOutputFolderResource,
      String expectedResourcesOutputFolderResource) {
    WrapperGeneratorContext context =
        new WrapperGeneratorContext(TestUtils.getUDFMetadataFromResource(udfMetadataFileResource),
            _sourcesOutputDir, _resourcesOutputDir);

    getWrapperGenerator().generateWrappers(context);

    final Path expectedSourcesOutputPath;
    final Path expectedResourcesOutputPath;
    try {
      expectedSourcesOutputPath = Paths.get(
          Thread.currentThread().getContextClassLoader().getResource(expectedSourcesOutputFolderResource).toURI());
      if (expectedResourcesOutputFolderResource != null) {
        expectedResourcesOutputPath = Paths.get(
            Thread.currentThread().getContextClassLoader().getResource(expectedResourcesOutputFolderResource).toURI());
      } else {
        // No resources directory specified, so compare to an empty dir
        File tmpEmptyResourcesDir = Files.createTempDir();
        tmpEmptyResourcesDir.deleteOnExit();
        expectedResourcesOutputPath = tmpEmptyResourcesDir.toPath();
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("Error constructing URI", e);
    }

    TestUtils.assertDirectoriesAreEqual(_sourcesOutputDir.toPath(), expectedSourcesOutputPath);
    TestUtils.assertDirectoriesAreEqual(_resourcesOutputDir.toPath(), expectedResourcesOutputPath);
  }

  abstract WrapperGenerator getWrapperGenerator();
}
