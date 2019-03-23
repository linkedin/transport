/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.linkedin.transport.compile.UDFProperties;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;


class TestUtils {

  private TestUtils() {
  }

  static UDFProperties getUDFPropertiesFromResource(String resource) {
    try (InputStreamReader reader = new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource))) {
      return UDFProperties.fromJson(reader);
    } catch (IOException e) {
      throw new RuntimeException("Could not read UDF properties from resource: " + resource, e);
    }
  }

  /**
   * Asserts that the contents of both directories (and their subdirectories) are equal
   */
  static void assertDirectoriesAreEqual(Path actualDir, Path expectedDir) {
    Set<Path> actualRelativeFilePaths = getRelativeFilePaths(actualDir);
    Set<Path> expectedRelativeFilePaths = getRelativeFilePaths(expectedDir);

    Assert.assertEquals(actualRelativeFilePaths, expectedRelativeFilePaths,
        String.format("Either the number or names of files in the directories %s and %s are not equal", actualDir,
            expectedDir));

    for (Path p : expectedRelativeFilePaths) {
      File actualFile = actualDir.resolve(p).toFile();
      File expectedFile = expectedDir.resolve(p).toFile();
      try {
        Assert.assertEquals(FileUtils.readFileToString(actualFile), FileUtils.readFileToString(expectedFile),
            String.format("Contents of file %s and %s are not equal", actualFile, expectedFile));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns a {@link Set} of relative {@link Path}s to all files in the directory and its subdirectories
   */
  private static Set<Path> getRelativeFilePaths(Path directory) {
    Collection<File> files1 = FileUtils.listFiles(directory.toFile(), null, true);
    return files1.stream().map(x -> directory.relativize(x.toPath())).collect(Collectors.toSet());
  }
}
