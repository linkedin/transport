/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.linkedin.transport.compile.TransportUDFMetadata;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;


class TestUtils {

  private TestUtils() {
  }

  static TransportUDFMetadata getUDFPropertiesFromResource(String resource) {
    try (InputStreamReader reader = new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource))) {
      return TransportUDFMetadata.fromJson(reader);
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

    // First check that the number and names of the files are the same
    Assert.assertEquals(actualRelativeFilePaths, expectedRelativeFilePaths,
        String.format("Either the number or names of files in the directories %s and %s are not equal", actualDir,
            expectedDir));

    // For every path in the expected directory, compare the contents of corresponding path in the actual directory.
    // We can be sure that there always exists a corresponding path in the actual directory since we have asserted that
    // above.
    for (Path p : expectedRelativeFilePaths) {
      Path expectedFile = expectedDir.resolve(p);
      Path actualFile = actualDir.resolve(p);
      try {
        Assert.assertEquals(Files.readAllLines(actualFile), Files.readAllLines(expectedFile),
            String.format("Contents of file %s and %s are not equal", actualFile, expectedFile));
      } catch (IOException e) {
        throw new RuntimeException(String.format("Error comparing files: %s and %s", actualFile, expectedFile), e);
      }
    }
  }

  /**
   * Returns a {@link Set} of relative {@link Path}s to all files in the directory and its subdirectories
   */
  private static Set<Path> getRelativeFilePaths(Path directory) {
    try (Stream<Path> recursivePaths = Files.walk(directory).filter(Files::isRegularFile)) {
      return recursivePaths.map(directory::relativize).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error traversing directory: %s", directory), e);
    }
  }
}
