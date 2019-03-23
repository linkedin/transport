/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.base.Preconditions;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;


class CodegenUtils {

  private CodegenUtils() {
  }

  static void writeCodeFile(Path outputDir, String packageName, String className, String extension,
      String code) throws IOException {
    Preconditions.checkArgument(Files.notExists(outputDir) || Files.isDirectory(outputDir),
        "Path %s exists but is not a directory", outputDir);

    Path outputDirectory = outputDir;
    if (!packageName.isEmpty()) {
      for (String packageComponent : packageName.split("\\.")) {
        outputDirectory = outputDirectory.resolve(packageComponent);
      }
      Files.createDirectories(outputDirectory);
    }

    Path outputPath = outputDirectory.resolve(className + "." + extension);
    Files.write(outputPath, code.getBytes());
  }

  static void writeServiceFile(Path outputDir, String serviceFileName, Collection<String> services)
      throws IOException {
    Preconditions.checkArgument(Files.notExists(outputDir) || Files.isDirectory(outputDir),
        "Path %s exists but is not a directory", outputDir);

    Path outputPath = outputDir.resolve(serviceFileName);
    Files.createDirectories(outputPath.getParent());

    try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
      for (String service : services) {
        writer.write(service + "\n");
      }
    }
  }
}
