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
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;


class CodegenUtils {

  private CodegenUtils() {
  }

  /**
   * Writes a source code file for a class to the provided output directory creating package directories if required
   *
   * @param outputDir Output directory
   * @param packageName Package name of the class (can be {@code null} for the default package)
   * @param className Class name
   * @param extension Extension of the file (can be {@code null} for no extension)
   * @param code Source code of the class
   * @throws IOException
   */
  static void writeCodeFile(Path outputDir, String packageName, String className, String extension,
      String code) throws IOException {
    Preconditions.checkArgument(Files.notExists(outputDir) || Files.isDirectory(outputDir),
        "Output directory should not exist or must be a directory", outputDir);
    Preconditions.checkArgument(!StringUtils.isBlank(className), "Class name should not be null or an empty string");
    Preconditions.checkNotNull(code, "Code should not be null");

    final Path packageDir;
    if (!StringUtils.isBlank(packageName)) {
      packageDir = Paths.get(outputDir.toString(), packageName.split("\\."));
    } else {
      packageDir = outputDir;
    }
    Files.createDirectories(packageDir);

    String fileName = className + (StringUtils.isBlank(extension) ? "" : "." + extension);
    Path outputPath = packageDir.resolve(fileName);
    Files.write(outputPath, code.getBytes());
  }

  /**
   * Writes a list of services to a specified service file
   *
   * @param outputDir Output directory
   * @param serviceFilePath Path of the service file relative to the output directory
   * @param services List of services
   * @throws IOException
   */
  static void writeServiceFile(Path outputDir, Path serviceFilePath, Collection<String> services)
      throws IOException {
    Preconditions.checkArgument(Files.notExists(outputDir) || Files.isDirectory(outputDir),
        "Output directory should not exist or must be a directory", outputDir);
    Preconditions.checkNotNull(serviceFilePath, "Service file path should not be null");

    Path resolvedServiceFilePath = outputDir.resolve(serviceFilePath);
    Files.createDirectories(resolvedServiceFilePath.getParent());

    try (BufferedWriter writer = Files.newBufferedWriter(resolvedServiceFilePath)) {
      for (String service : services) {
        writer.write(service + "\n");
      }
    }
  }
}
