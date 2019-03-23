/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.transport.compile.UDFProperties;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


/**
 * Encapsulates configuration related to a Transport UDF project required by a {@link WrapperGenerator} such as
 * UDF metadata and the output directories for the wrappers and corresponding resources
 */
public class ProjectContext {

  private UDFProperties _udfProperties;
  private File _sourcesOutputDir;
  private File _resourcesOutputDir;

  public UDFProperties getUdfProperties() {
    return _udfProperties;
  }

  public File getSourcesOutputDir() {
    return _sourcesOutputDir;
  }

  public File getResourcesOutputDir() {
    return _resourcesOutputDir;
  }

  @VisibleForTesting
  ProjectContext(UDFProperties udfProperties, File sourcesOutputDir, File resourcesOutputDir) {
    _udfProperties = udfProperties;
    _sourcesOutputDir = sourcesOutputDir;
    _resourcesOutputDir = resourcesOutputDir;
  }

  public ProjectContext(File udfPropertiesFile, File sourcesOutputDir, File resourcesOutputDir) {
    try (FileReader reader = new FileReader(udfPropertiesFile)) {
      _udfProperties = UDFProperties.fromJson(reader);
    } catch (IOException e) {
      throw new RuntimeException("Could not read UDF properties file: " + udfPropertiesFile, e);
    }
    _sourcesOutputDir = sourcesOutputDir;
    _resourcesOutputDir = resourcesOutputDir;
  }
}
