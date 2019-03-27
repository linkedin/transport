/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.transport.compile.TransportUDFMetadata;
import java.io.File;


/**
 * Encapsulates configuration related to a Transport UDF project required by a {@link WrapperGenerator} such as
 * UDF metadata and the output directories for the wrappers and corresponding resources
 */
public class ProjectContext {

  private final TransportUDFMetadata _transportUdfMetadata;
  private final File _sourcesOutputDir;
  private final File _resourcesOutputDir;

  public TransportUDFMetadata getTransportUdfMetadata() {
    return _transportUdfMetadata;
  }

  public File getSourcesOutputDir() {
    return _sourcesOutputDir;
  }

  public File getResourcesOutputDir() {
    return _resourcesOutputDir;
  }

  @VisibleForTesting
  ProjectContext(TransportUDFMetadata transportUdfMetadata, File sourcesOutputDir, File resourcesOutputDir) {
    _transportUdfMetadata = transportUdfMetadata;
    _sourcesOutputDir = sourcesOutputDir;
    _resourcesOutputDir = resourcesOutputDir;
  }

  public ProjectContext(File udfPropertiesFile, File sourcesOutputDir, File resourcesOutputDir) {
    _transportUdfMetadata = TransportUDFMetadata.fromJsonFile(udfPropertiesFile);
    _sourcesOutputDir = sourcesOutputDir;
    _resourcesOutputDir = resourcesOutputDir;
  }
}
