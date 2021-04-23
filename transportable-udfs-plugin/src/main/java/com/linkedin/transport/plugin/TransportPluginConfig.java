/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import java.io.File;
import org.gradle.api.Project;


/**
 * Custom configuration for the {@link TransportPlugin}.
 */
public class TransportPluginConfig {

  /**
   * Available gradle property names.
   */
  private static final String MAIN_SOURCE_SET_NAME_PROP = "transport-plugin-main-source-set";
  private static final String TEST_SOURCE_SET_NAME_PROP = "transport-plugin-test-source-set";
  private static final String OUTPUT_DIR_PROP = "transport-plugin-output-dir";

  /**
   * The main source set used by the plugin.
   */
  public String mainSourceSetName;
  /**
   * The test source set used by the plugin.
   */
  public String testSourceSetName;
  /**
   * The output code-gen directory, relative the to the project directory.
   */
  public File outputDirFile;

  /**
   * Create a config object from the gradle {@link Project}.
   *
   * On creation, we attempt to populate config values using gradle properties or set to default values.
   */
  public TransportPluginConfig(Project project) {
    mainSourceSetName = getPropertyOrDefault(project, MAIN_SOURCE_SET_NAME_PROP, "main");
    testSourceSetName = getPropertyOrDefault(project, TEST_SOURCE_SET_NAME_PROP, "test");
    outputDirFile = project.hasProperty(OUTPUT_DIR_PROP)
        ? project.file(project.property(OUTPUT_DIR_PROP).toString())
        : project.getBuildDir(); // Build dir by default
  }

  /**
   * Retrieve a string property from a gradle project if it has been set, a default value otherwise.
   */
  private String getPropertyOrDefault(Project project, String propertyName, String defaultValue) {
    return project.hasProperty(propertyName)
        ? project.property(propertyName).toString()
        : defaultValue;
  }
}
