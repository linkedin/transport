/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.codegen.HiveWrapperGenerator;
import com.linkedin.transport.codegen.PrestoWrapperGenerator;
import com.linkedin.transport.codegen.SparkWrapperGenerator;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.linkedin.transport.plugin.DependencyConfigurationName.*;


/**
 * Stores default configurations for the Transport UDF plugin
 */
class Defaults {

  private Defaults() {
  }

  // The versions of the Transport and supported platforms to apply corresponding versions of the platform dependencies
  private static final Properties DEFAULT_VERSIONS;

  static {
    DEFAULT_VERSIONS = new Properties();
    try {
      DEFAULT_VERSIONS.load(
          Thread.currentThread().getContextClassLoader().getResourceAsStream("version-info.properties"));
    } catch (IOException e) {
      throw new RuntimeException("Error loading version-info.properties", e);
    }
  }

  static final List<DependencyConfiguration> MAIN_SOURCE_SET_DEPENDENCIES = ImmutableList.of(
      getDependencyConfiguration(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-api", "transport"),
      getDependencyConfiguration(ANNOTATION_PROCESSOR, "com.linkedin.transport:transportable-udfs-annotation-processor",
          "transport")
  );

  static final List<DependencyConfiguration> TEST_SOURCE_SET_DEPENDENCIES = ImmutableList.of(
      getDependencyConfiguration(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-test-api", "transport"),
      getDependencyConfiguration(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-generic", "transport")
  );

  static final List<PlatformConfiguration> DEFAULT_PLATFORMS = ImmutableList.of(
      new PlatformConfiguration(
          "presto",
          Language.JAVA,
          PrestoWrapperGenerator.class,
          ImmutableList.of(
              getDependencyConfiguration(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-presto",
                  "transport"),
              getDependencyConfiguration(COMPILE_ONLY, "com.facebook.presto:presto-main", "presto")
          ),
          ImmutableList.of(
              getDependencyConfiguration(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-presto",
                  "transport")
          )
      ),
      new PlatformConfiguration(
          "hive",
          Language.JAVA,
          HiveWrapperGenerator.class,
          ImmutableList.of(
              getDependencyConfiguration(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-hive", "transport"),
              getDependencyConfiguration(COMPILE_ONLY, "org.apache.hive:hive-exec", "hive")
          ),
          ImmutableList.of(
              getDependencyConfiguration(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-hive",
                  "transport")
          )
      ),
      new PlatformConfiguration(
          "spark",
          Language.SCALA,
          SparkWrapperGenerator.class,
          ImmutableList.of(
              getDependencyConfiguration(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-spark",
                  "transport"),
              getDependencyConfiguration(COMPILE_ONLY, "org.apache.spark:spark-sql_2.11", "spark")
          ),
          ImmutableList.of(
              getDependencyConfiguration(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-spark",
                  "transport")
          )
      )
  );

  private static DependencyConfiguration getDependencyConfiguration(DependencyConfigurationName configurationName,
      String moduleCoordinate, String platform) {
    return new DependencyConfiguration(configurationName,
        moduleCoordinate + ":" + DEFAULT_VERSIONS.getProperty(platform + "-version"));
  }
}