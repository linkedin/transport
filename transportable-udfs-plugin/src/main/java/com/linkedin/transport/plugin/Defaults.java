/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.codegen.HiveWrapperGenerator;
import com.linkedin.transport.codegen.SparkWrapperGenerator;
import com.linkedin.transport.codegen.TrinoWrapperGenerator;
import com.linkedin.transport.plugin.packaging.DistributionPackaging;
import com.linkedin.transport.plugin.packaging.ShadedJarPackaging;
import com.linkedin.transport.plugin.packaging.ThinJarPackaging;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.gradle.jvm.toolchain.JavaLanguageVersion;

import static com.linkedin.transport.plugin.ConfigurationType.*;


/**
 * Stores default configurations for the Transport UDF plugin
 */
class Defaults {

  private Defaults() {
  }

  // The versions of the Transport and supported platforms to apply corresponding versions of the platform dependencies
  private static final Properties DEFAULT_VERSIONS = loadDefaultVersions();

  private static Properties loadDefaultVersions() {
    try (InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("version-info.properties")) {
      Properties defaultVersions = new Properties();
      defaultVersions.load(is);
      return defaultVersions;
    } catch (IOException e) {
      throw new RuntimeException("Error loading version-info.properties", e);
    }
  }

  private static final String getVersion(final String platform) {
    return DEFAULT_VERSIONS.getProperty(platform + "-version");
  }
  private static final String HIVE = "hive";
  private static final String SPARK = "spark";
  private static final String TRINO = "trino";

  private static final String TRANSPORT_VERSION = getVersion("transport");
  private static final String SCALA_VERSION = getVersion("scala");
  private static final String HIVE_VERSION = getVersion(HIVE);
  private static final String SPARK_VERSION = getVersion(SPARK);
  private static final String TRINO_VERSION = getVersion(TRINO);

  static final List<DependencyConfiguration> MAIN_SOURCE_SET_DEPENDENCY_CONFIGURATIONS = ImmutableList.of(
      DependencyConfiguration.builder(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-api", TRANSPORT_VERSION).build(),
      DependencyConfiguration.builder(ANNOTATION_PROCESSOR, "com.linkedin.transport:transportable-udfs-annotation-processor", TRANSPORT_VERSION).build(),
      // the idea plugin needs a scala-library on the classpath when the scala plugin is applied even when there are no
      // scala sources
      DependencyConfiguration.builder(COMPILE_ONLY, "org.scala-lang:scala-library", SCALA_VERSION).build()
  );

  static final List<DependencyConfiguration> TEST_SOURCE_SET_DEPENDENCY_CONFIGURATIONS = ImmutableList.of(
      DependencyConfiguration.builder(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-test-api", TRANSPORT_VERSION).build(),
      DependencyConfiguration.builder(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-generic", TRANSPORT_VERSION).build()
  );

  static final List<Platform> DEFAULT_PLATFORMS = ImmutableList.of(
      new Platform(TRINO,
          Language.JAVA,
          TrinoWrapperGenerator.class,
          JavaLanguageVersion.of(11),
          ImmutableList.of(
              DependencyConfiguration.builder(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-trino", TRANSPORT_VERSION).build(),
              DependencyConfiguration.builder(COMPILE_ONLY, "io.trino:trino-main", TRINO_VERSION).build()
          ),
          ImmutableList.of(
              DependencyConfiguration.builder(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-trino", TRANSPORT_VERSION).build(),
              // trino-main:tests is a transitive dependency of transportable-udfs-test-trino, but some POM -> IVY
              // converters drop dependencies with classifiers, so we apply this dependency explicitly
              DependencyConfiguration.builder(RUNTIME_ONLY, "io.trino:trino-main", TRINO_VERSION).classifier("tests").build()
          ),
          ImmutableList.of(new ThinJarPackaging(), new DistributionPackaging())),
      new Platform(HIVE,
          Language.JAVA,
          HiveWrapperGenerator.class,
          JavaLanguageVersion.of(8),
          ImmutableList.of(
              DependencyConfiguration.builder(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-hive", TRANSPORT_VERSION).build(),
              DependencyConfiguration.builder(COMPILE_ONLY, "org.apache.hive:hive-exec", HIVE_VERSION).exclude("org.apache.calcite").build()
          ),
          ImmutableList.of(
              DependencyConfiguration.builder(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-hive", TRANSPORT_VERSION).build()
          ),
          ImmutableList.of(new ShadedJarPackaging(ImmutableList.of("org.apache.hadoop", "org.apache.hive"), null))),
      new Platform(SPARK,
          Language.SCALA,
          SparkWrapperGenerator.class,
          JavaLanguageVersion.of(8),
          ImmutableList.of(
              DependencyConfiguration.builder(IMPLEMENTATION, "com.linkedin.transport:transportable-udfs-spark", TRANSPORT_VERSION).build(),
              DependencyConfiguration.builder(COMPILE_ONLY, "org.apache.spark:spark-sql_2.12", SPARK_VERSION).build()
          ),
          ImmutableList.of(
              DependencyConfiguration.builder(RUNTIME_ONLY, "com.linkedin.transport:transportable-udfs-test-spark", TRANSPORT_VERSION).build()
          ),
          ImmutableList.of(new ShadedJarPackaging(
              ImmutableList.of("org.apache.hadoop", "org.apache.spark"),
              ImmutableList.of("com.linkedin.transport.spark.**")))
      )
  );
}
