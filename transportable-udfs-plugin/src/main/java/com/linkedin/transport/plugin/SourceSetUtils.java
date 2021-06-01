/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.plugins.Convention;
import org.gradle.api.tasks.ScalaSourceSet;
import org.gradle.api.tasks.SourceSet;


/**
 * Utility class to help manipulate a {@link SourceSet}
 */
public class SourceSetUtils {

  private SourceSetUtils() {
  }

  /**
   * Returns the {@link SourceDirectorySet} for a given {@link SourceSet} depending on the language of the sources
   */
  static SourceDirectorySet getSourceDirectorySet(SourceSet sourceSet, Language language) {
    switch (language) {
      case JAVA:
        return sourceSet.getJava();
      case SCALA:
        Convention sourceSetConvention = (Convention) InvokerHelper.getProperty(sourceSet, "convention");
        ScalaSourceSet scalaSourceSet = sourceSetConvention.getPlugin(ScalaSourceSet.class);
        return scalaSourceSet.getScala();
      default:
        throw new UnsupportedOperationException("Language " + language + " not supported");
    }
  }

  /**
   * Returns the {@link Configuration} of a given {@link ConfigurationType} for the provided {@link SourceSet}
   */
  public static Configuration getConfigurationForSourceSet(Project project, SourceSet sourceSet,
      ConfigurationType configurationType) {
    return project.getConfigurations().getByName(getConfigurationNameForSourceSet(sourceSet, configurationType));
  }

  private static String getConfigurationNameForSourceSet(SourceSet sourceSet, ConfigurationType configurationType) {
    final String configName;
    switch (configurationType) {
      case ANNOTATION_PROCESSOR:
        configName = sourceSet.getAnnotationProcessorConfigurationName();
        break;
      case IMPLEMENTATION:
        configName = sourceSet.getImplementationConfigurationName();
        break;
      case COMPILE_ONLY:
        configName = sourceSet.getCompileOnlyConfigurationName();
        break;
      case RUNTIME_CLASSPATH:
        configName = sourceSet.getRuntimeClasspathConfigurationName();
        break;
      case RUNTIME_ONLY:
        configName = sourceSet.getRuntimeOnlyConfigurationName();
        break;
      default:
        throw new UnsupportedOperationException("Configuration " + configurationType + " not supported");
    }
    return configName;
  }

  /**
   * Adds the provided dependency to the given {@link Configuration}
   */
  static void addDependencyToConfiguration(Project project, Configuration configuration, Object dependency) {
    addDependencyToConfiguration(configuration, createDependency(project, dependency), null);
  }

  /**
   * Adds the provided dependency {@link Dependency} to the given {@link Configuration},
   * excluding the elements in the excludeProperties
   */
  static void addDependencyToConfiguration(final Configuration configuration, final Dependency dependency,
      final @Nullable Set<Map<String, String>> excludeProperties) {
    configuration.withDependencies(dependencySet -> {
      if (excludeProperties != null) {
        if (dependency instanceof ModuleDependency) {
          excludeProperties.stream().forEach(((ModuleDependency) dependency)::exclude);
        }
      }
      dependencySet.add(dependency);
    });
  }

  /**
   * Create {@link Dependency} by {@link Project}'s {@link DependencyHandler}
   */
  static Dependency createDependency(final Project project, Object dependency) {
    return project.getDependencies().create(dependency);
  }

  /**
   * Adds the provided dependency to the appropriate configurations of the given {@link SourceSet}
   */
  static void addDependencyConfigurationToSourceSet(Project project, SourceSet sourceSet,
      DependencyConfiguration dependencyConfiguration) {
    addDependencyToConfiguration(
        getConfigurationForSourceSet(project, sourceSet, dependencyConfiguration.getConfigurationType()),
        createDependency(project, dependencyConfiguration.getDependencyString()),
        dependencyConfiguration.getExcludedPackageModules()
    );
  }

  /**
   * Adds the provided dependencies to the appropriate configurations of the given {@link SourceSet}
   */
  static void addDependencyConfigurationsToSourceSet(Project project, SourceSet sourceSet,
      Collection<DependencyConfiguration> dependencyConfigurations) {
    dependencyConfigurations.forEach(
        dependencyConfiguration -> addDependencyConfigurationToSourceSet(project, sourceSet, dependencyConfiguration));
  }
}
