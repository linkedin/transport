/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin.tasks;

import com.github.jengelman.gradle.plugins.shadow.relocation.SimpleRelocator;
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;


/**
 * A {@link org.gradle.api.Task} which extends {@link ShadowJar} functionality by providing helper methods to exclude
 * class from being shaded and exclude dependencies altogether from the output Jar
 */
public class ShadeTask extends ShadowJar {
  private static final Logger LOG = Logging.getLogger(ShadeTask.class);

  /**
   * The prefix to be applied to all shaded classes in the JAR
   */
  private String _shadePrefix;

  /**
   * The patterns to be excluded from shading. The classes satisfying the pattern will be present in the shaded jar,
   * although, unshaded. There are multiple ways a pattern can be defined
   *
   * <ul>
   *   <li><code>**\/AClass.*</code></li>
   *   <li><code>com.linkedin.udf.AClass</code></li>
   *   <li><code>com/linkedin/udf/AClass</code></li>
   * </ul>
   */
  private List<String> _doNotShade;

  /**
   * The dependencies to be excluded from the shaded JAR. All transitive dependencies will also be excluded.
   * There are multiple ways an exclude can be defined
   *
   * <ul>
   *   <li>By group id:- <code>log4j</code></li>
   *   <li>By group and module id:- <code>com.linkedin.hive:hive-exec</code></li>
   * </ul>
   */
  private Set<String> _excludedDependencies;

  public ShadeTask() {
    super();
    // By default we use the prefix [product-group]_[product-name]_[product-version]
    _shadePrefix = defaultShadePrefix();
    _doNotShade = new LinkedList<>();
    _excludedDependencies = new HashSet<>();
  }

  @Input
  public String getShadePrefix() {
    return _shadePrefix;
  }

  @Input
  public List<String> getDoNotShade() {
    return _doNotShade;
  }

  @Input
  public Set<String> getExcludedDependencies() {
    return _excludedDependencies;
  }

  public void setShadePrefix(String shadePrefix) {
    _shadePrefix = sanitize(shadePrefix);
  }

  public void setDoNotShade(List<String> doNotShade) {
    _doNotShade = doNotShade;
  }

  public void setExcludedDependencies(Set<String> excludedDependencies) {
    _excludedDependencies = excludedDependencies;
  }

  /**
   * {@inheritDoc}
   */
  @TaskAction
  @Override
  protected void copy() {
    Set<String> classPathsToShade = new HashSet<>();

    List<Configuration> configurations =
        getConfigurations().stream().map(files -> this.setupConfiguration((Configuration) files)).collect(Collectors.toList());

    // Collect all classes which need to be shaded
    configurations.forEach(configuration -> classPathsToShade.addAll(classesInConfiguration(configuration)));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Classes to shade: " + classPathsToShade);
    }
    // The class names are the same as the paths, but with "." instead of "/"
    Set<String> classNamesToShade =
        classPathsToShade.stream().map(path -> path.replace('/', '.')).collect(Collectors.toSet());
    // Pass the new updated conf to _shadowJar
    setConfigurations(ImmutableList.copyOf(configurations));
    // Exclude source files
    exclude("**/*.java");
    // Do not shade classes we have excluded
    _doNotShade = ImmutableList.<String>builder().addAll(_doNotShade).addAll(getExcludes()).build();
    // Relocate base on the above restrictions
    super.relocate(new SimpleRelocator(null, _shadePrefix + '/', ImmutableList.of("**"), _doNotShade) {
      @Override
      public boolean canRelocatePath(String path) {
        // Only relocate those classes present in source project and dependency jars
        return classPathsToShade.contains(path) && super.canRelocatePath(path);
      }

      @Override

      public boolean canRelocateClass(String className) {
        // Only relocate those classes present in source project and dependency jars
        return classNamesToShade.contains(className) && super.canRelocateClass(className);
      }
    });
    super.copy();
  }

  /**
   * Create a copy of the input configuration and returns the copied configuration without the excluded dependencies
   */
  private Configuration setupConfiguration(Configuration configuration) {
    Configuration conf = configuration.copyRecursive();
    _excludedDependencies.forEach(artifact -> {
      int idx = artifact.indexOf(':');
      if (idx == -1) {
        LOG.info("Will exclude all artifacts having the group: " + artifact + " from the shaded jar");
        conf.exclude(ImmutableMap.of("group", artifact));
      } else {
        LOG.info("Will exclude all artifacts having the group and module: " + artifact + " from the shaded jar");
        conf.exclude(ImmutableMap.of("group", artifact.substring(0, idx), "module", artifact.substring(idx + 1)));
      }
    });
    return conf;
  }

  /**
   * Computes the default shade prefix which is [project.group]_[project.name]_[project.version] with hyphens and dots
   * replaced by underscore
   */
  private String defaultShadePrefix() {
    return String.join("_", sanitize(getProject().getGroup().toString()), sanitize(getProject().getName()),
        sanitize(getProject().getVersion().toString()));
  }

  private static String sanitize(String s) {
    return s.replaceAll("\\-", "_").replaceAll("\\.", "_");
  }

  /**
   * Returns all the classes present in all the dependencies of the input configuration
   */
  private Set<String> classesInConfiguration(Configuration conf) {
    // Do not modify original as it will be used in building fat jar
    Configuration configuration = conf.copyRecursive();

    FileCollection fileCollection = super.getDependencyFilter().resolve(configuration);
    FileCollection jars = fileCollection.getAsFileTree().filter(file -> file.getName().endsWith(".jar"));

    LOG.info("Will shade the following jars for project " + getProject().getName());
    new TreeSet<>(jars.getFiles()).forEach(jar -> LOG.info(jar.toString()));
    Set<String> classes = new HashSet<>();
    jars.forEach(jar -> classes.addAll(classesInJar(jar)));
    return classes;
  }

  /**
   * Returns all classes in the input jar
   */
  private static Set<String> classesInJar(File jar) {
    Set<String> classes = new HashSet<>();
    try (JarFile jf = new JarFile(jar)) {
      jf.stream().forEach(file -> {
        String name = file.getName();
        if (name.endsWith(".class")) {
          classes.add(name.substring(0, name.lastIndexOf('.')));
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Error reading from Jar file: " + jar, e);
    }
    return classes;
  }
}