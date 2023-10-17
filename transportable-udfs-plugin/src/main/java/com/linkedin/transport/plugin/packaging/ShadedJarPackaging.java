/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin.packaging;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.github.jengelman.gradle.plugins.shadow.ShadowJavaPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.plugin.Platform;
import com.linkedin.transport.plugin.tasks.ShadeTask;
import java.util.List;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.component.AdhocComponentWithVariants;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;

import static com.linkedin.transport.plugin.ConfigurationType.*;
import static com.linkedin.transport.plugin.SourceSetUtils.*;


/**
 * A {@link Packaging} class which generates Shaded JARs containing all runtime dependencies using the
 * {@link ShadeTask}
 */
public class ShadedJarPackaging implements Packaging {

  private final List<String> _excludedDependencies;
  private final List<String> _classesToNotShade;

  /**
   * Creates a {@link ShadedJarPackaging} object with the given configuration
   *
   * @param excludedDependencies Dependencies to be excluded form the shaded JAR. See {@link ShadeTask#getExcludedDependencies()}
   * @param classesToNotShade Classes to be kept unshaded in the shaded JAR. See {@link ShadeTask#getDoNotShade()}
   */
  public ShadedJarPackaging(List<String> excludedDependencies, List<String> classesToNotShade) {
    _excludedDependencies = excludedDependencies;
    _classesToNotShade = classesToNotShade;
  }

  @Override
  public List<TaskProvider<? extends Task>> configurePackagingTasks(Project project, Platform platform,
      SourceSet platformSourceSet, SourceSet mainSourceSet) {
    TaskProvider<ShadeTask> shadeTask = createShadeTask(project, platform, platformSourceSet, mainSourceSet);
    shadeTask.configure(task -> {
      if (_excludedDependencies != null) {
        task.setExcludedDependencies(ImmutableSet.copyOf(_excludedDependencies));
      }
      if (_classesToNotShade != null) {
        task.setDoNotShade(_classesToNotShade);
      }
    });
    return ImmutableList.of(shadeTask);
  }

  /**
   * Creates a {@link ShadeTask} which generates a shaded JAR containing all runtime dependencies of the platform's
   * {@link SourceSet}
   *
   * TODO: This code is borrowed from the Shade plugin. Call the functionality residing in the Shade plugin once it is
   * available
   */
  private TaskProvider<ShadeTask> createShadeTask(Project project, Platform platform, SourceSet sourceSet,
      SourceSet mainSourceSet) {
    TaskProvider<ShadeTask> shadeTask =
        project.getTasks().register(sourceSet.getTaskName("shade", "Jar"), ShadeTask.class, task -> {
          task.setGroup(ShadowJavaPlugin.SHADOW_GROUP);
          task.setDescription("Create a combined JAR of " + platform.getName() + " output and runtime dependencies");
          task.setClassifier(platform.getName());
          task.getManifest()
              .inheritFrom(project.getTasks().named(mainSourceSet.getJarTaskName(), Jar.class).get().getManifest());
          task.from(sourceSet.getOutput());
          task.setConfigurations(ImmutableList.of(getConfigurationForSourceSet(project, sourceSet, RUNTIME_CLASSPATH)));
          task.exclude("META-INF/INDEX.LIST", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA");
        });

    String configuration = ShadowBasePlugin.CONFIGURATION_NAME;
    project.getArtifacts().add(configuration, shadeTask);
    AdhocComponentWithVariants java = project.getComponents().withType(AdhocComponentWithVariants.class).getByName("java");
    java.addVariantsFromConfiguration(project.getConfigurations().getByName(configuration), v -> v.mapToOptional());
    return shadeTask;
  }
}
