/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin.packaging;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.plugin.Platform;
import java.util.List;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.distribution.DistributionContainer;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.bundling.Tar;
import org.gradle.api.tasks.bundling.Zip;

import static com.linkedin.transport.plugin.ConfigurationType.*;
import static com.linkedin.transport.plugin.SourceSetUtils.*;


/**
 * A {@link Packaging} class which generates distribution TARs and ZIPs containing all runtime dependencies using the
 * {@link org.gradle.api.distribution.plugins.DistributionPlugin}
 */
public class DistributionPackaging implements Packaging {

  @Override
  public List<TaskProvider<? extends Task>> configurePackagingTasks(Project project, Platform platform,
      SourceSet platformSourceSet, SourceSet mainSourceSet) {
    // Create a thin JAR to be included in the distribution
    final TaskProvider<Jar> platformThinJarTask = createThinJarTask(project, platformSourceSet, platform.getName());

    /*
      Include the thin JAR and all the runtime dependencies into the distribution for a given platform

      distributions {
        <platformName> {
          contents {
            from <platformThinJarTask>
            from project.configurations.<platformRuntimeClasspath>
          }
        }
      }
     */
    DistributionContainer distributions = project.getExtensions().getByType(DistributionContainer.class);
    distributions.register(platform.getName(), distribution -> {
      distribution.setBaseName(project.getName());
      distribution.getContents()
          .from(platformThinJarTask)
          .from(getConfigurationForSourceSet(project, platformSourceSet, RUNTIME_CLASSPATH));
    });

    // Explicitly set classifiers for the created distributions or else leads to Maven packaging issues due to multiple
    // artifacts with the same classifier
    project.getTasks().named(platform.getName() + "DistTar", Tar.class, tar -> tar.setClassifier(platform.getName()));
    project.getTasks().named(platform.getName() + "DistZip", Zip.class, zip -> zip.setClassifier(platform.getName()));
    return ImmutableList.of(project.getTasks().named(platform.getName() + "DistTar", Tar.class),
        project.getTasks().named(platform.getName() + "DistZip", Zip.class));
  }

  /**
   * Creates a thin JAR for the platform's {@link SourceSet} to be included in the distribution
   */
  private TaskProvider<Jar> createThinJarTask(Project project, SourceSet sourceSet, String platformName) {
      /*
        task <platformName>DistThinJar(type: Jar, dependsOn: trinoClasses) {
          classifier '<platformName>-dist-thin'
          from sourceSets.<platform>.output
          from sourceSets.<platform>.resources
        }
      */

    return project.getTasks().register(sourceSet.getTaskName(null, "distThinJar"), Jar.class, task -> {
      task.dependsOn(project.getTasks().named(sourceSet.getClassesTaskName()));
      task.setDescription("Assembles a thin jar archive containing the " + platformName
          + " classes to be included in the distribution");
      task.setClassifier(platformName + "-dist-thin");
      task.from(sourceSet.getOutput());
      task.from(sourceSet.getResources());
    });
  }
}
