/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin.packaging;

import com.linkedin.transport.plugin.Platform;
import java.util.List;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;


/**
 * Creates and configures packaging tasks for a given platform which generate publishable/distributable artifacts.
 * Usually this involves identify all dependencies to be included in the artifact, configuring tasks to generate these
 * artifacts and registering the artifact as an outgoing artifact of the project.
 */
public interface Packaging {

  /**
   * Creates and configures packaging tasks for a given platform which generate publishable/distributable artifacts.
   * Usually this involves identify all dependencies to be included in the artifact, configuring tasks to generate these
   * artifacts and registering the artifact as an outgoing artifact of the project.
   *
   * @param project The Gradle {@link Project} for which the task will be created
   * @param platform The {@link Platform} for which the artifacts are being generated
   * @param platformSourceSet The {@link SourceSet} containing the generated sources for the platform
   * @param mainSourceSet The {@link SourceSet} containing user-defined classes for which the platform sources were
   *                      generated
   * @return A {@link List} of {@link TaskProvider} representing the generated packaging tasks
   */
  List<TaskProvider<? extends Task>> configurePackagingTasks(Project project, Platform platform,
      SourceSet platformSourceSet, SourceSet mainSourceSet);
}
