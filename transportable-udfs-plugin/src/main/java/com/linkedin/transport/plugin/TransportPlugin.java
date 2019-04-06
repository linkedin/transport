/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.plugin.tasks.GenerateWrappersTask;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.plugins.scala.ScalaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;


/**
 * A {@link Plugin} to be applied to a Transport UDF module which:
 * <ol>
 *   <li>Configures default dependencies for the main and test source sets</li>
 *   <li>Applies the Transport UDF annotation processor</li>
 *   <li>Creates a SourceSet for UDF wrapper generation</li>
 *   <li>Configures default dependencies for the platform wrappers</li>
 *   <li>Configures wrapper code generation tasks</li>
 *   <li>TODO: Configures tasks to package wrappers with appropriate shading rules</li>
 *   <li>Configures tasks to run UDF tests for the platform using the Unified Testing Framework</li>
 * </ol>
 */
public class TransportPlugin implements Plugin<Project> {

  public void apply(Project project) {
    project.getPlugins().withType(JavaPlugin.class, (javaPlugin) -> {
      project.getPlugins().apply(ScalaPlugin.class);

      JavaPluginConvention javaConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
      SourceSet mainSourceSet = javaConvention.getSourceSets().getByName("main");
      SourceSet testSourceSet = javaConvention.getSourceSets().getByName("test");

      configureBaseSourceSets(project, mainSourceSet, testSourceSet);
      Defaults.DEFAULT_PLATFORMS.forEach(config -> configurePlatform(project, config, mainSourceSet, testSourceSet));
    });
  }

  /**
   * Configures default dependencies for the main and test source sets
   */
  private void configureBaseSourceSets(Project project, SourceSet mainSourceSet, SourceSet testSourceSet) {
    SourceSetUtils.addDependenciesToSourceSet(project, mainSourceSet, Defaults.MAIN_SOURCE_SET_DEPENDENCIES);
    SourceSetUtils.addDependenciesToSourceSet(project, testSourceSet, Defaults.TEST_SOURCE_SET_DEPENDENCIES);
  }

  /**
   * Configures SourceSets, dependencies and tasks related to each Transport UDF platform
   */
  private void configurePlatform(Project project, Platform platform,
      SourceSet mainSourceSet, SourceSet testSourceSet) {
    SourceSet sourceSet = configureSourceSet(project, platform, mainSourceSet);
    TaskProvider<GenerateWrappersTask> generateWrappersTask =
        configureGenerateWrappersTask(project, platform, mainSourceSet, sourceSet);
    // TODO: shade and package into Jar
    // Add Transport tasks to build task dependencies
    project.getTasks().named(LifecycleBasePlugin.BUILD_TASK_NAME).configure(task -> {
      // TODO: Replace this task with the shaded jar tasks once we create them
      task.dependsOn(sourceSet.getClassesTaskName());
    });

    TaskProvider<Test> testTask = configureTestTask(project, platform, mainSourceSet, testSourceSet);
    project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(task -> task.dependsOn(testTask));
  }

  /**
   * Creates and configures a {@link SourceSet} for a given platform, sets up the source directories and
   * configurations and configures the default dependencies required for compilation and runtime of the wrapper
   * SourceSet
   */
  private SourceSet configureSourceSet(Project project, Platform platform,
      SourceSet mainSourceSet) {
    JavaPluginConvention javaConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
    Path platformBaseDir =
        project.getBuildDir().toPath().resolve(Paths.get("generatedWrappers", platform.getName()));
    Path wrapperSourceOutputDir = platformBaseDir.resolve("sources");
    Path wrapperResourceOutputDir = platformBaseDir.resolve("resources");

    return javaConvention.getSourceSets().create(platform.getName(), sourceSet -> {
      /*
        Creates a SourceSet and set the source directories. E.g.

        presto {
          java.srcDirs = ["${buildDir}/generatedWrappers/sources"]
          resources.srcDirs = ["${buildDir}/generatedWrappers/resources"]
        }
       */
      SourceSetUtils.getSourceDirectorySet(sourceSet, platform.getLanguage()).setSrcDirs(
          ImmutableList.of(wrapperSourceOutputDir));
      sourceSet.getResources().setSrcDirs(ImmutableList.of(wrapperResourceOutputDir));

      /*
        Sets up the configuration for the wrapper SourceSet. E.g.

        configurations {
          prestoImplementation.extendsFrom mainImplementation
          prestoRuntimeOnly.extendsFrom mainRuntimeOnly
        }
       */
      project.getConfigurations().getByName(sourceSet.getImplementationConfigurationName())
          .extendsFrom(project.getConfigurations().getByName(mainSourceSet.getImplementationConfigurationName()));
      project.getConfigurations().getByName(sourceSet.getRuntimeOnlyConfigurationName()).extendsFrom(
          project.getConfigurations().getByName(mainSourceSet.getRuntimeOnlyConfigurationName()));

      /*
        Adds default dependency config for Presto

        dependencies {
          prestoImplementation sourceSets.main.output
          prestoImplementation 'com.linkedin.transport:transportable-udfs-presto:$version'
          prestoCompileOnly 'com.facebook.presto:presto-main:$version'
        }
       */
      SourceSetUtils.addDependencyToConfiguration(project,
          project.getConfigurations().getByName(sourceSet.getImplementationConfigurationName()),
          mainSourceSet.getOutput());
      SourceSetUtils.addDependenciesToSourceSet(project, sourceSet,
          platform.getDefaultWrapperDependencies());
    });
  }

  /**
   * Creates and configures a task to generate UDF wrappers for a given platform
   */
  private TaskProvider<GenerateWrappersTask> configureGenerateWrappersTask(Project project,
      Platform platform, SourceSet inputSourceSet, SourceSet outputSourceSet) {

    /*
      Example generateWrapper task for Presto

      task generatePrestoWrappers {
        generatorClass = 'com.linkedin.transport.codegen.PrestoWrapperGenerator'
        inputClassesDirs = sourceSets.main.output.classesDirs
        sourcesOutputDir = sourceSets.presto.java.srcDirs[0]
        resourcesOutputDir = sourceSets.presto.resources.srcDirs[0]
        dependsOn classes
      }

      prestoClasses.dependsOn(generatePrestoWrappers)
     */
    String taskName = outputSourceSet.getTaskName("generate", "Wrappers");
    File sourcesOutputDir =
        SourceSetUtils.getSourceDirectorySet(outputSourceSet, platform.getLanguage())
            .getSrcDirs().iterator().next();
    File resourcesOutputDir = outputSourceSet.getResources().getSrcDirs().iterator().next();

    TaskProvider<GenerateWrappersTask>
        generateWrappersTask = project.getTasks().register(taskName, GenerateWrappersTask.class, task -> {
      task.setDescription("Generates Transport UDF wrappers for " + platform.getName());
      task.getGeneratorClass().set(platform.getWrapperGeneratorClass().getName());
      task.getInputClassesDirs().set(inputSourceSet.getOutput().getClassesDirs());
      task.getSourcesOutputDir().set(sourcesOutputDir);
      task.getResourcesOutputDir().set(resourcesOutputDir);
      task.dependsOn(project.getTasks().named(inputSourceSet.getClassesTaskName()));
    });

    project.getTasks().named(outputSourceSet.getCompileTaskName(platform.getLanguage().toString()))
        .configure(task -> task.dependsOn(generateWrappersTask));

    return generateWrappersTask;
  }

  /**
   * Creates and configures a task to run tests written using the Unified Testing Framework against a given platform
   */
  private TaskProvider<Test> configureTestTask(Project project, Platform platform,
      SourceSet mainSourceSet, SourceSet testSourceSet) {

    /*
      Configures the classpath configuration to run platform-specific tests. E.g. for Presto,

      configurations {
        prestoTestClasspath {
          extendsFrom testImplementation
        }
      }

      dependencies {
        prestoTestClasspath sourceSets.main.output, sourceSets.test.output
        prestoTestClasspath 'com.linkedin.transport:transportable-udfs-test-presto'
      }
     */
    Configuration testClasspath =
        project.getConfigurations().create(platform.getName() + "TestClasspath", config ->
            config.extendsFrom(
                project.getConfigurations().getByName(testSourceSet.getImplementationConfigurationName()))
        );
    SourceSetUtils.addDependencyToConfiguration(project, testClasspath, mainSourceSet.getOutput());
    SourceSetUtils.addDependencyToConfiguration(project, testClasspath, testSourceSet.getOutput());
    SourceSetUtils.addDependenciesToConfiguration(project, testClasspath,
        platform.getDefaultTestDependencies());

    /*
      Creates the test task for the platform. E.g. For Presto,

      task prestoTest(type: Test, dependsOn: test) {
        group 'Verification'
        description 'Runs the Presto tests.'
        testClassesDirs = sourceSets.test.output.classesDirs
        classpath = configurations.prestoTestClasspath
        useTestNG()
      }
    */

    return project.getTasks().register(platform.getName() + "Test", Test.class, task -> {
      task.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
      task.setDescription("Runs Transport UDF tests on " + platform.getName());
      task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
      task.setClasspath(testClasspath);
      task.useTestNG();
      task.mustRunAfter(project.getTasks().named("test"));
    });
  }
}
