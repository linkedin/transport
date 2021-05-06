/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.google.common.collect.ImmutableList;
import com.linkedin.transport.plugin.tasks.GenerateWrappersTask;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.distribution.DistributionContainer;
import org.gradle.api.distribution.plugins.DistributionPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.plugins.scala.ScalaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.language.base.plugins.LifecycleBasePlugin;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension;

import static com.linkedin.transport.plugin.ConfigurationType.*;
import static com.linkedin.transport.plugin.Language.*;
import static com.linkedin.transport.plugin.SourceSetUtils.*;


/**
 * A {@link Plugin} to be applied to a Transport UDF module which:
 * <ol>
 *   <li>Configures default dependencies for the main and test source sets</li>
 *   <li>Applies the Transport UDF annotation processor</li>
 *   <li>Creates a SourceSet for UDF wrapper generation</li>
 *   <li>Configures default dependencies for the platform wrappers</li>
 *   <li>Configures wrapper code generation tasks</li>
 *   <li>Configures tasks to package wrappers with appropriate shading rules</li>
 *   <li>Configures tasks to run UDF tests for the platform using the Unified Testing Framework</li>
 * </ol>
 */
public class TransportPlugin implements Plugin<Project> {

  public void apply(Project project) {

    TransportPluginConfig extension = project.getExtensions().create("transport", TransportPluginConfig.class, project);

    project.getPlugins().withType(JavaPlugin.class, (javaPlugin) -> {
      project.getPlugins().apply(ScalaPlugin.class);
      project.getPlugins().apply(DistributionPlugin.class);
      project.getConfigurations().create(ShadowBasePlugin.getCONFIGURATION_NAME());

      JavaPluginConvention javaConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
      SourceSet mainSourceSet = javaConvention.getSourceSets().getByName(extension.mainSourceSetName);
      SourceSet testSourceSet = javaConvention.getSourceSets().getByName(extension.testSourceSetName);

      configureBaseSourceSets(project, mainSourceSet, testSourceSet);
      Defaults.DEFAULT_PLATFORMS.forEach(
          platform -> configurePlatform(project, platform, mainSourceSet, testSourceSet, extension.outputDirFile));
    });
    // Disable Jacoco for platform test tasks as it is known to cause issues with Trino and Hive tests
    project.getPlugins().withType(JacocoPlugin.class, (jacocoPlugin) -> {
        Defaults.DEFAULT_PLATFORMS.forEach(platform -> {
          project.getTasksByName(testTaskName(platform), true).forEach(task -> {
            JacocoTaskExtension jacocoExtension = task.getExtensions().findByType(JacocoTaskExtension.class);
            if (jacocoExtension != null) {
              jacocoExtension.setEnabled(false);
            }
          });
        });
    });
  }

  /**
   * Configures default dependencies for the main and test source sets
   */
  private void configureBaseSourceSets(Project project, SourceSet mainSourceSet, SourceSet testSourceSet) {
    addDependencyConfigurationsToSourceSet(project, mainSourceSet, Defaults.MAIN_SOURCE_SET_DEPENDENCY_CONFIGURATIONS);
    addDependencyConfigurationsToSourceSet(project, testSourceSet, Defaults.TEST_SOURCE_SET_DEPENDENCY_CONFIGURATIONS);

    // Configure the default distribution task configured by the distribution plugin, else build fails
    DistributionContainer distributions = project.getExtensions().getByType(DistributionContainer.class);
    distributions.getByName(DistributionPlugin.MAIN_DISTRIBUTION_NAME)
        .getContents()
        .from(project.getTasks().named(mainSourceSet.getJarTaskName()));
  }

  /**
   * Configures SourceSets, dependencies and tasks related to each Transport UDF platform
   */
  private void configurePlatform(Project project, Platform platform, SourceSet mainSourceSet, SourceSet testSourceSet,
      File baseOutputDir) {
    SourceSet sourceSet = configureSourceSet(project, platform, mainSourceSet, baseOutputDir);
    configureGenerateWrappersTask(project, platform, mainSourceSet, sourceSet);
    List<TaskProvider<? extends Task>> packagingTasks =
        configurePackagingTasks(project, platform, sourceSet, mainSourceSet);
    // Add Transport tasks to build task dependencies
    project.getTasks().named(LifecycleBasePlugin.BUILD_TASK_NAME).configure(task -> task.dependsOn(packagingTasks));

    TaskProvider<Test> testTask = configureTestTask(project, platform, mainSourceSet, testSourceSet);
    project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(task -> task.dependsOn(testTask));
  }

  /**
   * Creates and configures a {@link SourceSet} for a given platform, sets up the source directories and
   * configurations and configures the default dependencies required for compilation and runtime of the wrapper
   * SourceSet
   */
  private SourceSet configureSourceSet(Project project, Platform platform, SourceSet mainSourceSet, File baseOutputDir) {
    JavaPluginConvention javaConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
    Path platformBaseDir = Paths.get(baseOutputDir.toString(), "generatedWrappers", platform.getName());
    Path wrapperSourceOutputDir = platformBaseDir.resolve("sources");
    Path wrapperResourceOutputDir = platformBaseDir.resolve("resources");

    return javaConvention.getSourceSets().create(platform.getName(), sourceSet -> {
      /*
        Creates a SourceSet and set the source directories for a given platform. E.g. For the Trino platform,

        trino {
          java.srcDirs = ["${buildDir}/generatedWrappers/sources"]
          resources.srcDirs = ["${buildDir}/generatedWrappers/resources"]
        }
       */
      getSourceDirectorySet(sourceSet, platform.getLanguage()).setSrcDirs(ImmutableList.of(wrapperSourceOutputDir));
      sourceSet.getResources().setSrcDirs(ImmutableList.of(wrapperResourceOutputDir));

      /*
        Sets up the configuration for the platform's wrapper SourceSet. E.g. For the Trino platform,

        configurations {
          trinoImplementation.extendsFrom mainImplementation
          trinoRuntimeOnly.extendsFrom mainRuntimeOnly
        }
       */
      getConfigurationForSourceSet(project, sourceSet, IMPLEMENTATION).extendsFrom(
          getConfigurationForSourceSet(project, mainSourceSet, IMPLEMENTATION));
      getConfigurationForSourceSet(project, sourceSet, RUNTIME_ONLY).extendsFrom(
          getConfigurationForSourceSet(project, mainSourceSet, RUNTIME_ONLY));

      /*
        Adds the default dependencies for the platform. E.g For the Trino platform,

        dependencies {
          trinoImplementation project.files(project.tasks.jar)
          trinoImplementation 'com.linkedin.transport:transportable-udfs-trino:$version'
          trinoCompileOnly 'io.trino:trino-main:$version'
        }
       */
      addDependencyToConfiguration(project, getConfigurationForSourceSet(project, sourceSet, IMPLEMENTATION),
          project.files(project.getTasks().named(mainSourceSet.getJarTaskName())));
      addDependencyConfigurationsToSourceSet(project, sourceSet, platform.getDefaultWrapperDependencyConfigurations());
    });
  }

  /**
   * Creates and configures a task to generate UDF wrappers for a given platform
   */
  private TaskProvider<GenerateWrappersTask> configureGenerateWrappersTask(Project project, Platform platform,
      SourceSet inputSourceSet, SourceSet outputSourceSet) {

    /*
      Creates a generateWrapper task for a given platform. E.g For the Trino platform,

      task generateTrinoWrappers {
        generatorClass = 'com.linkedin.transport.codegen.TrinoWrapperGenerator'
        inputClassesDirs = sourceSets.main.output.classesDirs
        sourcesOutputDir = sourceSets.trino.java.srcDirs[0]
        resourcesOutputDir = sourceSets.trino.resources.srcDirs[0]
        dependsOn classes
      }

      trinoClasses.dependsOn(generateTrinoWrappers)
     */
    String taskName = outputSourceSet.getTaskName("generate", "Wrappers");
    File sourcesOutputDir =
        getSourceDirectorySet(outputSourceSet, platform.getLanguage()).getSrcDirs().iterator().next();
    File resourcesOutputDir = outputSourceSet.getResources().getSrcDirs().iterator().next();

    TaskProvider<GenerateWrappersTask> generateWrappersTask =
        project.getTasks().register(taskName, GenerateWrappersTask.class, task -> {
          task.setDescription("Generates Transport UDF wrappers for " + platform.getName());
          task.getGeneratorClass().set(platform.getWrapperGeneratorClass().getName());
          task.getInputClassesDirs().set(inputSourceSet.getOutput().getClassesDirs());
          task.getSourcesOutputDir().set(sourcesOutputDir);
          task.getResourcesOutputDir().set(resourcesOutputDir);
          task.dependsOn(project.getTasks().named(inputSourceSet.getClassesTaskName()));
        });

    // Configure Java compile tasks to run with platform specific jdk
    // TODO: set platform specific jdks/toolchain for scala tasks when support is available
    if (platform.getLanguage() == JAVA) {
      project.getTasks()
          .named(outputSourceSet.getCompileTaskName(platform.getLanguage().toString()), JavaCompile.class, task -> {
            JavaToolchainService javaToolchains = project.getExtensions().getByType(JavaToolchainService.class);
            task.getJavaCompiler().set(javaToolchains.compilerFor(toolChainSpec -> {
              toolChainSpec.getLanguageVersion().set(platform.getJavaLanguageVersion());
            }));
          });
    }

    project.getTasks()
        .named(outputSourceSet.getCompileTaskName(platform.getLanguage().toString()))
        .configure(task -> task.dependsOn(generateWrappersTask));

    return generateWrappersTask;
  }

  /**
   * Creates and configures packaging tasks for a given platform which generate publishable/distributable artifacts
   */
  private List<TaskProvider<? extends Task>> configurePackagingTasks(Project project, Platform platform,
      SourceSet sourceSet, SourceSet mainSourceSet) {
    return platform.getPackaging().stream()
        .flatMap(p -> p.configurePackagingTasks(project, platform, sourceSet, mainSourceSet).stream())
        .collect(Collectors.toList());
  }

  /**
   * Creates and configures a task to run tests written using the Unified Testing Framework against a given platform
   */
  private TaskProvider<Test> configureTestTask(Project project, Platform platform, SourceSet mainSourceSet,
      SourceSet testSourceSet) {

    /*
      Configures the classpath configuration to run platform-specific tests. E.g. For the Trino platform,

      configurations {
        trinoTestClasspath {
          extendsFrom testImplementation
        }
      }

      dependencies {
        trinoTestClasspath sourceSets.main.output, sourceSets.test.output
        trinoTestClasspath 'com.linkedin.transport:transportable-udfs-test-trino'
      }
     */
    Configuration testClasspath = project.getConfigurations()
        .create(platform.getName() + "TestClasspath",
            config -> config.extendsFrom(getConfigurationForSourceSet(project, testSourceSet, IMPLEMENTATION)));
    addDependencyToConfiguration(project, testClasspath, mainSourceSet.getOutput());
    addDependencyToConfiguration(project, testClasspath, testSourceSet.getOutput());
    platform.getDefaultTestDependencyConfigurations()
        .forEach(dependencyConfiguration -> addDependencyToConfiguration(project, testClasspath,
            dependencyConfiguration.getDependencyString()));

    /*
      Creates the test task for a given platform. E.g. For the Trino platform,

      task trinoTest(type: Test, dependsOn: test) {
        group 'Verification'
        description 'Runs the Trino tests.'
        testClassesDirs = sourceSets.test.output.classesDirs
        classpath = configurations.trinoTestClasspath
        useTestNG()
      }
    */

    return project.getTasks().register(testTaskName(platform), Test.class, task -> {
      task.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
      task.setDescription("Runs Transport UDF tests on " + platform.getName());
      task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
      task.setClasspath(testClasspath);
      task.useTestNG();
      task.mustRunAfter(project.getTasks().named("test"));

      // configure test task to run with platform specific jdk
      JavaToolchainService javaToolchains = project.getExtensions().getByType(JavaToolchainService.class);
      task.getJavaLauncher().set(javaToolchains.launcherFor(toolChainSpec -> {
        toolChainSpec.getLanguageVersion().set(platform.getJavaLanguageVersion());
      }));
    });
  }

  private String testTaskName(Platform platform) {
    return platform.getName() + "Test";
  }
}
