/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.squareup.javapoet.ClassName;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;


public class SparkWrapperGenerator implements WrapperGenerator {

  private static final String SPARK_PACKAGE_SUFFIX = "spark";
  private static final String SPARK_WRAPPER_TEMPLATE_RESOURCE_PATH = "wrapper-templates/spark";
  private static final String SUBSTITUTOR_KEY_WRAPPER_PACKAGE = "wrapperPackage";
  private static final String SUBSTITUTOR_KEY_WRAPPER_CLASS = "wrapperClass";
  private static final String SUBSTITUTOR_KEY_UDF_TOP_LEVEL_CLASS = "udfTopLevelClass";
  private static final String SUBSTITUTOR_KEY_UDF_IMPLEMENTATIONS = "udfImplementations";

  @Override
  public void generateWrappers(ProjectContext context) {
    Multimap<String, String> udfs = context.getUdfProperties().getUdfs();
    for (String topLevelStdUDFClass : udfs.keySet()) {
      generateWrapper(topLevelStdUDFClass, udfs.get(topLevelStdUDFClass), context.getSourcesOutputDir());
    }
  }

  private void generateWrapper(String topLevelStdUDFClass, Collection<String> implementationClasses, File outputDir) {
    final String wrapperTemplate;
    try {
      wrapperTemplate = IOUtils.toString(
          Thread.currentThread().getContextClassLoader().getResourceAsStream(SPARK_WRAPPER_TEMPLATE_RESOURCE_PATH));
    } catch (IOException e) {
      throw new RuntimeException("Could not read wrapper template for Spark", e);
    }

    ClassName topLevelStdUDFClassName = ClassName.bestGuess(topLevelStdUDFClass);
    ClassName wrapperClass = ClassName.get(
        topLevelStdUDFClassName.packageName() + "." + SPARK_PACKAGE_SUFFIX,
        topLevelStdUDFClassName.simpleName());
    String udfImplementationInstantiations = implementationClasses.stream()
        .map(clazz -> "new " + clazz + "()")
        .collect(Collectors.joining(", "));

    ImmutableMap<String, String> substitutionMap = ImmutableMap.of(
        SUBSTITUTOR_KEY_WRAPPER_PACKAGE, wrapperClass.packageName(),
        SUBSTITUTOR_KEY_WRAPPER_CLASS, wrapperClass.simpleName(),
        SUBSTITUTOR_KEY_UDF_TOP_LEVEL_CLASS, topLevelStdUDFClassName.toString(),
        SUBSTITUTOR_KEY_UDF_IMPLEMENTATIONS, udfImplementationInstantiations
    );

    StringSubstitutor substitutor = new StringSubstitutor(substitutionMap);
    String wrapperCode = substitutor.replace(wrapperTemplate);

    try {
      CodegenUtils.writeCodeFile(outputDir.toPath(), wrapperClass.packageName(), wrapperClass.simpleName(), "scala",
          wrapperCode);
    } catch (IOException e) {
      throw new RuntimeException("Error writing wrapper to file", e);
    }
  }
}
