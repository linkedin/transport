/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.compile.TransportUDFMetadata;
import com.squareup.javapoet.ClassName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;


public class SparkWrapperGenerator implements WrapperGenerator {

  private static final String SPARK_PACKAGE_SUFFIX = "spark";
  private static final String SPARK_WRAPPER_TEMPLATE_RESOURCE_PATH = "wrapper-templates/spark";
  private static final String SUBSTITUTOR_KEY_WRAPPER_PACKAGE = "wrapperPackage";
  private static final String SUBSTITUTOR_KEY_WRAPPER_CLASS = "wrapperClass";
  private static final String SUBSTITUTOR_KEY_WRAPPER_CLASS_PARAMERTERS = "wrapperClassParameters";
  private static final String SUBSTITUTOR_KEY_UDF_TOP_LEVEL_CLASS = "udfTopLevelClass";
  private static final String SUBSTITUTOR_KEY_UDF_IMPLEMENTATIONS = "udfImplementations";

  @Override
  public void generateWrappers(WrapperGeneratorContext context) {
    TransportUDFMetadata udfMetadata = context.getTransportUdfMetadata();
    for (String topLevelClass : udfMetadata.getTopLevelClasses()) {
      generateWrapper(
          topLevelClass,
          udfMetadata.getStdUDFImplementations(topLevelClass),
          udfMetadata.getClassToNumberOfTypeParameters(),
          context.getSourcesOutputDir());
    }
  }

  private void generateWrapper(String topLevelClass, Collection<String> implementationClasses,
      Map<String, Integer> classToNumberOfTypeParameters, File outputDir) {
    final String wrapperTemplate;
    try (InputStream wrapperTemplateStream = Thread.currentThread()
        .getContextClassLoader()
        .getResourceAsStream(SPARK_WRAPPER_TEMPLATE_RESOURCE_PATH)) {
      wrapperTemplate = IOUtils.toString(wrapperTemplateStream);
    } catch (IOException e) {
      throw new RuntimeException("Could not read wrapper template for Spark", e);
    }

    ClassName topLevelClassName = ClassName.bestGuess(topLevelClass);
    ClassName wrapperClass =
        ClassName.get(topLevelClassName.packageName() + "." + SPARK_PACKAGE_SUFFIX, topLevelClassName.simpleName());
    String udfImplementationInstantiations = implementationClasses.stream()
        .map(clazz -> "new " + clazz + parameters(clazz, classToNumberOfTypeParameters) + "()")
        .collect(Collectors.joining(", "));
    String topLevelClassNameString = topLevelClassName.toString();

    ImmutableMap<String, String> substitutionMap = ImmutableMap.of(
        SUBSTITUTOR_KEY_WRAPPER_PACKAGE, wrapperClass.packageName(),
        SUBSTITUTOR_KEY_WRAPPER_CLASS, wrapperClass.simpleName(),
        SUBSTITUTOR_KEY_UDF_TOP_LEVEL_CLASS, topLevelClassNameString
            + parameters(topLevelClassNameString, classToNumberOfTypeParameters),
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

  private static String parameters(String clazz, Map<String, Integer> classToNumberOfTypeParameters) {
    int numberOfTypeParameters = classToNumberOfTypeParameters.get(clazz);
    String[] objectTypes = new String[numberOfTypeParameters];
    Arrays.fill(objectTypes, "Object");
    return numberOfTypeParameters > 0 ?  "[" + String.join(", ", objectTypes) + "]" : "";
  }
}
