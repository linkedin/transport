/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import java.io.File;
import java.util.Collection;
import java.util.List;
import javax.lang.model.element.Modifier;


public class HiveWrapperGenerator implements WrapperGenerator {

  private static final String HIVE_PACKAGE_SUFFIX = "hive";
  private static final String GET_TOP_LEVEL_UDF_CLASS_METHOD = "getTopLevelUdfClass";
  private static final String GET_STD_UDF_IMPLEMENTATIONS_METHOD = "getStdUdfImplementations";
  private static final ClassName HIVE_STD_UDF_WRAPPER_CLASS_NAME =
      ClassName.bestGuess("com.linkedin.transport.hive.StdUdfWrapper");

  @Override
  public void generateWrappers(ProjectContext context) {
    Multimap<String, String> udfs = context.getUdfProperties().getUdfs();
    for (String topLevelStdUDFClass : udfs.keySet()) {
      generateWrapper(topLevelStdUDFClass, udfs.get(topLevelStdUDFClass), context.getSourcesOutputDir());
    }
  }

  private void generateWrapper(String topLevelStdUDFClass, Collection<String> implementationClasses, File outputDir) {

    ClassName topLevelStdUDFClassName = ClassName.bestGuess(topLevelStdUDFClass);
    ClassName wrapperClassName = ClassName.get(topLevelStdUDFClassName.packageName() + "." + HIVE_PACKAGE_SUFFIX,
        topLevelStdUDFClassName.simpleName());

    MethodSpec getTopLevelUdfClass = MethodSpec.methodBuilder(GET_TOP_LEVEL_UDF_CLASS_METHOD)
        .addAnnotation(Override.class)
        .returns(
            ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(TopLevelStdUDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return $T.class", topLevelStdUDFClassName)
        .build();

    MethodSpec.Builder getStdUdfImplementationsBuilder = MethodSpec.methodBuilder(GET_STD_UDF_IMPLEMENTATIONS_METHOD)
        .addAnnotation(Override.class)
        .returns(ParameterizedTypeName.get(ClassName.get(List.class), WildcardTypeName.subtypeOf(StdUDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("$T builder = $T.builder()", ParameterizedTypeName.get(ImmutableList.Builder.class, StdUDF.class),
            ImmutableList.class);

    for (String implementationClass : implementationClasses) {
      getStdUdfImplementationsBuilder.addStatement("builder.add(new $T())", ClassName.bestGuess(implementationClass));
    }
    MethodSpec getStdUdfImplementations =
        getStdUdfImplementationsBuilder.addStatement("return builder.build()").build();

    TypeSpec wrapperClass = TypeSpec.classBuilder(wrapperClassName)
        .addModifiers(Modifier.PUBLIC)
        .superclass(HIVE_STD_UDF_WRAPPER_CLASS_NAME)
        .addMethod(getTopLevelUdfClass)
        .addMethod(getStdUdfImplementations)
        .build();

    JavaFile javaFile = JavaFile.builder(wrapperClassName.packageName(), wrapperClass).build();

    try {
      javaFile.writeTo(outputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file", e);
    }
  }
}
