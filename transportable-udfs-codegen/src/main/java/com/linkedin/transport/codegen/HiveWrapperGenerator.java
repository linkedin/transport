/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.compile.TransportUDFMetadata;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;


public class HiveWrapperGenerator implements WrapperGenerator {

  private static final String HIVE_PACKAGE_SUFFIX = "hive";
  private static final String GET_TOP_LEVEL_UDF_CLASS_METHOD = "getTopLevelUdfClass";
  private static final String GET_STD_UDF_IMPLEMENTATIONS_METHOD = "getStdUdfImplementations";
  private static final ClassName HIVE_STD_UDF_WRAPPER_CLASS_NAME =
      ClassName.bestGuess("com.linkedin.transport.hive.StdUdfWrapper");

  @Override
  public void generateWrappers(WrapperGeneratorContext context) {
    TransportUDFMetadata udfMetadata = context.getTransportUdfMetadata();
    for (String topLevelClass : udfMetadata.getTopLevelClasses()) {
      generateWrapper(topLevelClass, udfMetadata.getStdUDFImplementations(topLevelClass),
          context.getSourcesOutputDir());
    }
  }

  private void generateWrapper(String topLevelClass, Collection<String> implementationClasses, File outputDir) {

    ClassName topLevelClassName = ClassName.bestGuess(topLevelClass);
    ClassName wrapperClassName = ClassName.get(topLevelClassName.packageName() + "." + HIVE_PACKAGE_SUFFIX,
        topLevelClassName.simpleName());

    /*
      Generates ->

      @Override
      protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
        return ${topLevelClass}.class;
      }
     */
    MethodSpec getTopLevelUdfClassMethod = MethodSpec.methodBuilder(GET_TOP_LEVEL_UDF_CLASS_METHOD)
        .addAnnotation(Override.class)
        .returns(
            ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(TopLevelStdUDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return $T.class", topLevelClassName)
        .build();

    /*
      Generates ->

      @Override
      protected List<? extends StdUDF> getStdUdfImplementations() {
        return ImmutableList.of(
          new ${implementationClasses(0)}(),
          new ${implementationClasses(1)}(),
          .
          .
          .
        );
      }
     */
    MethodSpec getStdUdfImplementationsMethod = MethodSpec.methodBuilder(GET_STD_UDF_IMPLEMENTATIONS_METHOD)
        .addAnnotation(Override.class)
        .returns(ParameterizedTypeName.get(ClassName.get(List.class), WildcardTypeName.subtypeOf(StdUDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return $T.of($L)", ImmutableList.class, implementationClasses.stream()
            .map(clazz -> "new " + clazz + "()")
            .collect(Collectors.joining(", ")))
        .build();

    /*
      Generates ->

      public class ${wrapperClassName} extends StdUdfWrapper {

        .
        .
        .

      }
     */
    TypeSpec wrapperClass = TypeSpec.classBuilder(wrapperClassName)
        .addModifiers(Modifier.PUBLIC)
        .superclass(HIVE_STD_UDF_WRAPPER_CLASS_NAME)
        .addMethod(getTopLevelUdfClassMethod)
        .addMethod(getStdUdfImplementationsMethod)
        .build();

    JavaFile javaFile = JavaFile.builder(wrapperClassName.packageName(), wrapperClass).build();

    try {
      javaFile.writeTo(outputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file", e);
    }
  }
}
