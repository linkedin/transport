/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
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
  private static final String GET_UDF_IMPLEMENTATIONS_METHOD = "getUdfImplementations";
  private static final ClassName HIVE_UDF_CLASS_NAME = ClassName.bestGuess("com.linkedin.transport.hive.HiveUDF");

  @Override
  public void generateWrappers(WrapperGeneratorContext context) {
    TransportUDFMetadata udfMetadata = context.getTransportUdfMetadata();
    for (String topLevelClass : udfMetadata.getTopLevelClasses()) {
      generateWrapper(topLevelClass, udfMetadata.getUDFImplementations(topLevelClass),
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
      protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
        return ${topLevelClass}.class;
      }
     */
    MethodSpec getTopLevelUdfClassMethod = MethodSpec.methodBuilder(GET_TOP_LEVEL_UDF_CLASS_METHOD)
        .addAnnotation(Override.class)
        .returns(
            ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(TopLevelUDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return $T.class", topLevelClassName)
        .build();

    /*
      Generates ->

      @Override
      protected List<? extends UDF> getUdfImplementations() {
        return ImmutableList.of(
          new ${implementationClasses(0)}(),
          new ${implementationClasses(1)}(),
          .
          .
          .
        );
      }
     */
    MethodSpec getUdfImplementations = MethodSpec.methodBuilder(GET_UDF_IMPLEMENTATIONS_METHOD)
        .addAnnotation(Override.class)
        .returns(ParameterizedTypeName.get(ClassName.get(List.class), WildcardTypeName.subtypeOf(UDF.class)))
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return $T.of($L)", ImmutableList.class, implementationClasses.stream()
            .map(clazz -> "new " + clazz + "()")
            .collect(Collectors.joining(", ")))
        .build();

    /*
      Generates ->

      public class ${wrapperClassName} extends HiveUDF {

        .
        .
        .

      }
     */
    TypeSpec wrapperClass = TypeSpec.classBuilder(wrapperClassName)
        .addModifiers(Modifier.PUBLIC)
        .superclass(HIVE_UDF_CLASS_NAME)
        .addMethod(getTopLevelUdfClassMethod)
        .addMethod(getUdfImplementations)
        .build();

    JavaFile javaFile = JavaFile.builder(wrapperClassName.packageName(), wrapperClass).build();

    try {
      javaFile.writeTo(outputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file", e);
    }
  }
}
