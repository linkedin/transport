/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.linkedin.transport.api.udf.StdUDF;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.lang.model.element.Modifier;


public class PrestoWrapperGenerator implements WrapperGenerator {

  private static final String PRESTO_PACKAGE_SUFFIX = "presto";
  private static final String GET_STD_UDF_METHOD = "getStdUDF";
  private static final ClassName PRESTO_STD_UDF_WRAPPER_CLASS_NAME =
      ClassName.bestGuess("com.linkedin.transport.presto.StdUdfWrapper");
  private static final String SERVICE_FILE = "META-INF/services/com.facebook.presto.metadata.SqlScalarFunction";

  @Override
  public void generateWrappers(ProjectContext context) {
    List<String> services = new LinkedList<>();
    for (String implementationClass : context.getUdfProperties().getUdfs().values()) {
      generateWrapper(implementationClass, context.getSourcesOutputDir(), services);
    }
    try {
      CodegenUtils.writeServiceFile(context.getResourcesOutputDir().toPath(), SERVICE_FILE, services);
    } catch (IOException e) {
      throw new RuntimeException("Error creating service file", e);
    }
  }

  private void generateWrapper(String implementationClass, File sourcesOutputDir, List<String> services) {
    ClassName implClass = ClassName.bestGuess(implementationClass);
    ClassName wrapperClassName =
        ClassName.get(implClass.packageName() + "." + PRESTO_PACKAGE_SUFFIX, implClass.simpleName());

    MethodSpec constructor = MethodSpec.constructorBuilder()
        .addModifiers(Modifier.PUBLIC)
        .addStatement("super(new $T())", implClass)
        .build();

    MethodSpec getStdUDF = MethodSpec.methodBuilder(GET_STD_UDF_METHOD)
        .addAnnotation(Override.class)
        .returns(StdUDF.class)
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return new $T()", implClass)
        .build();

    TypeSpec wrapperClass = TypeSpec.classBuilder(wrapperClassName)
        .addModifiers(Modifier.PUBLIC)
        .superclass(PRESTO_STD_UDF_WRAPPER_CLASS_NAME)
        .addMethod(constructor)
        .addMethod(getStdUDF)
        .build();

    services.add(wrapperClassName.toString());
    JavaFile javaFile = JavaFile.builder(wrapperClassName.packageName(), wrapperClass).build();

    try {
      javaFile.writeTo(sourcesOutputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file: ", e);
    }
  }
}
