/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.compile.TransportUDFMetadata;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import javax.lang.model.element.Modifier;


public class TrinoWrapperGenerator implements WrapperGenerator {

  private static final String TRINO_PACKAGE_SUFFIX = "trino";
  private static final String GET_UDF_METHOD = "getUDF";
  private static final ClassName TRINO_UDF_CLASS_NAME = ClassName.bestGuess("com.linkedin.transport.trino.TrinoUDF");
  private static final String SERVICE_FILE = "META-INF/services/io.trino.metadata.SqlScalarFunction";

  @Override
  public void generateWrappers(WrapperGeneratorContext context) {
    List<String> services = new LinkedList<>();
    TransportUDFMetadata udfMetadata = context.getTransportUdfMetadata();
    for (String topLevelClass : context.getTransportUdfMetadata().getTopLevelClasses()) {
      for (String implementationClass : udfMetadata.getUDFImplementations(topLevelClass)) {
        generateWrapper(implementationClass, context.getSourcesOutputDir(), services);
      }
    }
    try {
      CodegenUtils.writeServiceFile(context.getResourcesOutputDir().toPath(), Paths.get(SERVICE_FILE), services);
    } catch (IOException e) {
      throw new RuntimeException("Error creating service file", e);
    }
  }

  private void generateWrapper(String implementationClass, File sourcesOutputDir, List<String> services) {
    ClassName implementationClassName = ClassName.bestGuess(implementationClass);
    ClassName wrapperClassName =
        ClassName.get(implementationClassName.packageName() + "." + TRINO_PACKAGE_SUFFIX,
            implementationClassName.simpleName());

    /*
      Generates constructor ->

      public ${wrapperClassName}() {
        super(new ${implementationClassName}());
      }
     */
    MethodSpec constructor = MethodSpec.constructorBuilder()
        .addModifiers(Modifier.PUBLIC)
        .addStatement("super(new $T())", implementationClassName)
        .build();

    /*
      Generates ->

      @Override
      protected UDF getUDF() {
        return new ${implementationClassName}();
      }
     */
    MethodSpec getUDFMethod = MethodSpec.methodBuilder(GET_UDF_METHOD)
        .addAnnotation(Override.class)
        .returns(UDF.class)
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return new $T()", implementationClassName)
        .build();

    /*
      Generates ->

      public class ${wrapperClassName} extends TrinoUDF {

        .
        .
        .

      }
     */
    TypeSpec wrapperClass = TypeSpec.classBuilder(wrapperClassName)
        .addModifiers(Modifier.PUBLIC)
        .superclass(TRINO_UDF_CLASS_NAME)
        .addMethod(constructor)
        .addMethod(getUDFMethod)
        .build();

    services.add(wrapperClassName.toString());
    JavaFile javaFile = JavaFile.builder(wrapperClassName.packageName(), wrapperClass)
        .skipJavaLangImports(true)
        .build();

    try {
      javaFile.writeTo(sourcesOutputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file: ", e);
    }
  }
}
