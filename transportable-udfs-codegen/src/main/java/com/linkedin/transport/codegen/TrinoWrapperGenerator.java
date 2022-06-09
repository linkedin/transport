/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

import com.linkedin.transport.api.udf.StdUDF;
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
  private static final String GET_STATE_CLASS_NAME_METHOD = "getStateClassName";
  private static final ClassName TRINO_STD_UDF_WRAPPER_CLASS_NAME =
      ClassName.bestGuess("com.linkedin.transport.trino.StdUdfWrapper");
  private static final ClassName TRINO_STD_UDF_WRAPPER_STATE_CLASS_NAME =
          ClassName.bestGuess("com.linkedin.transport.trino.StdUdfWrapper.State");
  private static final String SERVICE_FILE = "META-INF/services/io.trino.metadata.SqlScalarFunction";

  @Override
  public void generateWrappers(WrapperGeneratorContext context) {
    List<String> services = new LinkedList<>();
    TransportUDFMetadata udfMetadata = context.getTransportUdfMetadata();
    for (String topLevelClass : context.getTransportUdfMetadata().getTopLevelClasses()) {
      for (String implementationClass : udfMetadata.getStdUDFImplementations(topLevelClass)) {
        ClassName implementationClassName = ClassName.bestGuess(implementationClass);
        ClassName stateClassName =
                ClassName.get(implementationClassName.packageName() + "." + TRINO_PACKAGE_SUFFIX,
                        implementationClassName.simpleName() + "State");
        ClassName wrapperClassName =
                ClassName.get(implementationClassName.packageName() + "." + TRINO_PACKAGE_SUFFIX,
                        implementationClassName.simpleName());
        generateWrapperClass(wrapperClassName, implementationClassName, context.getSourcesOutputDir(), services);
        generateStateClass(stateClassName, implementationClassName, context.getSourcesOutputDir(), services);
      }
    }
    try {
      CodegenUtils.writeServiceFile(context.getResourcesOutputDir().toPath(), Paths.get(SERVICE_FILE), services);
    } catch (IOException e) {
      throw new RuntimeException("Error creating service file", e);
    }
  }

  private void generateWrapperClass(ClassName wrapperClassName, ClassName implementationClassName, File sourcesOutputDir,
                                    List<String> services) {
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
      protected String getStateClassName() {
        return new ${implementationClassName}();
      }
     */
    MethodSpec getStateClassNameMethod = MethodSpec.methodBuilder(GET_STATE_CLASS_NAME_METHOD)
        .addAnnotation(Override.class)
        .returns(String.class)
        .addModifiers(Modifier.PROTECTED)
        .addStatement("return \"" + implementationClassName.reflectionName() + "\"")
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
        .superclass(TRINO_STD_UDF_WRAPPER_CLASS_NAME)
        .addMethod(constructor)
        .addMethod(getStateClassNameMethod)
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

  private void generateStateClass(ClassName stateClassName, ClassName implementationClassName, File sourcesOutputDir,
                                  List<String> services) {
    /*
      Generates constructor ->

      public ${stateClassName}() {
        super();
        stdUDF = new ${implementationClassName};
      }
     */
    MethodSpec constructor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addStatement("super()")
            .addStatement("stdUDF = new $T()", implementationClassName)
            .build();

    /*
      Generates ->

      public class ${stateClassName} extends State {

        .
        .
        .

      }
     */
    TypeSpec wrapperClass = TypeSpec.classBuilder(stateClassName)
            .addModifiers(Modifier.PUBLIC)
            .superclass(TRINO_STD_UDF_WRAPPER_STATE_CLASS_NAME)
            .addMethod(constructor)
            .build();

    services.add(stateClassName.toString());
    JavaFile javaFile = JavaFile.builder(stateClassName.packageName(), wrapperClass)
            .skipJavaLangImports(true)
            .build();

    try {
      javaFile.writeTo(sourcesOutputDir);
    } catch (Exception e) {
      throw new RuntimeException("Error writing wrapper to file: ", e);
    }
  }
}
