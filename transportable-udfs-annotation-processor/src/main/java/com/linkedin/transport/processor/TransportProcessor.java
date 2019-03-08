/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;


/**
 * Annotation Processor for Transport UDFs which extracts UDF metadata from user defined UDF classes and stores it in a
 * resource file
 *
 * The annotation processor does not rely on a special annotation, instead it will look for non-abstract classes which
 * extend {@link StdUDF}. It will then perform checks against class to ensure that the type hierarchy meets the rules
 * mentioned below. If the class is considered to be a valid UDF class, it will create groupings of UDFs which are
 * considered to be overloadings of each other and store them in the resource file.
 *
 * Validation Rules:
 * Rule 1: A UDF class implementing TopLevelStdUDF through a superclass is not supported as of now since it is hard to
 * determine which interface we want to use as the base for the group of UDF overloading. This is not a common use case
 * anyway. If required, use composition instead of inheritance.
 * Rule 2: More than one interface implementing TopLevelStdUDF can lead to ambiguity with respect to the name of the UDF
 * and hence is not allowed.
 * Rule 3: A UDF class should implement TopLevelStdUDF either directly or indirectly through an interface only.
 * Rule 4: If the class does not directly implement TopLevelStdUDF, then verify that interface methods are not overriden
 * since it can lead to UDF name ambiguity.
 */
@SupportedOptions({"debug"})
@SupportedAnnotationTypes({"*"})
public class TransportProcessor extends AbstractProcessor {

  private Types _types;
  private Elements _elements;
  private TypeMirror _topLevelStdUDFInterfaceType;
  private TypeMirror _stdUDFClassType;
  private UDFProperties _udfProperties;

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latestSupported();
  }

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    _types = processingEnv.getTypeUtils();
    _elements = processingEnv.getElementUtils();
    _topLevelStdUDFInterfaceType = _elements.getTypeElement(TopLevelStdUDF.class.getName()).asType();
    _stdUDFClassType = _elements.getTypeElement(StdUDF.class.getName()).asType();
    _udfProperties = new UDFProperties();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    try {
      processImpl(roundEnv);
    } catch (Exception e) {
      // We don't allow exceptions of any kind to propagate to the compiler
      try (StringWriter stringWriter = new StringWriter(); PrintWriter printWriter = new PrintWriter(stringWriter)) {
        e.printStackTrace(printWriter);
        fatalError(stringWriter.toString());
      } catch (IOException ioe) {
        fatalError("Could not close resources " + ioe);
      }
    }
    // Universal processors should return false since other processor can be potentially acting on the same element
    return false;
  }

  private void processImpl(RoundEnvironment roundEnv) {
    if (roundEnv.processingOver()) {
      generateUDFPropertiesFile();
    } else {
      processElements(roundEnv.getRootElements());
    }
  }

  private void processElements(Set<? extends Element> elements) {
    for (Element element : elements) {
      // Check if the element is a non-abstract class which extends the StdUDF class
      if (element.getKind().equals(ElementKind.CLASS) && !element.getModifiers().contains(Modifier.ABSTRACT)
          && _types.isAssignable(element.asType(), _stdUDFClassType)) {
        processUDFClass((TypeElement) element);
      }
    }
  }

  /**
   * Finds the {@link TopLevelStdUDF} for the given {@link StdUDF} class and adds it to the list of discovered UDFs
   */
  private void processUDFClass(TypeElement udfClassElement) {
    debug(String.format("Processing UDF Class: %s", udfClassElement.getQualifiedName()));

    if (isValidUDFClass(udfClassElement)) {
      // At this point we have already verified that TopLevelStdUDF interface is coming from one and only one of the
      // implemented interfaces
      TypeElement elementImplementingTopLevelStdUDFInterface =
          getElementImplementingTopLevelStdUDFInterface(udfClassElement);

      _udfProperties.addUDF(elementImplementingTopLevelStdUDFInterface.getQualifiedName().toString(),
          udfClassElement.getQualifiedName().toString());
    }
  }

  /**
   * Returns the element(class/interface) implementing {@link TopLevelStdUDF} for the given class
   *
   * If the class directly implements {@link TopLevelStdUDF}, the class is returned.
   * If the class indirectly implements {@link TopLevelStdUDF} through an interface, the interface is returned.
   */
  private TypeElement getElementImplementingTopLevelStdUDFInterface(TypeElement udfClassElement) {
    TypeMirror ifaceImplementingTopLevelStdUDFInterface = udfClassElement.getInterfaces()
        .stream()
        .filter(iface -> _types.isAssignable(iface, _topLevelStdUDFInterfaceType))
        .findFirst().get();

    if (_types.isSameType(ifaceImplementingTopLevelStdUDFInterface, _topLevelStdUDFInterfaceType)) {
      return udfClassElement;
    } else {
      return (TypeElement) _types.asElement(ifaceImplementingTopLevelStdUDFInterface);
    }
  }

  /**
   * Performs validations on the type hierarchy to verify UDF overloading rules
   */
  private boolean isValidUDFClass(TypeElement udfClassElement) {
    // Check if any of the interfaces/superclass of the UDF class implement TopLevelStdUDF directly or indirectly
    TypeMirror superClass = udfClassElement.getSuperclass();
    boolean superClassImplementsTopLevelStdUDFInterface = _types.isAssignable(superClass, _topLevelStdUDFInterfaceType);
    LinkedList<TypeMirror> candidateInterfaces = new LinkedList<>();
    for (TypeMirror iface : udfClassElement.getInterfaces()) {
      if (_types.isAssignable(iface, _topLevelStdUDFInterfaceType)) {
        candidateInterfaces.add(iface);
      }
    }

    if (superClassImplementsTopLevelStdUDFInterface) {
      // Rule 1: A UDF class implementing TopLevelStdUDF through a superclass is not supported as of now since it is
      // hard to determine which interface we want to use as the base for the group of UDF overloading. This is not a
      // common use case anyway. If required, use composition instead of inheritance.
      error(Constants.SUPERCLASS_IMPLEMENTS_INTERFACE_ERROR, udfClassElement);
      return false;
    } else if (candidateInterfaces.size() > 1) {
      // Rule 2: More than one interface implementing TopLevelStdUDF can lead to ambiguity with respect to the name of
      // the UDF and hence is not allowed.
      error(Constants.MULTIPLE_INTERFACES_ERROR, udfClassElement);
      return false;
    } else if (candidateInterfaces.size() == 0) {
      // Rule 3: A UDF class should implement TopLevelStdUDF either directly or indirectly through an interface only
      error(Constants.INTERFACE_NOT_IMPLEMENTED_ERROR, udfClassElement);
      return false;
    } else if (!_types.isSameType(candidateInterfaces.getFirst(), _topLevelStdUDFInterfaceType)
        && classOverridesTopLevelStdUDFMethods(udfClassElement)) {
      // Rule 4: If the class does not directly implement TopLevelStdUDF, then verify that interface methods are not
      // overriden since it can lead to UDF name ambiguity
      error(Constants.CLASS_SHOULD_NOT_OVERRIDE_INTERFACE_METHODS_ERROR, udfClassElement);
      return false;
    } else {
      return true;
    }
  }

  /**
   * Returns true if the given class overrides {@link TopLevelStdUDF} methods coming from an interface
   */
  private boolean classOverridesTopLevelStdUDFMethods(TypeElement udfClassElement) {

    Map<String, ExecutableElement> topLevelStdUDFMethods = ElementFilter.methodsIn(
        _types.asElement(_topLevelStdUDFInterfaceType).getEnclosedElements())
        .stream().collect(Collectors.toMap(e -> e.getSimpleName().toString(), Function.identity()));

    // Check if any method also present in TopLevelStdUDF is being overriden in this class
    // For simplicity we assume function names in TopLevelStdUDF are distinct
    return ElementFilter.methodsIn(udfClassElement.getEnclosedElements())
        .stream()
        .anyMatch(method -> {
          ExecutableElement matchingMethodFromTopLevelStdUDF =
              topLevelStdUDFMethods.get(method.getSimpleName().toString());
          return matchingMethodFromTopLevelStdUDF != null
              && _elements.overrides(method, matchingMethodFromTopLevelStdUDF, udfClassElement);
        });
  }

  /**
   * Generates the UDF properties resource file in a pretty-printed JSON format
   */
  private void generateUDFPropertiesFile() {
    Filer filer = processingEnv.getFiler();
    try {
      FileObject fileObject = filer.createResource(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH);
      try (Writer writer = fileObject.openWriter()) {
        _udfProperties.toJson(writer);
      }
      debug("Wrote Transport UDF properties file to: " + fileObject.toUri());
    } catch (IOException e) {
      fatalError(String.format("Unable to create UDF properties resource file: %s", e));
    }
  }

  /* Helper methods for logging */

  private void debug(String msg) {
    if (processingEnv.getOptions().containsKey("debug")) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, msg);
    }
  }

  private void warn(String msg, Element element) {
    processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, msg, element);
  }

  private void error(String msg, Element element) {
    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, msg, element);
  }

  private void fatalError(String msg) {
    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "FATAL ERROR: " + msg);
  }
}
