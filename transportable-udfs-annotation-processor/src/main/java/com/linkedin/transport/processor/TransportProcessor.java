/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.compile.TransportUDFMetadata;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
 * The annotation processor does not rely on a special annotation. Instead it will look for non-abstract classes which
 * indirectly extend {@link StdUDF}. For each class, it will then perform checks against the class to ensure that there
 * is only one overriding of {@link TopLevelStdUDF} methods in its type hierarchy. If the checks are successful, it will
 * create groupings of UDFs which are considered to be overloadings of each other and store them in the resource file.
 */
@SupportedOptions({"debug"})
@SupportedAnnotationTypes({"*"})
public class TransportProcessor extends AbstractProcessor {

  private Types _types;
  private Elements _elements;
  private TypeMirror _topLevelStdUDFInterfaceType;
  private TypeMirror _stdUDFClassType;
  private TransportUDFMetadata _transportUdfMetadata;

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
    _transportUdfMetadata = new TransportUDFMetadata();
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
      generateUDFMetadataFile();
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
   * Finds the {@link TopLevelStdUDF} for the given {@link StdUDF} class and adds it to the list of discovered UDFs.
   * Also ensures that there is one and only one overriding of {@link TopLevelStdUDF} methods in the type hierarchy
   */
  private void processUDFClass(TypeElement udfClassElement) {
    debug(String.format("Processing UDF Class: %s", udfClassElement.getQualifiedName()));

    Set<TypeElement> elementsOverridingTopLevelStdUDFMethods =
        getElementsOverridingTopLevelStdUDFMethods(udfClassElement);

    if (elementsOverridingTopLevelStdUDFMethods.size() == 0) {
      error(Constants.INTERFACE_NOT_IMPLEMENTED_ERROR, udfClassElement);
    } else if (elementsOverridingTopLevelStdUDFMethods.size() > 1) {
      error(
          String.format("Multiple overridings of %s methods found in %s. %s",
              TopLevelStdUDF.class.getSimpleName(),
              elementsOverridingTopLevelStdUDFMethods.stream()
                  .map(TypeElement::getQualifiedName)
                  .collect(Collectors.joining(", ")),
              Constants.MORE_THAN_ONE_TYPE_OVERRIDING_ERROR),
          udfClassElement
      );
    } else {
      String topLevelStdUdfClassName =
          elementsOverridingTopLevelStdUDFMethods.iterator().next().getQualifiedName().toString();
      debug(String.format("TopLevelStdUDF class found: %s", topLevelStdUdfClassName));
      _transportUdfMetadata.addUDF(topLevelStdUdfClassName, udfClassElement.getQualifiedName().toString());
    }
  }

  /**
   * Returns all types (self + ancestors) in the type hierarchy of an element
   */
  private Stream<TypeMirror> getAllTypesInHierarchy(TypeMirror typeMirror) {
    return Stream.concat(Stream.of(typeMirror),
        _types.directSupertypes(typeMirror).stream().flatMap(this::getAllTypesInHierarchy));
  }

  /**
   * Finds all elements in the type hierarchy of a {@link TypeElement} which override {@link TopLevelStdUDF} methods
   */
  private Set<TypeElement> getElementsOverridingTopLevelStdUDFMethods(TypeElement typeElement) {
    return getAllTypesInHierarchy(typeElement.asType())
        .map(typeMirror -> (TypeElement) _types.asElement(typeMirror))
        .filter(this::typeElementOverridesTopLevelStdUDFMethods)
        .collect(Collectors.toSet());
  }

  /**
   * Returns true if the given {@link TypeElement} (class/interface) overrides {@link TopLevelStdUDF} methods
   */
  private boolean typeElementOverridesTopLevelStdUDFMethods(TypeElement typeElement) {

    Map<String, ExecutableElement> topLevelStdUDFMethods =
        ElementFilter.methodsIn(_types.asElement(_topLevelStdUDFInterfaceType).getEnclosedElements())
            .stream()
            .collect(Collectors.toMap(e -> e.getSimpleName().toString(), Function.identity()));

    // Check if any method defined in TopLevelStdUDF is being overriden in this class/interface
    // For simplicity we assume function names in TopLevelStdUDF are distinct
    return ElementFilter.methodsIn(typeElement.getEnclosedElements())
        .stream()
        .anyMatch(method -> {
          ExecutableElement matchingMethodFromTopLevelStdUDF =
              topLevelStdUDFMethods.get(method.getSimpleName().toString());
          return matchingMethodFromTopLevelStdUDF != null
              && _elements.overrides(method, matchingMethodFromTopLevelStdUDF, typeElement);
        });
  }

  /**
   * Generates the UDF metadata resource file in a pretty-printed JSON format
   */
  private void generateUDFMetadataFile() {
    Filer filer = processingEnv.getFiler();
    try {
      FileObject fileObject = filer.createResource(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH);
      try (Writer writer = fileObject.openWriter()) {
        _transportUdfMetadata.toJson(writer);
      }
      debug("Wrote Transport UDF metadata file to: " + fileObject.toUri());
    } catch (IOException e) {
      fatalError(String.format("Unable to create UDF metadata resource file: %s", e));
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
