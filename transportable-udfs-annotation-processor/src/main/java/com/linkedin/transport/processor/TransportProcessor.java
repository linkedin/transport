package com.linkedin.transport.processor;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
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
 * extend {@link StdUDF} and implement {@link TopLevelStdUDF}. It will then identify all overloadings of the same UDF
 * name and store them in the resource file.
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
      StringWriter writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      fatalError(writer.toString());
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
    log(String.format("Processing UDF Class: %s", udfClassElement.getQualifiedName()));

    TypeElement closestTopLevelStdUDFInterface = getClosestTopLevelStdUDFInterface(udfClassElement);
    if (closestTopLevelStdUDFInterface != null) {
      _udfProperties.addUDF(closestTopLevelStdUDFInterface.getQualifiedName().toString(),
          udfClassElement.getQualifiedName().toString());
    }
  }

  /**
   * Returns a {@link TypeElement} for the closest (in the type hierarchy) class/interface which implements the
   * {@link TopLevelStdUDF} interface for a given {@link StdUDF} class
   */
  private TypeElement getClosestTopLevelStdUDFInterface(TypeElement udfClassElement) {
    // If the UDF class directly implements TopLevelStdUDF, return the UDF class itself
    List<? extends TypeMirror> implementedInterfaces = udfClassElement.getInterfaces();
    for (TypeMirror iface : implementedInterfaces) {
      if (_types.isSameType(iface, _topLevelStdUDFInterfaceType)) {
        return udfClassElement;
      }
    }

    // Check if any of the interfaces/superclass of the UDF class implement TopLevelStdUDF
    TypeMirror superClass = udfClassElement.getSuperclass();
    boolean superClassImplementsTopLevelStdUDFInterface = _types.isAssignable(superClass, _topLevelStdUDFInterfaceType);
    LinkedList<TypeMirror> candidateInterfaces = new LinkedList<>();
    for (TypeMirror iface : implementedInterfaces) {
      if (_types.isAssignable(iface, _topLevelStdUDFInterfaceType)) {
        candidateInterfaces.add(iface);
      }
    }

    if (candidateInterfaces.size() > 1 || (candidateInterfaces.size() == 1
        && superClassImplementsTopLevelStdUDFInterface)) {
      // If more than one interface/superclass implements TopLevelStdUDF, Java will require the class to override the
      // methods of TopLevelStdUDF again, hence we can safely return the UDF class itself
      warn(Constants.MULTIPLE_INTERFACES_WARNING, udfClassElement);
      return udfClassElement;
    } else if (candidateInterfaces.size() == 1) {
      // If only one interface implements TopLevelStdUDF, return the interface
      return (TypeElement) _types.asElement(candidateInterfaces.getFirst());
    } else if (superClassImplementsTopLevelStdUDFInterface) {
      // If superclass indirectly implements TopLevelStdUDF, find the closest interface for the superclass
      return getClosestTopLevelStdUDFInterface((TypeElement) _types.asElement(superClass));
    } else {
      error(Constants.INTERFACE_NOT_IMPLEMENTED_ERROR, udfClassElement);
      return null;
    }
  }

  /**
   * Generates the UDF properties resource file in a pretty-printed JSON format
   */
  private void generateUDFPropertiesFile() {
    Filer filer = processingEnv.getFiler();
    try {
      FileObject fileObject = filer.createResource(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH);
      Writer writer = fileObject.openWriter();
      _udfProperties.toJson(writer);
      writer.close();
      log("Wrote Transport UDF properties file to: " + fileObject.toUri());
    } catch (IOException e) {
      fatalError(String.format("Unable to create UDF properties resource file: %s", e));
    }
  }

  /* Helper methods for logging */

  private void log(String msg) {
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
