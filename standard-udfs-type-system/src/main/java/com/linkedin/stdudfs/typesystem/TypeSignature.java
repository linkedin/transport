/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.typesystem;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.stdudfs.typesystem.ConcreteTypeSignatureElement.*;


/**
 * TypeSignature class represents types with type parameters, with generics support. A type signature consists of a base
 * type and its type parameters. Type parameters are recursively of type TypeSignature, hence the tree structure of type
 * signatures. Base types can be parametric (i.e., intermediate nodes in the type signature tree, or non-parametric
 * (i.e., leaf nodes in the type signature tree). Base types can be both concrete or generic, and are represented by
 * the class TypeSignatureElement. Generic types are always non-parametric. ConcreteTypeSignatureElement represents
 * concrete elements and GenericTypeSignatureElement represents generic signature elements.
 */
public class TypeSignature {
  final TypeSignatureElement _base;
  final List<TypeSignature> _parameters;
  final List<String> _parameterNames;

  protected TypeSignature(TypeSignatureElement base, List<TypeSignature> parameters) {
    this(base, parameters, null);
  }

  protected TypeSignature(TypeSignatureElement base, List<TypeSignature> parameters, List<String> parameterNames) {
    if (!base.acceptsVariableLengthParameters()) {
      boolean isNumParametersExpected =
          (parameters == null && base.numParameters() == 0)
              || (parameters.size() == base.numParameters());
      if (!isNumParametersExpected) {
        throw new RuntimeException("Unexpected number of parameters for type: " + base);
      }
    }
    if (parameters == null || parameterNames != null && parameterNames.isEmpty()) {
      parameterNames = null;
    }
    if (parameterNames != null) {
      if (!base.equals(STRUCT)) {
        throw new RuntimeException("Parameter names only allowed for row type. Found: " + base);
      }
      if (parameters.size() != parameterNames.size()) {
        throw new RuntimeException("Unexpected number of parameter names. Expected: " + parameters.size() + " Found: "
            + parameterNames.size());
      }
      parameterNames.forEach(name -> {
        if (!validId(name)) {
          throw new RuntimeException("Illegal identifier: " + name);
        }
      });
    }
    _base = base;
    _parameters = parameters;
    _parameterNames = parameterNames;
  }

  /**
   * Method to parse an input string and generate the corresponding type signature represented by this class.
   * For example, given the string map(array(long),K), it would return the following TypeSignature:
   *
   *                                 map (ConcreteTypeSignatureElement)
   *                                 /                             \
   *                                /                               \
   *                               /                                 \
   *               array (ConcreteTypeSignatureElement)       K (GenericTypeSignatureElement)
   *                               |
   *                               |
   *               LONG (ConcreteTypeSignatureElement)
   *
   * @param typeSignature Input type signature to parse
   * @return Corresponding TypeSignature representation.
   */
  public static TypeSignature parse(String typeSignature) {
    Stack<StringBuilder> baseStringBuilderStack = new Stack<>();
    Stack<List<TypeSignature>> parametersStack = new Stack<>();
    Stack<List<String>> parameterNamesStack = new Stack<>();
    StringBuilder currentBaseStringBuilder = new StringBuilder();
    List<TypeSignature> currentParameters = null;
    List<String> currentParameterNames = null;
    boolean rightParenthesisWasLastToken = false;
    for (char c : typeSignature.toCharArray()) {
      if (c == '(') {
        baseStringBuilderStack.push(currentBaseStringBuilder);
        parametersStack.push(new ArrayList<>());
        parameterNamesStack.push(new ArrayList<>());
        currentBaseStringBuilder = new StringBuilder();
        currentParameters = null;
        currentParameterNames = null;
      } else if (c == ')') {
        if (parametersStack.isEmpty()) {
          throw new RuntimeException("Unexpected ')'.");
        }
        addCurrentTypeSignatureToStackPeek(currentBaseStringBuilder, currentParameters, parametersStack,
            currentParameterNames, parameterNamesStack);
        currentBaseStringBuilder = baseStringBuilderStack.pop();
        currentParameters = parametersStack.pop();
        currentParameterNames = parameterNamesStack.pop();
        rightParenthesisWasLastToken = true;
      } else if (c == ',') {
        addCurrentTypeSignatureToStackPeek(currentBaseStringBuilder, currentParameters, parametersStack,
            currentParameterNames, parameterNamesStack);
        currentBaseStringBuilder = new StringBuilder();
        currentParameters = null;
        currentParameterNames = null;
        rightParenthesisWasLastToken = false;
      } else {
        if (rightParenthesisWasLastToken && !Character.isWhitespace(c)) {
          throw new RuntimeException("Illegal character: " + c + ". Expecting ',', or <EOL>.");
        }
        currentBaseStringBuilder.append(c);
      }
    }
    if (!baseStringBuilderStack.isEmpty()) {
      throw new RuntimeException("Expecting ')'.");
    }
    return new TypeSignature(getTypeSignatureElement(currentBaseStringBuilder.toString().trim()), currentParameters,
        currentParameterNames);
  }

  private static void addCurrentTypeSignatureToStackPeek(StringBuilder currentBaseStringBuilder,
      List<TypeSignature> currentParameters, Stack<List<TypeSignature>> parameterStack,
      List<String> currentParameterNames,
      Stack<List<String>> parameterNamesStack) {
    String currentBase = currentBaseStringBuilder.toString().trim();
    String name = null;
    if (currentBase.contains(" ")) {
      String[] parts = currentBase.split("\\s+", 2);
      name = parts[0];
      currentBase = parts[1];
    }
    parameterStack.peek()
        .add(new TypeSignature(getTypeSignatureElement(currentBase), currentParameters, currentParameterNames));
    if (name != null) {
      parameterNamesStack.peek().add(name);
    }
  }

  private static TypeSignatureElement getTypeSignatureElement(String currentBase) {
    TypeSignatureElement currentBaseElement;
    if (validId(currentBase)) {
      switch (currentBase.toLowerCase()) {
        case "boolean":
          currentBaseElement = BOOLEAN;
          break;
        case "integer":
          currentBaseElement = INTEGER;
          break;
        case "bigint":
          currentBaseElement = LONG;
          break;
        case "varchar":
          currentBaseElement = STRING;
          break;
        case "array":
          currentBaseElement = ARRAY;
          break;
        case "map":
          currentBaseElement = MAP;
          break;
        case "row":
          currentBaseElement = STRUCT;
          break;
        default:
          currentBaseElement = new GenericTypeSignatureElement(currentBase);
          break;
      }
    } else {
      throw new RuntimeException("Illegal identifier: " + currentBase);
    }
    return currentBaseElement;
  }

  private static boolean validId(String s) {
    return s.matches("[_a-zA-Z][_a-zA-Z0-9]*");
  }

  public TypeSignatureElement getBase() {
    return _base;
  }

  public List<TypeSignature> getParameters() {
    return _parameters;
  }

  public List<String> getParameterNames() {
    return _parameterNames;
  }

  public String toString() {
    if (_parameters == null) {
      return _base.toString();
    } else if (_parameterNames == null) {
      return _base + "("
          + _parameters.stream().map(p -> p.toString()).collect(Collectors.joining(","))
          + ")";
    } else {
      return _base + "("
          + IntStream.range(0, _parameterNames.size()).boxed()
          .map(i -> _parameterNames.get(i) + " " + _parameters.get(i).toString())
          .collect(Collectors.joining(","))
          + ")";
    }
  }

  public int hashCode() {
    return toString().hashCode();
  }

  public boolean equals(Object other) {
    return toString().equals(other.toString());
  }

  public Set<GenericTypeSignatureElement> getGenericTypeSignatureElements() {
    Set<GenericTypeSignatureElement> genericTypes = new HashSet<>();
    getGenericTypeSignatureElements(genericTypes);
    return genericTypes;
  }

  private void getGenericTypeSignatureElements(Set<GenericTypeSignatureElement> genericTypes) {
    if (getBase() instanceof GenericTypeSignatureElement) {
      genericTypes.add((GenericTypeSignatureElement) getBase());
    } else if (getBase() instanceof ConcreteTypeSignatureElement) {
      if (getParameters() != null) {
        for (TypeSignature s : getParameters()) {
          s.getGenericTypeSignatureElements(genericTypes);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unrecognized TypeSignatureElement type: " + getBase().getClass());
    }
  }
}
