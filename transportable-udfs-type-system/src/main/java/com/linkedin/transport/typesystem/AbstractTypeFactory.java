/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

import java.util.List;
import java.util.stream.Collectors;


public abstract class AbstractTypeFactory<T> {

  private final AbstractTypeSystem<T> _typeSystem;

  public AbstractTypeFactory() {
    _typeSystem = getTypeSystem();
  }

  protected abstract AbstractTypeSystem<T> getTypeSystem();

  private T createBooleanType() {
    return _typeSystem.createBooleanType();
  }

  private T createIntegerType() {
    return _typeSystem.createIntegerType();
  }

  private T createLongType() {
    return _typeSystem.createLongType();
  }

  private T createStringType() {
    return _typeSystem.createStringType();
  }

  private T createUnknownType() {
    return _typeSystem.createUnknownType();
  }

  private T createArrayType(T elementType) {
    return _typeSystem.createArrayType(elementType);
  }

  private T createMapType(T keyType, T valueType) {
    return _typeSystem.createMapType(keyType, valueType);
  }

  private T createStructType(List<String> fieldNames, List<T> fieldTypes) {
    return _typeSystem.createStructType(fieldNames, fieldTypes);
  }

  public T createType(TypeSignature typeSignatureTree, AbstractBoundVariables<T> boundVariables) {
    TypeSignatureElement base = typeSignatureTree.getBase();
    if (base.numParameters() == 0 && !base.acceptsVariableLengthParameters()) {
      if (base instanceof GenericTypeSignatureElement) {
        T binding = boundVariables.getBinding((GenericTypeSignatureElement) base);
        if (binding == null) {
          throw new RuntimeException("Unknown generic parameter: " + base);
        }
        return binding;
      } else if (base instanceof ConcreteTypeSignatureElement) {
        ConcreteTypeSignatureElement concreteType = (ConcreteTypeSignatureElement) base;
        switch (concreteType) {
          case BOOLEAN:
            return createBooleanType();
          case INTEGER:
            return createIntegerType();
          case LONG:
            return createLongType();
          case STRING:
            return createStringType();
          default:
            throw new RuntimeException("Unrecognized non-parametric type: " + concreteType);
        }
      }
    } else if (base instanceof ConcreteTypeSignatureElement) {
      ConcreteTypeSignatureElement concreteType = (ConcreteTypeSignatureElement) base;
      List<TypeSignature> parameters = typeSignatureTree.getParameters();
      if (concreteType == ConcreteTypeSignatureElement.ARRAY) {
        assert parameters.size() == 1;
        return
            createArrayType(
                createType(parameters.get(0), boundVariables)
            );
      } else if (concreteType == ConcreteTypeSignatureElement.MAP) {
        assert parameters.size() == 2;
        return
            createMapType(
                createType(parameters.get(0), boundVariables),
                createType(parameters.get(1), boundVariables)
            );
      } else if (concreteType == ConcreteTypeSignatureElement.STRUCT) {
        return createStructType(
            typeSignatureTree.getParameterNames(),
            parameters.stream().map(p -> createType(p, boundVariables)).collect(Collectors.toList())
        );
      } else {
        throw new RuntimeException("Unrecognized type signature element: " + concreteType.name());
      }
    }
    return null;
  }
}
