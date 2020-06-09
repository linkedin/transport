/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class to represent which platform types (which are represented by the class parameter T) the generic parameters are
 * bound to at runtime. For example, if we have a signature map(K,V) that matched a platform type
 * map(array(string),array(long)) at runtime (i.e., query compile time), then K is bound to array(string),
 * and V is bound to array(long).
 */
public abstract class AbstractBoundVariables<T> {

  final Map<GenericTypeSignatureElement, T> _boundVariables;

  final AbstractTypeSystem<T> _typeSystem;

  public AbstractBoundVariables() {
    _boundVariables = new HashMap<>();
    _typeSystem = getTypeSystem();
  }

  protected abstract AbstractTypeSystem<T> getTypeSystem();

  private boolean isUnknownType(T dataType) {
    return _typeSystem.isUnknownType(dataType);
  }

  private boolean isBooleanType(T dataType) {
    return _typeSystem.isBooleanType(dataType);
  }

  private boolean isIntegerType(T dataType) {
    return _typeSystem.isIntegerType(dataType);
  }

  private boolean isLongType(T dataType) {
    return _typeSystem.isLongType(dataType);
  }

  private boolean isStringType(T dataType) {
    return _typeSystem.isStringType(dataType);
  }

  private boolean isFloatType(T dataType) {
    return _typeSystem.isFloatType(dataType);
  }

  private boolean isDoubleType(T dataType) {
    return _typeSystem.isDoubleType(dataType);
  }

  private boolean isBytesType(T dataType) {
    return _typeSystem.isBytesType(dataType);
  }

  private boolean isArrayType(T dataType) {
    return _typeSystem.isArrayType(dataType);
  }

  private boolean isMapType(T dataType) {
    return _typeSystem.isMapType(dataType);
  }

  private boolean isStructType(T dataType) {
    return _typeSystem.isStructType(dataType);
  }

  private T getArrayElementType(T dataType) {
    return _typeSystem.getArrayElementType(dataType);
  }

  private T getMapKeyType(T dataType) {
    return _typeSystem.getMapKeyType(dataType);
  }

  private T getMapValueType(T dataType) {
    return _typeSystem.getMapValueType(dataType);
  }

  private List<T> getStructFieldTypes(T dataType) {
    return _typeSystem.getStructFieldTypes(dataType);
  }

  /**
   * Binds a generic type signature element to a platform type. Throws an error if the generic type was already
   * bound to a different platform type.
   * @param genericTypeSignatureElement Input generic type.
   * @param dataType Input platform type to be bound to the generic type.
   */
  public boolean bind(GenericTypeSignatureElement genericTypeSignatureElement, T dataType) {
    T previous = _boundVariables.putIfAbsent(genericTypeSignatureElement, dataType);
    if (previous != null && !previous.equals(dataType)) {
      // Allow a generic type bound to an unknown type to be overridden
      if (isUnknownType(previous)) {
        _boundVariables.put(genericTypeSignatureElement, dataType);
        // Allow an unknown type to be accepted for an already bound generic
      } else if (!isUnknownType(dataType)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Unifies a type signature with an input platform type and binds generic parameters with their corresponding
   * platform types. Throws an error if the signature does not match the platform type for the concrete part.
   * @param typeSignature Input type signature.
   * @param dataType Input platform type to be unified with the input type signature
   */
  public boolean bind(TypeSignature typeSignature, T dataType) {
    TypeSignatureElement base = typeSignature.getBase();
    if (base.numParameters() == 0 && !base.acceptsVariableLengthParameters()) {
      if (base instanceof GenericTypeSignatureElement) {
        return bind((GenericTypeSignatureElement) base, dataType);
      } else if (base instanceof ConcreteTypeSignatureElement) {
        ConcreteTypeSignatureElement concreteType = (ConcreteTypeSignatureElement) base;
        boolean typeMismatch = false;
        switch (concreteType) {
          case BOOLEAN:
            if (!isBooleanType(dataType)) {
              typeMismatch = true;
            }
            break;
          case INTEGER:
            if (!isIntegerType(dataType)) {
              typeMismatch = true;
            }
            break;
          case LONG:
            if (!isLongType(dataType)) {
              typeMismatch = true;
            }
            break;
          case STRING:
            if (!isStringType(dataType)) {
              typeMismatch = true;
            }
            break;
          case FLOAT:
            if (!isFloatType(dataType)) {
              typeMismatch = true;
            }
            break;
          case DOUBLE:
            if (!isDoubleType(dataType)) {
              typeMismatch = true;
            }
            break;
          case BYTES:
            if (!isBytesType(dataType)) {
              typeMismatch = true;
            }
            break;
          case UNKNOWN:
            if (!isUnknownType(dataType)) {
              typeMismatch = true;
            }
            break;
          default:
            throw new RuntimeException("Unrecognized non-parametric type: " + concreteType);
        }
        return !typeMismatch || isUnknownType(dataType);
      }
    } else if (base instanceof ConcreteTypeSignatureElement) {
      ConcreteTypeSignatureElement concreteType = (ConcreteTypeSignatureElement) base;
      List<TypeSignature> parameters = typeSignature.getParameters();
      if (concreteType == ConcreteTypeSignatureElement.ARRAY) {
        if (parameters.size() == 1) {
          TypeSignature elementTypeSignature = typeSignature.getParameters().get(0);
          if (isArrayType(dataType)) {
            return bind(elementTypeSignature, getArrayElementType(dataType));
          } else if (isUnknownType(dataType)) {
            return bind(elementTypeSignature, dataType);
          } else {
            return false;
          }
        } else {
          throw new RuntimeException("Invalid type signature. Array type accepts only 1 parameter.");
        }
      } else if (concreteType == ConcreteTypeSignatureElement.MAP) {
        if (parameters.size() == 2) {
          TypeSignature keyTypeSignature = typeSignature.getParameters().get(0);
          TypeSignature valueTypeSignature = typeSignature.getParameters().get(1);
          if (isMapType(dataType)) {
            return
                bind(keyTypeSignature, getMapKeyType(dataType))
                    && bind(valueTypeSignature, getMapValueType(dataType));
          } else if (isUnknownType(dataType)) {
            return
                bind(keyTypeSignature, dataType)
                    && bind(valueTypeSignature, dataType);
          } else {
            return false;
          }
        } else {
          throw new RuntimeException("Invalid type signature. Map type accepts only 2 parameters.");
        }
      } else if (concreteType == ConcreteTypeSignatureElement.STRUCT) {
        if (isStructType(dataType)) {
          int i = 0;
          boolean bindingSuccess = true;
          for (TypeSignature parameter : parameters) {
            bindingSuccess = bindingSuccess && bind(
                parameter,
                getStructFieldTypes(dataType).get(i++)
            );
          }
          return bindingSuccess;
        } else if (isUnknownType(dataType)) {
          return parameters.stream().map(p -> bind(p, dataType)).reduce(Boolean::logicalAnd).get();
        } else {
          return false;
        }
      } else {
        throw new RuntimeException("Unrecognized type signature element: " + concreteType.name());
      }
    }
    return true;
  }

  /**
   * Returns the corresponding platform type to a given generic type parameter. Throws an error if it does not
   * recognize the generic parameter.
   */
  public T getBinding(GenericTypeSignatureElement genericTypeSignatureElement) {
    T binding = _boundVariables.get(genericTypeSignatureElement);
    if (binding == null) {
      throw new RuntimeException("Unrecognized generic parameter: " + genericTypeSignatureElement + ".");
    } else {
      return binding;
    }
  }
}
