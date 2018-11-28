/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

/**
 * Different types of concrete type signature elements, along with their variadic property and their number of
 * parameters.
 */
public enum ConcreteTypeSignatureElement implements TypeSignatureElement {
  BOOLEAN(false, 0),
  INTEGER(false, 0),
  LONG(false, 0),
  STRING(false, 0),
  ARRAY(false, 1),
  MAP(false, 2),
  STRUCT(true, 0);

  final boolean _acceptsVariableLengthParameters;
  final int _numParameters;

  ConcreteTypeSignatureElement(boolean acceptsVariableLengthParameters, int numParameters) {
    _acceptsVariableLengthParameters = acceptsVariableLengthParameters;
    _numParameters = numParameters;
  }

  @Override
  public int numParameters() {
    return _numParameters;
  }

  @Override
  public boolean acceptsVariableLengthParameters() {
    return _acceptsVariableLengthParameters;
  }
}
