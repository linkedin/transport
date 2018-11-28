/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

/**
 * Interface TypeSignatureElement to represent values of nodes in the TypeSignature tree. A node can be a parametric
 * concrete type, non-parametric concrete type (both captured in ConcreteTypeSignatureElement) or generic type
 * (captured in GenericTypeSignatureElement).
 */
public interface TypeSignatureElement {
  boolean acceptsVariableLengthParameters();

  int numParameters();
}
