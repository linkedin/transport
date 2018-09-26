/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.typesystem;

import java.util.Arrays;
import java.util.List;


public class TypeSignatureFactory {
  private TypeSignatureFactory() {
  }

  final public static TypeSignature BOOLEAN = new TypeSignature(ConcreteTypeSignatureElement.BOOLEAN, null);
  final public static TypeSignature INTEGER = new TypeSignature(ConcreteTypeSignatureElement.INTEGER, null);
  final public static TypeSignature LONG = new TypeSignature(ConcreteTypeSignatureElement.LONG, null);
  final public static TypeSignature STRING = new TypeSignature(ConcreteTypeSignatureElement.STRING, null);

  public static TypeSignature array(TypeSignature elementTypeSignature) {
    return new TypeSignature(ConcreteTypeSignatureElement.ARRAY, Arrays.asList(elementTypeSignature));
  }

  public static TypeSignature map(TypeSignature keyTypeSignature, TypeSignature valueTypeSignature) {
    return new TypeSignature(ConcreteTypeSignatureElement.MAP, Arrays.asList(keyTypeSignature, valueTypeSignature));
  }

  public static TypeSignature struct(TypeSignature... fieldTypeSignature) {
    return new TypeSignature(ConcreteTypeSignatureElement.STRUCT, Arrays.asList(fieldTypeSignature));
  }

  public static TypeSignature struct(List<TypeSignature> fieldTypeSignatures, List<String> fieldNames) {
    return new TypeSignature(ConcreteTypeSignatureElement.STRUCT, fieldTypeSignatures, fieldNames);
  }

  static TypeSignature generic(String name) {
    return new TypeSignature(new GenericTypeSignatureElement(name), null);
  }
}
