/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
