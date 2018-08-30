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
import org.testng.annotations.Test;

import static org.testng.Assert.*;

// Suppressing type naming convention style check to be consistent with the way we name types in other classes, where
// they are static variables. They cannot be static variables here, but that is just because of the type system abstraction
@SuppressWarnings("checkstyle:membername")
public abstract class AbstractTestTypeFactory<T> {

  final private T LONG = getTypeSystem().createLongType();
  final private T INTEGER = getTypeSystem().createIntegerType();
  final private T STRING = getTypeSystem().createStringType();
  final private T BOOLEAN = getTypeSystem().createBooleanType();
  final private T NULL = getTypeSystem().createUnknownType();

  protected abstract AbstractTypeSystem<T> getTypeSystem();

  protected abstract AbstractTypeFactory<T> getTypeFactory();

  protected abstract AbstractBoundVariables<T> createBoundVariables();

  private T array(T elementType) {
    return getTypeSystem().createArrayType(elementType);
  }

  private T map(T keyType, T valueType) {
    return getTypeSystem().createMapType(keyType, valueType);
  }

  private T struct(T... argTypes) {
    return getTypeSystem().createStructType(null, Arrays.asList(argTypes));
  }

  private T struct(List<String> argNames, T... argTypes) {
    return getTypeSystem().createStructType(argNames, Arrays.asList(argTypes));
  }

  public void assertCreateType(String typeSignatureString, T dataType) {
    assertEquals(
        getTypeFactory().createType(TypeSignature.parse(typeSignatureString), createBoundVariables()), dataType);
  }

  @Test
  public void testCreateTypePrimitives() {
    assertCreateType("integer", INTEGER);
    assertCreateType("boolean", BOOLEAN);
    assertCreateType("bigint", LONG);
    assertCreateType("varchar", STRING);
  }

  @Test
  public void testCreateTypeArray() {
    assertCreateType("array(integer)", array(INTEGER));
    assertCreateType("array(array(integer))", array(array(INTEGER)));
  }

  @Test
  public void testCreateTypeMap() {
    assertCreateType("map(integer,varchar)", map(INTEGER, STRING));
    assertCreateType("map(map(integer,varchar), varchar)", map(map(INTEGER, STRING), STRING));
  }

  @Test
  public void testCreateTypeStruct() {
    assertCreateType("row(array(integer), varchar, map(varchar,varchar))",
        struct(array(INTEGER), STRING, map(STRING, STRING)));
    assertCreateType("row(arrField array(integer), strField varchar, mapField map(varchar,varchar), rowField row(integer))",
        struct(Arrays.asList("arrField", "strField", "mapField", "rowField"), array(INTEGER), STRING, map(STRING, STRING), struct(INTEGER))
    );
  }
}