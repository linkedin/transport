/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

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