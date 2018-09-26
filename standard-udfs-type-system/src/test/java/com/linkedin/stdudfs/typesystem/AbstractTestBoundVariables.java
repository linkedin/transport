/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.typesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


// Suppressing type naming convention style check to be consistent with the way we name types in other classes, where
// they are static variables. They cannot be static variables here, but that is just because of the type system abstraction
@SuppressWarnings("checkstyle:membername")
public abstract class AbstractTestBoundVariables<T> {

  final private T LONG = getTypeSystem().createLongType();
  final private T INTEGER = getTypeSystem().createIntegerType();
  final private T STRING = getTypeSystem().createStringType();
  final private T BOOLEAN = getTypeSystem().createBooleanType();
  final private T NULL = getTypeSystem().createUnknownType();

  protected abstract AbstractTypeSystem<T> getTypeSystem();

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

  private void assertBoundVariables(List<String> inputTypeSignatures, List<T> inputTypes,
      Map<String, T> expectedBindings) {
    AbstractBoundVariables<T> boundVariables = createBoundVariables();

    boolean bindingSuccess = true;
    for (int i = 0; i < inputTypeSignatures.size(); i++) {
      bindingSuccess = bindingSuccess
          && boundVariables.bind(TypeSignature.parse(inputTypeSignatures.get(i)), inputTypes.get(i));
    }

    if (expectedBindings != null) {
      Assert.assertTrue(bindingSuccess);
      for (Map.Entry<String, T> binding : expectedBindings.entrySet()) {
        Assert.assertEquals(
            boundVariables.getBinding((GenericTypeSignatureElement) TypeSignature.parse(binding.getKey()).getBase()),
            binding.getValue());
      }
    } else {
      Assert.assertFalse(bindingSuccess);
    }
  }

  @Test
  public void testBoundVariables1() {
    assertBoundVariables(
        ImmutableList.of(
            "map(K,V)",
            "K"
        ),
        ImmutableList.of(
            map(STRING, BOOLEAN),
            STRING
        ),
        ImmutableMap.of(
            "K", STRING,
            "V", BOOLEAN
        )
    );
  }

  @Test
  public void testBoundVariables2() {
    assertBoundVariables(
        ImmutableList.of(
            "map(K,array(V))",
            "K"
        ),
        ImmutableList.of(
            map(STRING, array(array(struct(BOOLEAN, STRING)))),
            STRING
        ),
        ImmutableMap.of(
            "K", STRING,
            "V", array(struct(BOOLEAN, STRING))
        )
    );
  }

  @Test
  public void testBoundVariablesVoids() {
    // An unknown type can bind to any type signature (concrete or generic) / (primitive or complex)
    assertBoundVariables(
        ImmutableList.of(
            "K",
            "string",
            "array(A)",
            "row(B,C,D)"
        ),
        ImmutableList.of(
            NULL,
            NULL,
            NULL,
            NULL
        ),
        ImmutableMap.of(
            "A", NULL,
            "B", NULL,
            "C", NULL,
            "D", NULL,
            "K", NULL
        )
    );
    assertBoundVariables(
        ImmutableList.of(
            "map(A,B)",
            "map(C,D)"
        ),
        ImmutableList.of(
            NULL,
            map(STRING, NULL)
        ),
        ImmutableMap.of(
            "A", NULL,
            "B", NULL,
            "C", STRING,
            "D", NULL
        )
    );

    // An unknown type bound to a generic type can be overridden by other OIs
    assertBoundVariables(
        ImmutableList.of(
            "map(K, V)",
            "row(V)"
        ),
        ImmutableList.of(
            map(STRING, NULL),
            struct(STRING)
        ),
        ImmutableMap.of(
            "K", STRING,
            "V", STRING
        )
    );

    // An unknown type should not override any existing binding
    assertBoundVariables(
        ImmutableList.of(
            "map(K, V)",
            "row(V)",
            "array(V)"
        ),
        ImmutableList.of(
            map(STRING, NULL),
            struct(STRING),
            array(NULL)
        ),
        ImmutableMap.of(
            "K", STRING,
            "V", STRING
        )
    );
    assertBoundVariables(
        ImmutableList.of(
            "map(K, V)",
            "row(V)",
            "array(V)"
        ),
        ImmutableList.of(
            map(STRING, NULL),
            struct(STRING),
            NULL
        ),
        ImmutableMap.of(
            "K", STRING,
            "V", STRING
        )
    );
  }

  @Test
  public void testBoundVariableFailure1() {
    assertBoundVariables(
        ImmutableList.of(
            "map(K, V)"
        ),
        ImmutableList.of(
            array(LONG)
        ),
        null
    );
  }

  @Test
  public void testBoundVariableFailure2() {
    assertBoundVariables(
        ImmutableList.of(
            "map(K, V)",
            "map(K, V)"
        ),
        ImmutableList.of(
            map(STRING, INTEGER),
            map(STRING, BOOLEAN)
        ),
        null
    );
  }
}
