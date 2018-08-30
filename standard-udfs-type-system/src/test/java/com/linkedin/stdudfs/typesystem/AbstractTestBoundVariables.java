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
