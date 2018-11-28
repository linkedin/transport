/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

import java.util.Arrays;
import java.util.HashSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTypeSignature {

  @Test
  public void testTypeSignatureParse() {
    Assert.assertEquals(TypeSignature.parse("varchar"), TypeSignatureFactory.STRING);

    Assert.assertEquals(TypeSignature.parse("integer"), TypeSignatureFactory.INTEGER);

    Assert.assertEquals(TypeSignature.parse("bigint"), TypeSignatureFactory.LONG);

    Assert.assertEquals(TypeSignature.parse("array(bigint)"), TypeSignatureFactory.array(TypeSignatureFactory.LONG));

    Assert.assertEquals(TypeSignature.parse("array(map(varchar,boolean))"), TypeSignatureFactory.array(
        TypeSignatureFactory.map(TypeSignatureFactory.STRING, TypeSignatureFactory.BOOLEAN)));

    Assert.assertEquals(
        TypeSignature.parse("array(row(varchar,boolean,integer))"),
        TypeSignatureFactory.array(TypeSignatureFactory.struct(TypeSignatureFactory.STRING, TypeSignatureFactory.BOOLEAN, TypeSignatureFactory.INTEGER)));
  }

  @Test
  public void testTypeSignatureParseRow() {
    Assert.assertEquals(TypeSignature.parse("row(boolean,boolean)"), TypeSignatureFactory.struct(
        TypeSignatureFactory.BOOLEAN, TypeSignatureFactory.BOOLEAN));

    Assert.assertEquals(TypeSignature.parse("row(K,V)"), TypeSignatureFactory.struct(TypeSignatureFactory.generic("K"), TypeSignatureFactory
        .generic("V")));

    Assert.assertEquals(TypeSignature.parse("row(a boolean,b boolean)"),
        TypeSignatureFactory.struct(Arrays.asList(TypeSignatureFactory.BOOLEAN, TypeSignatureFactory.BOOLEAN), Arrays.asList("a", "b")));

    Assert.assertEquals(TypeSignature.parse("row(a boolean,b map(boolean, row(c boolean, d boolean)))"),
        TypeSignatureFactory.struct(Arrays.asList(TypeSignatureFactory.BOOLEAN, TypeSignatureFactory.map(
            TypeSignatureFactory.BOOLEAN, TypeSignatureFactory.struct(Arrays.asList(TypeSignatureFactory.BOOLEAN,
                TypeSignatureFactory.BOOLEAN), Arrays.asList("c", "d")))),
            Arrays.asList("a", "b")));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors1() {
    TypeSignature.parse("k(boolean,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors2() {
    TypeSignature.parse("map((boolean,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors3() {
    TypeSignature.parse("array(boolean,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors4() {
    TypeSignature.parse("map(boolean,boolean))");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors5() {
    TypeSignature.parse("map(boolean,boolean))");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors6() {
    TypeSignature.parse("map(boolean,,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors7() {
    TypeSignature.parse("ar(boolean)ray");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors8() {
    TypeSignature.parse("row(a boolean,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors9() {
    TypeSignature.parse("map(a boolean,boolean)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors10() {
    TypeSignature.parse("row(a boolean,row(b boolean))");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTypeSignatureParseErrors11() {
    TypeSignature.parse("row(1a boolean,b boolean)");
  }

  @Test
  public void testTypeSignatureGetGenericTypes() {
    Assert.assertEquals(
        TypeSignature.parse("row(boolean,boolean)").getGenericTypeSignatureElements(),
        new HashSet<>());
    Assert.assertEquals(
        TypeSignature.parse("row(K,V)").getGenericTypeSignatureElements(),
        new HashSet<>(Arrays.asList(
            new GenericTypeSignatureElement("K"),
            new GenericTypeSignatureElement("V"))));
    Assert.assertEquals(
        TypeSignature.parse("map(K,K)").getGenericTypeSignatureElements(),
        new HashSet<>(Arrays.asList(
            new GenericTypeSignatureElement("K"))));
    Assert.assertEquals(
        TypeSignature.parse("row(array(map(K,varchar)),row(V,integer))").getGenericTypeSignatureElements(),
        new HashSet<>(Arrays.asList(
            new GenericTypeSignatureElement("K"),
            new GenericTypeSignatureElement("V"))));
  }
}
