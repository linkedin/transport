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


import static com.linkedin.transport.typesystem.TypeSignatureFactory.*;


public class TestTypeSignature {

  @Test
  public void testTypeSignatureParse() {
    Assert.assertEquals(TypeSignature.parse("varchar"), STRING);

    Assert.assertEquals(TypeSignature.parse("integer"), INTEGER);

    Assert.assertEquals(TypeSignature.parse("bigint"), LONG);

    Assert.assertEquals(TypeSignature.parse("real"), FLOAT);

    Assert.assertEquals(TypeSignature.parse("double"), DOUBLE);

    Assert.assertEquals(TypeSignature.parse("bytes"), BYTES);

    Assert.assertEquals(TypeSignature.parse("array(bigint)"), array(LONG));

    Assert.assertEquals(TypeSignature.parse("array(unknown)"), array(NULL));

    Assert.assertEquals(TypeSignature.parse("array(map(varchar,boolean))"), array(map(STRING, BOOLEAN)));

    Assert.assertEquals(
        TypeSignature.parse("array(row(varchar,boolean,integer,real,double,bytes))"),
        array(struct(STRING, BOOLEAN, INTEGER, FLOAT, DOUBLE, BYTES)));
  }

  @Test
  public void testTypeSignatureParseRow() {
    Assert.assertEquals(TypeSignature.parse("row(boolean,boolean)"), struct(BOOLEAN, BOOLEAN));

    Assert.assertEquals(TypeSignature.parse("row(K,V)"), struct(generic("K"), generic("V")));

    Assert.assertEquals(TypeSignature.parse("row(a boolean,b boolean)"),
        struct(Arrays.asList(BOOLEAN, BOOLEAN), Arrays.asList("a", "b")));

    Assert.assertEquals(TypeSignature.parse("row(a boolean,b map(boolean, row(c boolean, d boolean)))"),
        struct(Arrays.asList(BOOLEAN, map(BOOLEAN, struct(Arrays.asList(BOOLEAN, BOOLEAN), Arrays.asList("c", "d")))),
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
