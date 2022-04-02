/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Iterator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.transport.trino.StdUDFUtils.quoteReservedKeywords;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;


public class TestQuoteReservedKeywords {

  @DataProvider(name = "typeSignatures")
  public Iterator<Object[]> getTypeSignatures() {
    return ImmutableList.<List<String>>builder().add(ImmutableList.of("row(varchar,integer)", "row(varchar,integer)"))
        .add(ImmutableList.of("array(row(key varchar,value varchar))", "array(row(key varchar,value varchar))"))
        .add(ImmutableList.of("array(row(valuess varchar,key varchar))", "array(row(valuess varchar,key varchar))"))
        .add(ImmutableList.of("array(row(varchar,boolean,integer,real,double,varbinary))",
            "array(row(varchar,boolean,integer,real,double,varbinary))"))
        .add(ImmutableList.of("array(row(key varchar,values varchar))", "array(row(key varchar,\"values\" varchar))"))
        .add(ImmutableList.of("array(row(values varchar,key varchar))", "array(row(\"values\" varchar,key varchar))"))
        .add(ImmutableList.of("map(row(values varchar,key varchar),row(values varchar,key varchar))",
            "map(row(\"values\" varchar,key varchar),row(\"values\" varchar,key varchar))"))
        .add(ImmutableList.of("row(order row(values varchar,current_user varchar))",
            "row(\"order\" row(\"values\" varchar,\"current_user\" varchar))"))
        .add(ImmutableList.of("row(order row(order varchar,current_user varchar))",
            "row(\"order\" row(\"order\" varchar,\"current_user\" varchar))"))
        .build()
        .stream()
        .map(x -> new Object[]{x.get(0), x.get(1)})
        .iterator();
  }

  @Test(dataProvider = "typeSignatures")
  public void testQuoteReservedKeywords(String typeSignature, String expected) {
    Assert.assertEquals(quoteReservedKeywords(typeSignature), expected);
    parseTypeSignature(quoteReservedKeywords(typeSignature), ImmutableSet.of());
  }
}
