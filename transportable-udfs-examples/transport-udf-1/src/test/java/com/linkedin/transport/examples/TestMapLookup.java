package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.StdTester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestMapLookup extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(MapLookup.class, ImmutableList.of(MapLookup.class));
  }

  @Test
  public void testMapLookup() {
    StdTester tester = getTester();
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), "a", "x"), "x", "varchar");
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), "a", ".*"), "x", "varchar");
    tester.check(functionCall("map_lookup", null, "a", "x"), null, "varchar");
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), null, "(x)"), "x", "varchar");
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), "    ", "(x)"), "x", "varchar");
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), null, "(\\w)"), "x", "varchar");

    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), "COMPANY_ID",
        "COMPANY:([0-9]+)"), "COMPANY:100", "varchar");
    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), null,
        "COMPANY:([0-9]+)"), "100", "varchar");
    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), "     ",
        "COMPANY:([0-9]+)"), "100", "varchar");
    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), "JOB_ID, JOB-ID",
        "JOB:([0-9]+)"), null, "varchar");
    tester.check(functionCall("map_lookup", null, "COMPANY_ID, COMPANY-ID",
        "COMPANY:([0-9]+)"), null, "varchar");
  }

  @Test
  public void test() {
    StdTester tester = getTester();
    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), null,
        "COMPANY:([0-9]+)"), "100", "varchar");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testMapLookupFailNull() {
    StdTester tester = getTester();
    tester.check(functionCall("map_lookup", map("COMPANY_ID", "COMPANY:100", "COMPANY ID", "COMPANY:200"), null,
        "COMPANY:[0-9]+"), null, "varchar");
    tester.check(functionCall("map_lookup", map("a", "x", "b", "y"), null, "x"), null, "varchar");
  }
}