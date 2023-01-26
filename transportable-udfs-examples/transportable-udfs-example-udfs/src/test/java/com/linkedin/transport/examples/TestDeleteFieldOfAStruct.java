package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.StdTester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestDeleteFieldOfAStruct extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(DeleteFieldOfAStruct.class, ImmutableList.of(DeleteFieldOfAStruct.class));
  }

  @Test
  public void testDeleteFieldOfAStructFunction() {
    StdTester tester = getTester();
    Row data = rowWithFieldNames(
        ImmutableList.of("firstName","lastName"),
        ImmutableList.of("foo","bar"));
    Row dataAfterDeleteFirstName = rowWithFieldNames(
        ImmutableList.of("firstName","lastName"),
        ImmutableList.of("","bar")); // <- value corresponding to field name "firstName" has been removed
    Row dataAfterDeleteLastName = rowWithFieldNames(
        ImmutableList.of("firstName","lastName"),
        ImmutableList.of("foo","")); // <- value corresponding to field name "lastName" has been removed
    tester.check(
        functionCall("deleteFieldOfAStruct", data, "firstName"),
        dataAfterDeleteFirstName,
        "row(varchar, varchar)");
    tester.check(
        functionCall("deleteFieldOfAStruct", data, "lastName"),
        dataAfterDeleteLastName,
        "row(varchar, varchar)");
  }
}
