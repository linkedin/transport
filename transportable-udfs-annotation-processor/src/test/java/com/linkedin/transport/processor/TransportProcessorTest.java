/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import java.io.IOException;
import java.nio.charset.Charset;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.testng.annotations.Test;

import static com.google.testing.compile.JavaFileObjects.*;
import static com.google.testing.compile.JavaSourcesSubject.*;


/**
 * Tests the {@link TransportProcessor}
 */
public class TransportProcessorTest {

  @Test
  public void simpleUDF() throws IOException {
    assertThat(
        forResource("udfs/SimpleUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/simpleUDF.json")));
  }

  @Test
  public void overloadedUDF() throws IOException {
    assertThat(
        forResource("udfs/UDFInterface1.java"),
        forResource("udfs/OverloadedUDFInt.java"),
        forResource("udfs/OverloadedUDFString.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/overloadedUDF.json")));
  }

  @Test
  public void udfWithMultipleInterfaces() throws IOException {
    assertThat(
        forResource("udfs/UDFInterface1.java"),
        forResource("udfs/UDFInterface2.java"),
        forResource("udfs/UDFWithMultipleInterfaces1.java"),
        forResource("udfs/AbstractUDF.java"),
        forResource("udfs/UDFWithMultipleInterfaces2.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .withWarningCount(2)
        .withWarningContaining(Constants.MULTIPLE_INTERFACES_WARNING)
        .in(forResource("udfs/UDFWithMultipleInterfaces1.java"))
        .onLine(14)
        .atColumn(8)
        .and()
        .withWarningContaining(Constants.MULTIPLE_INTERFACES_WARNING)
        .in(forResource("udfs/UDFWithMultipleInterfaces2.java"))
        .onLine(14)
        .atColumn(8)
        .and()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/udfWithMultipleInterfaces.json")));
  }

  @Test
  public void udfNotImplementingTopLevelStdUDF() throws IOException {
    assertThat(
        forResource("udfs/UDFNotImplementingTopLevelStdUDF.java")
    ).processedWith(new TransportProcessor())
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(Constants.INTERFACE_NOT_IMPLEMENTED_ERROR)
        .in(forResource("udfs/UDFNotImplementingTopLevelStdUDF.java"))
        .onLine(14)
        .atColumn(8);
  }

  @Test
  public void abstractUDF() throws IOException {
    assertThat(
        forResource("udfs/AbstractUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/empty.json")));
  }

  @Test
  public void nonUDF() throws IOException {
    assertThat(
        forResource("udfs/NonUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/empty.json")));
  }

  @Test
  public void innerClassUDFNotSupported() throws IOException {
    assertThat(
        forResource("udfs/InnerClassUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/empty.json")));
  }

  static String asString(final JavaFileObject javaFileObject) throws IOException {
    return javaFileObject.getCharContent(true).toString();
  }
}
