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
        forResource("udfs/OverloadedUDF.java"),
        forResource("udfs/OverloadedUDFInt.java"),
        forResource("udfs/OverloadedUDFString.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/overloadedUDF.json")));
  }

  @Test
  public void superClassShouldNotImplementInterface() throws IOException {
    assertThat(
        forResource("udfs/AbstractUDFImplementingInterface.java"),
        forResource("udfs/UDFForAbstractUDFImplementingInterface.java")
    ).processedWith(new TransportProcessor())
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(Constants.SUPERCLASS_IMPLEMENTS_INTERFACE_ERROR)
        .in(forResource("udfs/UDFForAbstractUDFImplementingInterface.java"))
        .onLine(13)
        .atColumn(8);
  }

  @Test
  public void udfShouldNotImplementMultipleInterfaces1() throws IOException {
    assertThat(
        forResource("udfs/OverloadedUDF.java"),
        forResource("udfs/OverloadedUDF2.java"),
        forResource("udfs/UDFWithMultipleInterfaces1.java")
    ).processedWith(new TransportProcessor())
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(Constants.MULTIPLE_INTERFACES_ERROR)
        .in(forResource("udfs/UDFWithMultipleInterfaces1.java"))
        .onLine(14)
        .atColumn(8);
  }

  @Test
  public void udfShouldNotImplementMultipleInterfaces2() throws IOException {
    assertThat(
        forResource("udfs/OverloadedUDF.java"),
        forResource("udfs/AbstractUDFImplementingInterface.java"),
        forResource("udfs/UDFWithMultipleInterfaces2.java")
    ).processedWith(new TransportProcessor())
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(Constants.SUPERCLASS_IMPLEMENTS_INTERFACE_ERROR)
        .in(forResource("udfs/UDFWithMultipleInterfaces2.java"))
        .onLine(14)
        .atColumn(8);
  }

  @Test
  public void udfShouldImplementTopLevelStdUDF() throws IOException {
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
  public void udfShouldNotOverrideInterfaceMethods() throws IOException {
    assertThat(
        forResource("udfs/OverloadedUDF.java"),
        forResource("udfs/UDFOverridingInterfaceMethod.java")
    ).processedWith(new TransportProcessor())
        .failsToCompile()
        .withErrorCount(1)
        .withErrorContaining(Constants.CLASS_SHOULD_NOT_OVERRIDE_INTERFACE_METHODS_ERROR)
        .in(forResource("udfs/UDFOverridingInterfaceMethod.java"))
        .onLine(14)
        .atColumn(8);
  }

  @Test
  public void abstractUDFShouldNotBeProcessed() throws IOException {
    assertThat(
        forResource("udfs/AbstractUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/empty.json")));
  }

  @Test
  public void classNotExtendingStdUDFShouldNotBeProcessed() throws IOException {
    assertThat(
        forResource("udfs/DoesNotExtendStdUDF.java")
    ).processedWith(new TransportProcessor())
        .compilesWithoutError()
        .and()
        .generatesFileNamed(StandardLocation.CLASS_OUTPUT, "", Constants.UDF_RESOURCE_FILE_PATH)
        .withStringContents(Charset.defaultCharset(), asString(forResource("outputs/empty.json")));
  }

  @Test
  public void innerClassUDFShouldNotBeProcessed() throws IOException {
    assertThat(
        forResource("udfs/OuterClassForInnerUDF.java")
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
