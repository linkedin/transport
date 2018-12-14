/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test;

import com.google.common.base.Preconditions;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.spi.FunctionCall;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.StdTester;
import com.linkedin.transport.test.spi.StdTesterService;
import com.linkedin.transport.test.spi.TestCase;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * An abstract class to be extended by all test classes. This class contains helper methods to initialize the
 * {@link StdTester} and create input and output data for the test cases.
 *
 * The mapping between a {@link StdData} to the corresponding Java type is given below:
 * <ul>
 *   <li>{@link com.linkedin.transport.api.data.StdInteger} = {@link Integer}</li>
 *   <li>{@link com.linkedin.transport.api.data.StdLong} = {@link Long}</li>
 *   <li>{@link com.linkedin.transport.api.data.StdBoolean} = {@link Boolean}</li>
 *   <li>{@link com.linkedin.transport.api.data.StdString} = {@link String}</li>
 *   <li>{@link com.linkedin.transport.api.data.StdArray} = Use {@link #array(Object...)} to create arrays</li>
 *   <li>{@link com.linkedin.transport.api.data.StdMap} = Use {@link #map(Object...)} to create maps</li>
 *   <li>{@link com.linkedin.transport.api.data.StdStruct} = Use {@link #row(Object...)} to create structs</li>
 * </ul>
 *
 *
 */
public abstract class AbstractStdUDFTest {

  /**
   * Returns a {@link StdTester} for the underlying test platform
   */
  protected StdTester getTester() {
    StdTester tester = StdTesterService.getTester();
    validateTopLevelStdUDFClassesAndImplementations(getTopLevelStdUDFClassesAndImplementations());
    tester.setup(getTopLevelStdUDFClassesAndImplementations());
    return tester;
  }

  /**
   * Returns a {@link Map} of {@link TopLevelStdUDF} classes to their corresponding {@link StdUDF} implementation
   * classes which are to be used in the test
   *
   * TODO: Auto-derive StdUDF implementation classes from the TopLevelStdUDF class
   */
  protected abstract Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations();

  /**
   * Creates a {@link FunctionCall} for a {@link TestCase} from the provided function name and parameters
   */
  protected static FunctionCall functionCall(String functionName, Object... parameters) {
    return new FunctionCall(functionName, Arrays.asList(parameters));
  }

  /**
   * Creates an array from the provided elements to pass to the test framework
   */
  protected static List<Object> array(Object... elements) {
    return Arrays.asList(elements);
  }

  /**
   * Creates a map from the provided keys and values to pass to the test framework
   *
   * The keys and values of the map should be provided in an interleaved manner
   */
  protected static Map<Object, Object> map(Object... args) {
    Preconditions.checkArgument(args.length % 2 == 0,
        "Total number of keys + values is expected to be an even number. Received: " + args.length);
    Map<Object, Object> dataMap = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      dataMap.put(args[i], args[i + 1]);
    }
    return dataMap;
  }

  /**
   * Creates a row from the provided elements to pass to the test framework
   */
  protected static Row row(Object... args) {
    return new Row(Arrays.asList(args));
  }

  /**
   * Creates a string containing the absolute path from the provided relative path of the resource to pass to the
   * test framework
   */
  protected static String resource(String relativeResourcePath) {
    String filePath = null;
    try {
      filePath = new URI("file", AbstractStdUDFTest.class.getClassLoader().getResource(relativeResourcePath).getPath(),
          null).toString();
    } catch (NullPointerException | URISyntaxException e) {
      throw new IllegalArgumentException("Error fetching resource file: " + relativeResourcePath, e);
    }
    return filePath;
  }

  private void validateTopLevelStdUDFClassesAndImplementations(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {
    topLevelStdUDFClassesAndImplementations.forEach((topLevelStdUDFClass, stdUDFImplementationClasses) -> {
      Preconditions.checkNotNull(topLevelStdUDFClass, "TopLevelStdUDF class cannot be null");
      Preconditions.checkArgument(stdUDFImplementationClasses.size() > 0,
          "At least one StdUDF implementation class required for TopLevelStdUDF " + topLevelStdUDFClass);
    });
  }
}
