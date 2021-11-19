/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test;

import com.google.common.base.Preconditions;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.test.spi.FunctionCall;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.Tester;
import com.linkedin.transport.test.spi.TesterService;
import com.linkedin.transport.test.spi.TestCase;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * An abstract class to be extended by all test classes. This class contains helper methods to initialize the
 * {@link Tester} and create input and output data for the test cases.
 *
 * Primitive data is represented by primitive types when passed to the test cases.
 * The mapping between container types to the corresponding Java type is given below:
 * <ul>
 *   <li>{@link ArrayData} = Use {@link #array(Object...)} to create arrays</li>
 *   <li>{@link MapData} = Use {@link #map(Object...)} to create maps</li>
 *   <li>{@link RowData} = Use {@link #row(Object...)} to create structs</li>
 * </ul>
 *
 *
 */
public abstract class AbstractUDFTest {

  /**
   * Returns a {@link Tester} for the underlying test platform
   */
  protected Tester getTester() {
    Tester tester = TesterService.getTester();
    validateTopLevelUDFClassesAndImplementations(getTopLevelUDFClassesAndImplementations());
    tester.setup(getTopLevelUDFClassesAndImplementations());
    return tester;
  }

  /**
   * Returns a {@link Map} of {@link TopLevelUDF} classes to their corresponding {@link UDF} implementation
   * classes which are to be used in the test
   *
   * TODO: Auto-derive UDF implementation classes from the TopLevelUDF class
   */
  protected abstract Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations();

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
      filePath = new URI("file", AbstractUDFTest.class.getClassLoader().getResource(relativeResourcePath).getPath(),
          null).toString();
    } catch (NullPointerException | URISyntaxException e) {
      throw new IllegalArgumentException("Error fetching resource file: " + relativeResourcePath, e);
    }
    return filePath;
  }

  private void validateTopLevelUDFClassesAndImplementations(
      Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> topLevelUDFClassesAndImplementations) {
    topLevelUDFClassesAndImplementations.forEach((topLevelUDFClass, udfImplementationClasses) -> {
      Preconditions.checkNotNull(topLevelUDFClass, "TopLevelUDF class cannot be null");
      Preconditions.checkArgument(udfImplementationClasses.size() > 0,
          "At least one UDF implementation class required for TopLevelUDF " + topLevelUDFClass);
    });
  }
}
