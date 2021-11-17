/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.test.generic.typesystem.GenericBoundVariables;
import com.linkedin.transport.test.generic.typesystem.GenericTypeFactory;
import com.linkedin.transport.test.spi.StdTester;
import com.linkedin.transport.test.spi.TestCase;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.typesystem.TypeSignature;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;


public class GenericTester implements StdTester {

  private GenericBoundVariables _boundVariables;
  private GenericTypeFactory _typeFactory;
  private GenericQueryExecutor _executor;

  @Override
  public void setup(
      Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> topLevelStdUDFClassesAndImplementations) {
    _boundVariables = new GenericBoundVariables();
    _typeFactory = new GenericTypeFactory();
    Map<String, GenericStdUDFWrapper> functionNameToWrapperMap = new HashMap<>();
    topLevelStdUDFClassesAndImplementations.forEach((topLevelStdUDF, stdUDFImplementations) -> {
      GenericStdUDFWrapper wrapper = new GenericStdUDFWrapper(topLevelStdUDF, stdUDFImplementations);
      try {
        String functionName =
            ((TopLevelUDF) stdUDFImplementations.get(0).getConstructor().newInstance()).getFunctionName();
        functionNameToWrapperMap.put(functionName, wrapper);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e) {
        throw new RuntimeException("Error registering UDF " + topLevelStdUDF.getName(), e);
      }
    });
    _executor = new GenericQueryExecutor(functionNameToWrapperMap);
  }

  @Override
  public void check(TestCase testCase) {
    Pair<TestType, Object> result = _executor.executeQuery(testCase.getFunctionCall());
    Assert.assertEquals(result.getLeft(),
        _typeFactory.createType(TypeSignature.parse(testCase.getExpectedOutputType()), _boundVariables));
    if (testCase.getExpectedOutput() instanceof ByteBuffer) {
      byte[] expected = ((ByteBuffer) testCase.getExpectedOutput()).array();
      byte[] actual = ((ByteBuffer) result.getRight()).array();
      Assert.assertEquals(actual, expected);
    } else {
      Assert.assertEquals(result.getRight(), testCase.getExpectedOutput());
    }
  }
}
