/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.hive.StdUdfWrapper;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;


/**
 * A {@link StdUdfWrapper} whose constructor takes enclosing {@link StdUDF} classes as parameters
 *
 * The wrapper's constructor here is parameterized so that the same wrapper can be used for all UDFs throughout the
 * test framework rather than generating UDF specific wrappers
 */
public class HiveTestStdUDFWrapper extends StdUdfWrapper {

  private Class<? extends TopLevelStdUDF> _topLevelStdUDFClass;
  private List<Class<? extends StdUDF>> _stdUDFClasses;

  // This constructor is needed as Hive calls the parameterless constructor using Reflection when cloning the UDF
  public HiveTestStdUDFWrapper() {
  }

  public HiveTestStdUDFWrapper(Class<? extends TopLevelStdUDF> topLevelStdUDFClass,
      List<Class<? extends StdUDF>> stdUDFClasses) {
    _topLevelStdUDFClass = topLevelStdUDFClass;
    _stdUDFClasses = stdUDFClasses;
  }

  @Override
  protected List<? extends StdUDF> getStdUdfImplementations() {
    return _stdUDFClasses.stream().map(HiveTestStdUDFWrapper::createInstance).collect(Collectors.toList());
  }

  @Override
  protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
    return _topLevelStdUDFClass;
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    HiveTestStdUDFWrapper newWrapper = (HiveTestStdUDFWrapper) newInstance;
    newWrapper._stdUDFClasses = _stdUDFClasses;
    newWrapper._topLevelStdUDFClass = _topLevelStdUDFClass;
  }

  private static <K extends StdUDF> K createInstance(Class<K> udfClass) {
    try {
      return udfClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
