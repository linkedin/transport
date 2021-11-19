/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.hive.HiveUDF;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;


/**
 * A {@link HiveUDF} whose constructor takes enclosing {@link UDF} classes as parameters
 *
 * The wrapper's constructor here is parameterized so that the same wrapper can be used for all UDFs throughout the
 * test framework rather than generating UDF specific wrappers
 */
public class HiveTestUDF extends HiveUDF {

  private Class<? extends TopLevelUDF> _topLevelUDFClass;
  private List<Class<? extends UDF>> _transportUDFClasses;

  // This constructor is needed as Hive calls the parameterless constructor using Reflection when cloning the UDF
  public HiveTestUDF() {
  }

  public HiveTestUDF(Class<? extends TopLevelUDF> topLevelUDFClass,
      List<Class<? extends UDF>> transportUDFClasses) {
    _topLevelUDFClass = topLevelUDFClass;
    _transportUDFClasses = transportUDFClasses;
  }

  @Override
  protected List<? extends UDF> getUdfImplementations() {
    return _transportUDFClasses.stream().map(HiveTestUDF::createInstance).collect(Collectors.toList());
  }

  @Override
  protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
    return _topLevelUDFClass;
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    HiveTestUDF newWrapper = (HiveTestUDF) newInstance;
    newWrapper._transportUDFClasses = _transportUDFClasses;
    newWrapper._topLevelUDFClass = _topLevelUDFClass;
  }

  private static <K extends UDF> K createInstance(Class<K> udfClass) {
    try {
      return udfClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
