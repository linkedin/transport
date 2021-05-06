/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.trino.StdUdfWrapper;
import java.lang.reflect.InvocationTargetException;


/**
 * A {@link StdUdfWrapper} whose constructor takes enclosing {@link StdUDF} classes as parameters
 *
 * The wrapper's constructor here is parameterized so that the same wrapper can be used for all UDFs throughout the
 * test framework rather than generating UDF specific wrappers
 */
public class TrinoTestStdUDFWrapper extends StdUdfWrapper {

  private final Class<? extends StdUDF> _udfClass;

  public TrinoTestStdUDFWrapper(Class<? extends StdUDF> udfClass) {
    super(createInstance(udfClass));
    _udfClass = udfClass;
  }

  @Override
  protected StdUDF getStdUDF() {
    return createInstance(_udfClass);
  }

  private static <K extends StdUDF> K createInstance(Class<K> udfClass) {
    try {
      return udfClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
