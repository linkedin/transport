/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.udf;

/**
 * An interface to define the top-level {@link UDF} properties.
 *
 * Top-level {@link UDF} properties include name and description which can be shared amongst all overloadings of the
 * UDF, where the overloadings share the name of the UDF but have different number of arguments or argument types. For a
 * UDF which does not require overloading, this interface should be implemented by the sole base UDF class extending
 * {@link UDF}. For a UDF that requires overloading, this interface should be extended by an interface which provides
 * the common name and description. The interface should then be implemented by all UDF classes extending UDF(i) that
 * share the same name (i.e., classes implementing overloaded UDFs).
 */
public interface TopLevelUDF {

  /** Returns the name of the {@link UDF}. */
  String getFunctionName();

  /** Returns the description of the {@link UDF}. */
  String getFunctionDescription();
}
