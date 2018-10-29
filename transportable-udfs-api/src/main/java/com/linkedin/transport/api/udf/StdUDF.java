/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.udf;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import java.util.List;


/**
 * A base class for all Standard UDF implementations.
 *
 * {@link StdUDF} abstract class is a base class for more specific StdUDF abstract sub-classes that are specific to the
 * number of UDF arguments, i.e., {@link StdUDF0}, {@link StdUDF1}, {@link StdUDF2}, etc. In general, StdUDF(i) is an
 * abstract class for UDFs expecting {@code i} arguments. Similar to lambda expressions, StdUDF(i) abstract classes are
 * type-parameterized by the input types and output type of the eval function. Each class is type-parameterized by
 * {@code (i+1)} type parameters; {@code i} type parameters for the UDF input types, and one type parameter for the
 * output type. All types (both input and output types) must extend the {@link StdData}
 * interface.
 */
public abstract class StdUDF {
  private StdFactory _stdFactory;

  /** Returns a {@link List} of type signature strings representing the input parameters to the UDF*/
  public abstract List<String> getInputParameterSignatures();

  /** Returns a type signature string representing the output parameter to the UDF */
  public abstract String getOutputParameterSignature();

  /**
   * Performs necessary initializations for a {@link StdUDF}.
   *
   * This method is called before any records are processed by the UDF. All overriding implementations <b>MUST</b> call
   * {@code super.init(stdFactory)} at the beginning of this method to ensure the {@link StdFactory} object is set.
   * Also any {@link StdUDF} instantiating another {@link StdUDF} inside it <b>MUST</b> call {@link #init(StdFactory)}
   * of contained UDF.
   *
   * @param stdFactory  a {@link StdFactory} object which can be used to create
   * {@link StdData} and {@link StdType} objects
   */
  public void init(StdFactory stdFactory) {
    _stdFactory = stdFactory;
  }

  /**
   * Processes the localized files for the {@link StdUDF}.
   *
   * This method is called before any records are processed. The Standard UDF framework localizes the files passed
   * through {@code getRequiredFiles()} and provides the localized file paths for further processing such as building of
   * lookup tables.
   *
   * @param localFiles  an array of localized file paths for the files specified in {@code getRequiredFiles()}
   */
  public void processRequiredFiles(String[] localFiles) {
  }

  /**
   * Returns an array of booleans indicating if any input argument is nullable.
   *
   * Nullable arguments are arguments that can receive a null value. For a nullable argument, the user must explicitly
   * handle null values in their implementation. For a non-nullable argument, the UDF returns null if the argument
   * is null. The length of the returned array should be equal to the number of input arguments. Defaults to all
   * arguments being non-nullable.
   */
  public boolean[] getNullableArguments() {
    return new boolean[numberOfArguments()];
  }

  /** Returns an array of booleans indicating if any input argument is nullable and also verifies its length */
  public final boolean[] getAndCheckNullableArguments() {
    boolean[] nullableArguments = getNullableArguments();
    if (nullableArguments.length != numberOfArguments()) {
      throw new RuntimeException(
          "Unexpected number of nullable arguments. Expected:" + numberOfArguments() + " Received:"
              + nullableArguments.length);
    }
    return nullableArguments;
  }

  /** Returns the number of input arguments for the {@link StdUDF} */
  protected abstract int numberOfArguments();

  /**
   * Returns a {@link StdFactory} object which can be used to create {@link StdData} and
   * {@link StdType} objects
   */
  public StdFactory getStdFactory() {
    return _stdFactory;
  }
}
