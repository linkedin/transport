/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.udf;


/**
 * A Standard UDF with four input arguments.
 *
 * @param <I1>  the type of the first input argument
 * @param <I2>  the type of the second input argument
 * @param <I3>  the type of the third input argument
 * @param <I4>  the type of the fourth input argument
 * @param <O>  the type of the return value of the {@link StdUDF}
 */
// Suppressing class parameter type parameter name and arg naming style checks since this naming convention is more
// suitable to Standard UDFs, and the code is more readable this way.
@SuppressWarnings({"checkstyle:classtypeparametername", "checkstyle:regexpsinglelinejava"})
public abstract class StdUDF4<I1, I2, I3, I4, O>
    extends StdUDF {

  /**
   * Returns the output of the {@link StdUDF} given the input arguments.
   *
   * This method is called once per input record. All UDF logic should be defined in this method.
   *
   * @param arg1  the first input argument
   * @param arg2  the second input argument
   * @param arg3  the third input argument
   * @param arg4  the fourth input argument
   * @return the output of the {@link StdUDF} given the input arguments.
   */
  public abstract O eval(I1 arg1, I2 arg2, I3 arg3, I4 arg4);

  /**
   * Returns an array of file paths to be localized at the worker nodes.
   *
   * The Standard UDF framework localizes the files passed through this method and provides the localized file paths to
   * {@link StdUDF#processRequiredFiles(String[])} for further processing. Users can use the pattern "#LATEST" instead
   * of a concrete directory name in the path as a way of selecting the directory with the most recent timestamp, and
   * hence obtaining the most recent version of a file.
   * Example: 'hdfs:///data/derived/dwh/prop/testMemberId/#LATEST/testMemberId.txt'
   *
   * The arguments passed to {@link #eval(Object,Object,Object,Object)} are passed to this method as well to
   * allow users to construct required file paths from arguments passed to the UDF. Since this method is called before
   * any rows are processed, only constant UDF arguments should be used to construct the file paths. Values of
   * non-constant arguments are not deterministic, and are null for most platforms. (Constant arguments are arguments
   * whose literal values are given to the UDF as opposed to non-constant arguments that are expressions which depend on
   * columns. For example, in the query {@code SELECT my_udf('my_value', T.Col1) FROM T}, {@literal my_value} is a
   * constant argument to the UDF and its value is the same for all invocations of this UDF, while {@code T.Col1} is a
   * non-constant argument since it is an expression that depends on a table column, and hence its value changes on a
   * per-row basis).
   *
   * @param arg1  the first input argument if the argument is constant, null otherwise
   * @param arg2  the second input argument if the argument is constant, null otherwise
   * @param arg3  the third input argument if the argument is constant, null otherwise
   * @param arg4  the fourth input argument if the argument is constant, null otherwise
   * @return an array of file paths to be localized at the worker nodes.
   */
  public String[] getRequiredFiles(I1 arg1, I2 arg2, I3 arg3, I4 arg4) {
    return new String[]{};
  }

  protected final int numberOfArguments() {
    return 4;
  }
}
