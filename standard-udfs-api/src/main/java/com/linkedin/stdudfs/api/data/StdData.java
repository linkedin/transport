/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.api.data;

/**
 * An interface for all data types in Standard UDFs.
 *
 * {@link StdData} is the main interface through which StdUDFs receive input data and return output data. All Standard
 * UDF data types (e.g., {@link StdInteger}, {@link StdArray}, {@link StdMap}) must extend {@link StdData}. Methods
 * inside {@link com.linkedin.stdudfs.api.StdFactory} can be used to create {@link StdData} objects.
 */
public interface StdData {
}
