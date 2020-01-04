/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api;

import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdArrayType;
import com.linkedin.transport.api.types.StdMapType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF;
import java.io.Serializable;
import java.util.List;


/**
 * {@link StdFactory} is used to create containter types (e.g., {@link ArrayData}, {@link MapData}, {@link RowData})
 * and {@link StdType} objects inside Standard UDFs.
 *
 * Specific APIs of {@link StdFactory} are implemented by each target platform (e.g., Spark, Presto, Hive) individually.
 * A {@link StdFactory} object is available inside Standard UDFs using {@link StdUDF#getStdFactory()}.
 * The Standard UDF framework is responsible for providing the correct platform specific implementation at runtime.
 */
public interface StdFactory extends Serializable {

  /**
   * Creates an empty {@link ArrayData} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link StdArrayType}.
   *
   * @param stdType  type of the array to be created
   * @param expectedSize  expected number of entries in the array
   * @return an empty {@link ArrayData}
   */
  ArrayData createArray(StdType stdType, int expectedSize);

  /**
   * Creates an empty {@link ArrayData} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link StdArrayType}.
   *
   * @param stdType  type of the array to be created
   * @return an empty {@link ArrayData}
   */
  ArrayData createArray(StdType stdType);

  /**
   * Creates an empty {@link MapData} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link StdMapType}.
   *
   * @param stdType  type of the map to be created
   * @return an empty {@link MapData}
   */
  MapData createMap(StdType stdType);

  /**
   * Creates a {@link RowData} with the given field names and types.
   *
   * @param fieldNames  names of the struct fields
   * @param fieldTypes  types of the struct fields
   * @return a {@link RowData} with all fields initialized to null
   */
  RowData createStruct(List<String> fieldNames, List<StdType> fieldTypes);

  /**
   * Creates a {@link RowData} with the given field types. Field names will be field0, field1, field2...
   *
   * @param fieldTypes  types of the struct fields
   * @return a {@link RowData} with all fields initialized to null
   */
  RowData createStruct(List<StdType> fieldTypes);

  /**
   * Creates a {@link RowData} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link com.linkedin.transport.api.types.RowType}.
   *
   * @param stdType  type of the struct to be created
   * @return a {@link RowData} with all fields initialized to null
   */
  RowData createStruct(StdType stdType);

  /**
   * Creates a {@link StdType} representing the given type signature.
   *
   * The following are considered valid type signatures:
   * <ul>
   *   <li>{@code "varchar"} - Represents SQL varchar type. Corresponding standard type is {@link String}</li>
   *   <li>{@code "integer"} - Represents SQL int type. Corresponding standard type is {@link Integer}</li>
   *   <li>{@code "bigint"} - Represents SQL bigint/long type. Corresponding standard type is {@link Long}</li>
   *   <li>{@code "boolean"} - Represents SQL boolean type. Corresponding standard type is {@link Boolean}</li>
   *   <li>{@code "array(T)"} - Represents SQL array type, where {@code T} is type signature of array element.
   *     Corresponding standard type is {@link ArrayData}</li>
   *   <li>{@code "map(K,V)"} - Represents SQL map type, where {@code K} and {@code V} are type signatures of the map
   *     keys and values respectively. array element. Corresponding standard type is {@link MapData}</li>
   *   <li>{@code "row(f0 T0, f1 T1,... fn Tn)"} - Represents SQL struct type, where {@code f0}...{@code fn} are field
   *     names and {@code T0}...{@code Tn} are type signatures for the fields. Field names are optional; if not
   *     specified they default to {@code field0}...{@code fieldn}. Corresponding standard type is {@link RowData}</li>
   * </ul>
   *
   * Generic type parameters can also be used as part of the type signatures; e.g., The type signature {@code "map(K,V)"}
   * is valid even without explicitly specifying what {@code K} and {@code V} are, given that {@code K} and {@code V} are
   * derivable from the input parameter signatures of a {@link StdUDF}.
   *
   * Type signatures can also be nested. Here are some more examples of valid type signatures:
   * <ul>
   *   <li>{@code "map(varchar, array(integer))"} - Represents a map from a string to an array of integers</li>
   *   <li>{@code "row(K, array(K), boolean)"} - Represents a struct with 3 fields. Here the first field of the struct
   *     and the element inside the array are of the same type</li>
   *   <li>{@code "array(row(varchar, map(integer, map(integer, varchar))))"} - Represents an array of structs containing
   *     a string and a two-level nested map with integer keys and a string value</li>
   * </ul>
   * @param typeSignature  the type signature string
   * @return a {@link StdType} for the given type signature
   */
  StdType createStdType(String typeSignature);
}
