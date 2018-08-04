package com.linkedin.stdudfs.api;

import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdBoolean;
import com.linkedin.stdudfs.api.data.StdInteger;
import com.linkedin.stdudfs.api.data.StdLong;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.data.StdString;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.api.types.StdType;
import java.io.Serializable;
import java.util.List;


/**
 * {@link StdFactory} is used to create {@link com.linkedin.stdudfs.api.data.StdData} and {@link StdType} objects inside Standard UDFs.
 *
 * Specific APIs of {@link StdFactory} are implemented by each target platform (e.g., Spark, Presto, Hive) individually.
 * A {@link StdFactory} object is available inside Standard UDFs using {@link com.linkedin.stdudfs.api.udf.StdUDF#getStdFactory()}.
 * The Standard UDF framework is responsible for providing the correct platform specific implementation at runtime.
 */
public interface StdFactory extends Serializable {

  /**
   * Creates a {@link StdInteger} representing a given integer value.
   *
   * @param value  the input integer value
   * @return {@link StdInteger} with the given integer value
   */
  StdInteger createInteger(int value);

  /**
   * Creates a {@link StdLong} representing a given long value.
   *
   * @param value  the input long value
   * @return {@link StdLong} with the given long value
   */
  StdLong createLong(long value);

  /**
   * Creates a {@link StdBoolean} representing a given boolean value.
   *
   * @param value  the input boolean value
   * @return {@link StdBoolean} with the given boolean value
   */
  StdBoolean createBoolean(boolean value);

  /**
   * Creates a {@link StdString} representing a given {@link String} value.
   *
   * @param value  the input {@link String} value
   * @return {@link StdString} with the given {@link String} value
   */
  StdString createString(String value);

  /**
   * Creates an empty {@link StdArray} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link com.linkedin.stdudfs.api.types.StdArrayType}.
   *
   * @param stdType  type of the array to be created
   * @param expectedSize  expected number of entries in the array
   * @return an empty {@link StdArray}
   */
  StdArray createArray(StdType stdType, int expectedSize);

  /**
   * Creates an empty {@link StdArray} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link com.linkedin.stdudfs.api.types.StdArrayType}.
   *
   * @param stdType  type of the array to be created
   * @return an empty {@link StdArray}
   */
  StdArray createArray(StdType stdType);

  /**
   * Creates an empty {@link StdMap} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link com.linkedin.stdudfs.api.types.StdMapType}.
   *
   * @param stdType  type of the map to be created
   * @return an empty {@link StdMap}
   */
  StdMap createMap(StdType stdType);

  /**
   * Creates a {@link StdStruct} with the given field names and types.
   *
   * @param fieldNames  names of the struct fields
   * @param fieldTypes  types of the struct fields
   * @return a {@link StdStruct} with all fields initialized to null
   */
  StdStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes);

  /**
   * Creates a {@link StdStruct} with the given field types. Field names will be field0, field1, field2...
   *
   * @param fieldTypes  types of the struct fields
   * @return a {@link StdStruct} with all fields initialized to null
   */
  StdStruct createStruct(List<StdType> fieldTypes);

  /**
   * Creates a {@link StdStruct} whose type is given by the given {@link StdType}.
   *
   * It is expected that the top-level {@link StdType} is a {@link com.linkedin.stdudfs.api.types.StdStructType}.
   *
   * @param stdType  type of the struct to be created
   * @return a {@link StdStruct} with all fields initialized to null
   */
  StdStruct createStruct(StdType stdType);

  /**
   * Creates a {@link StdType} representing the given type signature.
   *
   * The following are considered valid type signatures:
   * <ul>
   *   <li>{@code "varchar"} - Represents SQL varchar type. Corresponding standard type is {@link StdString}</li>
   *   <li>{@code "integer"} - Represents SQL int type. Corresponding standard type is {@link StdInteger}</li>
   *   <li>{@code "bigint"} - Represents SQL bigint/long type. Corresponding standard type is {@link StdLong}</li>
   *   <li>{@code "boolean"} - Represents SQL boolean type. Corresponding standard type is {@link StdBoolean}</li>
   *   <li>{@code "array(T)"} - Represents SQL array type, where {@code T} is type signature of array element.
   *     Corresponding standard type is {@link StdArray}</li>
   *   <li>{@code "map(K,V)"} - Represents SQL map type, where {@code K} and {@code V} are type signatures of the map
   *     keys and values respectively. array element. Corresponding standard type is {@link StdMap}</li>
   *   <li>{@code "row(f0 T0, f1 T1,... fn Tn)"} - Represents SQL struct type, where {@code f0}...{@code fn} are field
   *     names and {@code T0}...{@code Tn} are type signatures for the fields. Field names are optional; if not
   *     specified they default to {@code field0}...{@code fieldn}. Corresponding standard type is {@link StdStruct}</li>
   * </ul>
   *
   * Generic type parameters can also be used as part of the type signatures; e.g., The type signature {@code "map(K,V)"}
   * is valid even without explicitly specifying what {@code K} and {@code V} are, given that {@code K} and {@code V} are
   * derivable from the input parameter signatures of a {@link com.linkedin.stdudfs.api.udf.StdUDF}.
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
