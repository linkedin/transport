package com.linkedin.stdudfs.api.data;

import java.util.List;


/** A Standard UDF data type for representing structs. */
public interface StdStruct extends StdData {

  /**
   * Returns the value of the field at the given position in the struct.
   *
   * @param index  the position of the field in the struct
   */
  StdData getField(int index);

  /**
   * Returns the value of the given field from the struct.
   *
   * @param name  the name of the field
   */
  StdData getField(String name);

  /**
   * Sets the value of the field at the given position in the struct.
   *
   * @param index  the position of the field in the struct
   * @param value  the value to be set
   */
  void setField(int index, StdData value);

  /**
   * Sets the value of the given field in the struct.
   *
   * @param name  the name of the field
   * @param value  the value to be set
   */
  void setField(String name, StdData value);

  /** Returns a {@link List} of all fields in the struct. */
  List<StdData> fields();
}
