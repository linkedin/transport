/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.udf.UDF4;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class StructCreateByNameFunction extends UDF4<String, Object, String, Object, RowData> implements TopLevelUDF {

  private DataType _field1Type;
  private DataType _field2Type;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "varchar",
        "K",
        "varchar",
        "V"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "row(K,V)";
  }

  @Override
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    _field1Type = getTypeFactory().createDataType("K");
    _field2Type = getTypeFactory().createDataType("V");
  }

  @Override
  public RowData eval(String field1Name, Object field1Value, String field2Name, Object field2Value) {
    RowData struct = getTypeFactory().createStruct(
        ImmutableList.of(field1Name, field2Name),
        ImmutableList.of(_field1Type, _field2Type)
    );
    struct.setField(field1Name, field1Value);
    struct.setField(field2Name, field2Value);
    return struct;
  }

  @Override
  public String getFunctionName() {
    return "struct_create_by_name";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a pairwise struct from two fields with their names";
  }
}
