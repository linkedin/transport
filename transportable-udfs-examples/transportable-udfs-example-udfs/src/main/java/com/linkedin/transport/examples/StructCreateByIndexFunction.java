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
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class StructCreateByIndexFunction extends UDF2<Object, Object, RowData> implements TopLevelUDF {

  private DataType _field1Type;
  private DataType _field2Type;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "K",
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
  public RowData eval(Object field1Value, Object field2Value) {
    RowData row = getTypeFactory().createRowData(ImmutableList.of(_field1Type, _field2Type));
    row.setField(0, field1Value);
    row.setField(1, field2Value);
    return row;
  }

  @Override
  public String getFunctionName() {
    return "struct_create_by_index";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a pairwise struct from two fields with their names";
  }
}
