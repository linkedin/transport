/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF4;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class StructCreateByNameFunction extends StdUDF4<String, Object, String, Object, RowData> implements TopLevelStdUDF {

  private StdType _field1Type;
  private StdType _field2Type;

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
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _field1Type = getStdFactory().createStdType("K");
    _field2Type = getStdFactory().createStdType("V");
  }

  @Override
  public RowData eval(String field1Name, Object field1Value, String field2Name, Object field2Value) {
    RowData struct = getStdFactory().createStruct(
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
