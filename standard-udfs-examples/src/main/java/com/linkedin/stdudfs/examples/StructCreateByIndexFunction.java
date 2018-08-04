package com.linkedin.stdudfs.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.api.udf.StdUDF2;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
import java.util.List;


public class StructCreateByIndexFunction extends StdUDF2<StdData, StdData, StdStruct> implements TopLevelStdUDF {

  private StdType _field1Type;
  private StdType _field2Type;

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
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _field1Type = getStdFactory().createStdType("K");
    _field2Type = getStdFactory().createStdType("V");
  }

  @Override
  public StdStruct eval(StdData field1Value, StdData field2Value) {
    StdStruct struct = getStdFactory().createStruct(ImmutableList.of(_field1Type, _field2Type));
    struct.setField(0, field1Value);
    struct.setField(1, field2Value);
    return struct;
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
