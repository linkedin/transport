package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class DeleteFieldOfAStruct extends StdUDF2<StdData, StdString, StdData> implements TopLevelStdUDF  {

  /**
   *
   * @param input struct from which the field is to be removed
   * @param fieldName name of field to be removed. If no such field, don't do anything.
   * @return struct with field removed
   */
  @Override
  public StdData eval(StdData input, StdString fieldName) {
    if (input instanceof StdStruct) {
      StdStruct inputAsStruct = ((StdStruct) input);
      StdData field = inputAsStruct.getField(fieldName.get());
      if (field == null) {
        // no field with the matching name, don't do anything
        return input;
      } else {
        // Replaced with empty string.
        inputAsStruct.setField(fieldName.get(), getStdFactory().createString(""));
      }
    } else {
      throw new RuntimeException("Works only with a struct");
    }
    return input;
  }


  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "row(varchar, varchar)",
        "varchar"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "row(varchar, varchar)";
  }

  @Override
  public String getFunctionName() {
    return "deleteFieldOfAStruct";
  }

  @Override
  public String getFunctionDescription() {
    return "changes the content of the field called 'fieldName' to empty string";
  }

}
