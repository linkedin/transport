package udfs;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public class NonUDF implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "";
  }

  @Override
  public String getFunctionDescription() {
    return "";
  }
}