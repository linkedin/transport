package udfs;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public interface UDFInterface1 extends TopLevelStdUDF {

  @Override
  default String getFunctionName() {
    return "";
  }

  @Override
  default String getFunctionDescription() {
    return "";
  }
}