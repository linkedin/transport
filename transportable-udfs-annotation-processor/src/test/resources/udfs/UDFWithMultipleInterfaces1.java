package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.udf.StdUDF0;
import java.util.List;


public class UDFWithMultipleInterfaces1 extends StdUDF0<StdBoolean> implements UDFInterface1, UDFInterface2 {

  @Override
  public String getFunctionName() {
    return "";
  }

  @Override
  public String getFunctionDescription() {
    return "";
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of();
  }

  @Override
  public String getOutputParameterSignature() {
    return "boolean";
  }

  @Override
  public StdBoolean eval() {
    return null;
  }
}