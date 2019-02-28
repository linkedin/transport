package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF0;
import java.util.List;


public class UDFWithMultipleInterfaces2 extends AbstractUDF implements UDFInterface1 {

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
  public StdString eval() {
    return null;
  }
}