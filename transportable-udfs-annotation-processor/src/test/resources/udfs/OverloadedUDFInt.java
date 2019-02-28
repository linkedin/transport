package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.udf.StdUDF0;
import java.util.List;


public class OverloadedUDFInt extends StdUDF0<StdInteger> implements UDFInterface1 {

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of();
  }

  @Override
  public String getOutputParameterSignature() {
    return "integer";
  }

  @Override
  public StdInteger eval() {
    return null;
  }
}