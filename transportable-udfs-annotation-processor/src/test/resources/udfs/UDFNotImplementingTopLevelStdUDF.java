package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF0;
import java.util.List;


public class UDFNotImplementingTopLevelStdUDF extends StdUDF0<StdString> {

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of();
  }

  @Override
  public String getOutputParameterSignature() {
    return "varchar";
  }

  @Override
  public StdString eval() {
    return null;
  }
}