package udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class SimpleUDF extends StdUDF0<StdString> implements TopLevelStdUDF {

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
    return "varchar";
  }

  @Override
  public StdString eval() {
    return null;
  }
}