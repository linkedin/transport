package udfs;

import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.TopLevelStdUDF;


public abstract class AbstractUDF extends StdUDF0<StdString> implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "";
  }

  @Override
  public String getFunctionDescription() {
    return "";
  }
}