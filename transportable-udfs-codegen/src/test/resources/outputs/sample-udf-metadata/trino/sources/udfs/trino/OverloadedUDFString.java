package udfs.trino;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.trino.StdUdfWrapper;

public class OverloadedUDFString extends StdUdfWrapper {
  public OverloadedUDFString() {
    super(new udfs.OverloadedUDFString());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDFString();
  }
}
