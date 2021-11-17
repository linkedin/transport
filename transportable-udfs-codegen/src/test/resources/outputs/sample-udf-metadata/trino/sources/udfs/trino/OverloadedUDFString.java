package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.StdUdfWrapper;

public class OverloadedUDFString extends StdUdfWrapper {
  public OverloadedUDFString() {
    super(new udfs.OverloadedUDFString());
  }

  @Override
  protected UDF getStdUDF() {
    return new udfs.OverloadedUDFString();
  }
}
