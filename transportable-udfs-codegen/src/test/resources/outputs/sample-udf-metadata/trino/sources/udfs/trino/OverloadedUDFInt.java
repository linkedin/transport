package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.StdUdfWrapper;

public class OverloadedUDFInt extends StdUdfWrapper {
  public OverloadedUDFInt() {
    super(new udfs.OverloadedUDFInt());
  }

  @Override
  protected UDF getStdUDF() {
    return new udfs.OverloadedUDFInt();
  }
}
