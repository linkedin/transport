package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.TrinoUDF;

public class OverloadedUDFString extends TrinoUDF {
  public OverloadedUDFString() {
    super(new udfs.OverloadedUDFString());
  }

  @Override
  protected UDF getUDF() {
    return new udfs.OverloadedUDFString();
  }
}
