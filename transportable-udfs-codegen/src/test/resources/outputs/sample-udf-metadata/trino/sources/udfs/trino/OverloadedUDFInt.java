package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.TrinoUDF;

public class OverloadedUDFInt extends TrinoUDF {
  public OverloadedUDFInt() {
    super(new udfs.OverloadedUDFInt());
  }

  @Override
  protected UDF getUDF() {
    return new udfs.OverloadedUDFInt();
  }
}
