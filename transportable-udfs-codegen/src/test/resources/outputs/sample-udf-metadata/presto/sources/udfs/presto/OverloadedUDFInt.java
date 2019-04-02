package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDFInt extends StdUdfWrapper {
  public OverloadedUDFInt() {
    super(new udfs.OverloadedUDFInt());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDFInt();
  }
}
