package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF4 extends StdUdfWrapper {
  public OverloadedUDF4() {
    super(new udfs.OverloadedUDF4());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF4();
  }
}
