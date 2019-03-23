package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF9 extends StdUdfWrapper {
  public OverloadedUDF9() {
    super(new udfs.OverloadedUDF9());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF9();
  }
}
