package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF6 extends StdUdfWrapper {
  public OverloadedUDF6() {
    super(new udfs.OverloadedUDF6());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF6();
  }
}
