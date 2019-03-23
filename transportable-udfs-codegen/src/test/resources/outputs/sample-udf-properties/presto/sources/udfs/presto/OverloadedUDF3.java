package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF3 extends StdUdfWrapper {
  public OverloadedUDF3() {
    super(new udfs.OverloadedUDF3());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF3();
  }
}
