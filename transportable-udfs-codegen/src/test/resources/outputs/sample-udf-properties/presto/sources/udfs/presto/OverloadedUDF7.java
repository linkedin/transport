package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF7 extends StdUdfWrapper {
  public OverloadedUDF7() {
    super(new udfs.OverloadedUDF7());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF7();
  }
}
