package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF1 extends StdUdfWrapper {
  public OverloadedUDF1() {
    super(new udfs.OverloadedUDF1());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF1();
  }
}
