package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF10 extends StdUdfWrapper {
  public OverloadedUDF10() {
    super(new udfs.OverloadedUDF10());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF10();
  }
}
