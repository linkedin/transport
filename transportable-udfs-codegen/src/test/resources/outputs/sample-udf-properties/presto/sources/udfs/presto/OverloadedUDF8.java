package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF8 extends StdUdfWrapper {
  public OverloadedUDF8() {
    super(new udfs.OverloadedUDF8());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF8();
  }
}
