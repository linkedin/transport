package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF2 extends StdUdfWrapper {
  public OverloadedUDF2() {
    super(new udfs.OverloadedUDF2());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF2();
  }
}
