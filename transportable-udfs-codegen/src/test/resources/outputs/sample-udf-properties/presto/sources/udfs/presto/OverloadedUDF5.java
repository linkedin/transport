package udfs.presto;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.presto.StdUdfWrapper;
import java.lang.Override;

public class OverloadedUDF5 extends StdUdfWrapper {
  public OverloadedUDF5() {
    super(new udfs.OverloadedUDF5());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.OverloadedUDF5();
  }
}
