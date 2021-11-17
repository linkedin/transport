package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.StdUdfWrapper;

public class SimpleUDF extends StdUdfWrapper {
  public SimpleUDF() {
    super(new udfs.SimpleUDF());
  }

  @Override
  protected UDF getStdUDF() {
    return new udfs.SimpleUDF();
  }
}
