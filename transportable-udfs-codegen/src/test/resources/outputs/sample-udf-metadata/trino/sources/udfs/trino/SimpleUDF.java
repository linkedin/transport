package udfs.trino;

import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.trino.StdUdfWrapper;

public class SimpleUDF extends StdUdfWrapper {
  public SimpleUDF() {
    super(new udfs.SimpleUDF());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new udfs.SimpleUDF();
  }
}
