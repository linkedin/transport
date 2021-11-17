package udfs.trino;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.trino.TrinoUDF;

public class SimpleUDF extends TrinoUDF {
  public SimpleUDF() {
    super(new udfs.SimpleUDF());
  }

  @Override
  protected UDF getUDF() {
    return new udfs.SimpleUDF();
  }
}
