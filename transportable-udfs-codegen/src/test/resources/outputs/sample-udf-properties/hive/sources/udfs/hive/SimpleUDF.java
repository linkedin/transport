package udfs.hive;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.hive.StdUdfWrapper;
import java.lang.Class;
import java.lang.Override;
import java.util.List;

public class SimpleUDF extends StdUdfWrapper {
  @Override
  protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
    return udfs.SimpleUDF.class;
  }

  @Override
  protected List<? extends StdUDF> getStdUdfImplementations() {
    ImmutableList.Builder<StdUDF> builder = ImmutableList.builder();
    builder.add(new udfs.SimpleUDF());
    return builder.build();
  }
}
