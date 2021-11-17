package udfs.hive;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.hive.HiveUDF;
import java.lang.Class;
import java.lang.Override;
import java.util.List;

public class SimpleUDF extends HiveUDF {
  @Override
  protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
    return udfs.SimpleUDF.class;
  }

  @Override
  protected List<? extends UDF> getUdfImplementations() {
    return ImmutableList.of(new udfs.SimpleUDF());
  }
}
