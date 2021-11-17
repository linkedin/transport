package udfs.hive;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.hive.StdUdfWrapper;
import java.lang.Class;
import java.lang.Override;
import java.util.List;

public class OverloadedUDF extends StdUdfWrapper {
  @Override
  protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
    return udfs.OverloadedUDF.class;
  }

  @Override
  protected List<? extends UDF> getStdUdfImplementations() {
    return ImmutableList.of(new udfs.OverloadedUDFInt(), new udfs.OverloadedUDFString());
  }
}
