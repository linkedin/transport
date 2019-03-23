package udfs.hive;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.hive.StdUdfWrapper;
import java.lang.Class;
import java.lang.Override;
import java.util.List;
import udfs.OverloadedUDF1;
import udfs.OverloadedUDF10;
import udfs.OverloadedUDF2;
import udfs.OverloadedUDF3;
import udfs.OverloadedUDF4;
import udfs.OverloadedUDF5;
import udfs.OverloadedUDF6;
import udfs.OverloadedUDF7;
import udfs.OverloadedUDF8;
import udfs.OverloadedUDF9;

public class OverloadedUDF extends StdUdfWrapper {
  @Override
  protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
    return udfs.OverloadedUDF.class;
  }

  @Override
  protected List<? extends StdUDF> getStdUdfImplementations() {
    ImmutableList.Builder<StdUDF> builder = ImmutableList.builder();
    builder.add(new OverloadedUDF1());
    builder.add(new OverloadedUDF2());
    builder.add(new OverloadedUDF3());
    builder.add(new OverloadedUDF4());
    builder.add(new OverloadedUDF5());
    builder.add(new OverloadedUDF6());
    builder.add(new OverloadedUDF7());
    builder.add(new OverloadedUDF8());
    builder.add(new OverloadedUDF9());
    builder.add(new OverloadedUDF10());
    return builder.build();
  }
}
