/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive.udf;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.hive.HiveUDF;
import java.util.List;


public class MapFromEntriesWrapper extends HiveUDF {

  @Override
  protected List<? extends UDF> getUdfImplementations() {
    return ImmutableList.of(new MapFromEntries());
  }

  @Override
  protected Class<? extends TopLevelUDF> getTopLevelUdfClass() {
    return MapFromEntries.class;
  }
}
