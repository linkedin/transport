/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive.udf;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.MapType;
import com.linkedin.transport.api.udf.UDF1;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


/**
 * Hive's built-in map() UDF cannot be used to create maps with complex key types. This UDF allows you to do so.
 * This is used inside {@link com.linkedin.transport.test.hive.HiveTester} to create arbitrary map objects
 */
public class MapFromEntries extends UDF1<ArrayData, MapData> implements TopLevelUDF {

  private MapType _mapType;

  @Override
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    _mapType = (MapType) typeFactory.createDataType(getOutputParameterSignature());
  }

  @Override
  public MapData eval(ArrayData entryArray) {
    MapData result = getTypeFactory().createMap(_mapType);
    for (Object element : entryArray) {
      RowData elementStruct = (RowData) element;
      result.put(elementStruct.getField(0), elementStruct.getField(1));
    }
    return result;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("array(row(K,V))");
  }

  @Override
  public String getOutputParameterSignature() {
    return "map(K,V)";
  }

  @Override
  public String getFunctionName() {
    return "map_from_entries";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a map from and array of rows having 2 elements, a key and a value";
  }
}
