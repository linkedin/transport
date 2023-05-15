package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF3;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MapLookup extends StdUDF3<StdMap, StdString, StdString, StdString> implements TopLevelStdUDF {

  private static String getFirstNonemptyMatchedGroup(Matcher m) {
    for (int groupIndex = 1; groupIndex <= m.groupCount(); groupIndex++) {
      String match = m.group(groupIndex);
      if (match != null && !match.isEmpty()) {
        return match;
      }
    }
    return null;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "map(varchar,varchar)",
        "varchar",
        "varchar"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "varchar";
  }

  @Override
  public boolean[] getNullableArguments() {
    boolean[] args = {false, true, true};
    return args;
  }

  @Override
  public StdString eval(StdMap map, StdString keys, StdString valuePattern) {
    StdString[] stdKeyList = null;
    if (keys != null) {
      String keysString = keys.get();
      if (!keysString.trim().isEmpty()) {
        String[] keyList = keysString.split(",");
        stdKeyList = new StdString[keyList.length];
        for (int i = 0; i < keyList.length; i++) {
          stdKeyList[i] = getStdFactory().createString(keyList[i]);
        }
      }
    }

    Pattern pattern = null;
    if (valuePattern != null) {
      pattern = Pattern.compile(valuePattern.get());
    }

    return extractFirstMapValueByRegex(map, stdKeyList, pattern);
  }

  private StdString extractFirstMapValueByRegex(StdMap dataMap, StdString[] keyList, Pattern p) {
    if (keyList != null) {
      for (StdString key : keyList) {
        if (dataMap.containsKey(key)) {
          return (StdString) dataMap.get(key);
        }
      }
    }

    if (p == null) {
      return null;
    }

    for (StdData value : dataMap.values()) {
      Matcher m = p.matcher(((StdString) value).get());
      if (m.find()) {
        return getStdFactory().createString(getFirstNonemptyMatchedGroup(m));
      }
    }

    return null;
  }

  @Override
  public String getFunctionName() {
    return "map_lookup";
  }

  @Override
  public String getFunctionDescription() {
    return "For a given map and a set of keys, find the first value that matches a pattern.";
  }
}
