/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.processor;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


/**
 * Maintains all metadata of Transport UDFs to be included in the UDF properties file
 */
public class UDFProperties {
  private Multimap<String, String> _udfs;

  public UDFProperties() {
    _udfs = HashMultimap.create();
  }

  void addUDF(String topLevelStdUDFClassName, String udfClassName) {
    _udfs.put(topLevelStdUDFClassName, udfClassName);
  }

  public void toJson(Writer writer) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    gson.toJson(UDFPropertiesFile.fromUDFProperties(this), writer);
  }

  /**
   * Represents the JSON object structure of the UDF properties resource file
   */
  private static class UDFPropertiesFile {
    private List<UDF> udfs;

    private static class UDF {
      String topLevelStdUDFClass;
      Collection<String> implementations;
    }

    private static UDFPropertiesFile fromUDFProperties(UDFProperties properties) {
      UDFPropertiesFile file = new UDFPropertiesFile();
      file.udfs = new LinkedList<>();
      for (String topLevelStdUdfClass : properties._udfs.keySet()) {
        UDF udf = new UDF();
        udf.topLevelStdUDFClass = topLevelStdUdfClass;
        udf.implementations = properties._udfs.get(topLevelStdUdfClass);
        file.udfs.add(udf);
      }
      return file;
    }
  }
}
