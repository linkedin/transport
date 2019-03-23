/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.compile;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Reader;
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
    _udfs = LinkedHashMultimap.create();
  }

  public void addUDF(String topLevelStdUDFClassName, String udfClassName) {
    _udfs.put(topLevelStdUDFClassName, udfClassName);
  }

  public Multimap<String, String> getUdfs() {
    return _udfs;
  }

  public void toJson(Writer writer) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    gson.toJson(UDFPropertiesFile.fromUDFProperties(this), writer);
  }

  public static UDFProperties fromJson(Reader reader) {
    Gson gson = new GsonBuilder().create();
    return gson.fromJson(reader, UDFPropertiesFile.class).toUDFProperties();
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

    private UDFProperties toUDFProperties() {
      UDFProperties properties = new UDFProperties();
      for (UDF udf : udfs) {
        properties._udfs.putAll(udf.topLevelStdUDFClass, udf.implementations);
      }
      return properties;
    }
  }
}
