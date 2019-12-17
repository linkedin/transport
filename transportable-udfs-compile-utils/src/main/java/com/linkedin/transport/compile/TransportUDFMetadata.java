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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Maintains all metadata of Transport UDFs to be included in the UDF metadata file
 */
public class TransportUDFMetadata {
  private static final Gson GSON;
  private Multimap<String, String> _udfs;
  private Map<String, Integer> _classToNumberOfTypeParameters;

  static {
    GSON = new GsonBuilder().setPrettyPrinting().create();
  }

  public TransportUDFMetadata() {
    _udfs = LinkedHashMultimap.create();
    _classToNumberOfTypeParameters = new HashMap<>();
  }

  public void addUDF(String topLevelClass, String stdUDFImplementation) {
    _udfs.put(topLevelClass, stdUDFImplementation);
  }

  public void setClassNumberOfTypeParameters(String clazz, int numberOfTypeParameters) {
    _classToNumberOfTypeParameters.put(clazz, numberOfTypeParameters);
  }

  public Set<String> getTopLevelClasses() {
    return _udfs.keySet();
  }

  public Collection<String> getStdUDFImplementations(String topLevelClass) {
    return _udfs.get(topLevelClass);
  }

  public Map<String, Integer> getClassToNumberOfTypeParameters() {
    return _classToNumberOfTypeParameters;
  }

  public void toJson(Writer writer) {
    GSON.toJson(TransportUDFMetadataSerDe2.serialize(this), writer);
  }

  public static TransportUDFMetadata fromJsonFile(File jsonFile) {
    try (FileReader reader = new FileReader(jsonFile)) {
      return fromJson(reader);
    } catch (IOException e) {
      throw new RuntimeException("Could not read UDF properties file: " + jsonFile, e);
    }
  }

  public static TransportUDFMetadata fromJson(Reader reader) {
    return TransportUDFMetadataSerDe2.deserialize(new JsonParser().parse(reader));
  }

  /**
   * Represents the JSON object structure of the Transport UDF metadata resource file
   */
  private static class TransportUDFMetadataJson {
    private List<UDFInfo> udfs;

    TransportUDFMetadataJson() {
      this.udfs = new LinkedList<>();
    }

    static class UDFInfo {

      static class ClazzInfo {
        private String className;
        private boolean isTypeParameterized;

        @Override
        public boolean equals(Object o) {
          if (this == o) {
            return true;
          }
          if (o == null || getClass() != o.getClass()) {
            return false;
          }
          ClazzInfo clazzInfo = (ClazzInfo) o;
          return Objects.equals(className, clazzInfo.className);
        }

        @Override
        public int hashCode() {
          return Objects.hash(className);
        }

        ClazzInfo(String className, boolean isTypeParameterized) {
          this.className = className;
          this.isTypeParameterized = isTypeParameterized;
        }
      }

      private ClazzInfo topLevelClass;
      private Collection<ClazzInfo> stdUDFImplementations;

      UDFInfo(ClazzInfo topLevelClass, Collection<ClazzInfo> stdUDFImplementations) {
        this.topLevelClass = topLevelClass;
        this.stdUDFImplementations = stdUDFImplementations;
      }
    }
  }

  private static class TransportUDFMetadataSerDe2 {

    public static TransportUDFMetadata deserialize(JsonElement json) {
      TransportUDFMetadata metadata = new TransportUDFMetadata();
      JsonObject root = json.getAsJsonObject();

      // Deserialize udfs
      JsonObject udfs = root.getAsJsonObject("udfs");
      udfs.keySet().forEach(topLevelClass -> {
        JsonArray stdUdfImplementations = udfs.getAsJsonArray(topLevelClass);
        for (int i = 0; i < stdUdfImplementations.size(); i++) {
          metadata.addUDF(topLevelClass, stdUdfImplementations.get(i).getAsString());
        }
      });

      // Deserialize classToNumberOfTypeParameters
      JsonObject classToNumberOfTypeParameters = root.getAsJsonObject("classToNumberOfTypeParameters");
      classToNumberOfTypeParameters.entrySet().forEach(
          e -> metadata.setClassNumberOfTypeParameters(e.getKey(), e.getValue().getAsInt())
      );
      return metadata;
    }

    public static JsonElement serialize(TransportUDFMetadata metadata) {
      // Serialzie _udfs
      JsonObject udfs = new JsonObject();
      for (Map.Entry<String, Collection<String>> entry : metadata._udfs.asMap().entrySet()) {
        JsonArray stdUdfImplementations = new JsonArray();
        entry.getValue().forEach(f -> stdUdfImplementations.add(f));
        udfs.add(entry.getKey(), stdUdfImplementations);
      }

      // Serialize _classToNumberOfTypeParameters
      JsonObject classToNumberOfTypeParameters = new JsonObject();
      metadata._classToNumberOfTypeParameters.forEach((clazz, n) -> classToNumberOfTypeParameters.addProperty(clazz, n));

      JsonObject root = new JsonObject();
      root.add("udfs", udfs);
      root.add("classToNumberOfTypeParameters", classToNumberOfTypeParameters);
      return root;
    }
  }
  /**
   * Converts objects between {@link TransportUDFMetadata} and {@link TransportUDFMetadataJson}
   */
  /*private static class TransportUDFMetadataSerDe {

    private static TransportUDFMetadataJson fromUDFMetadata(TransportUDFMetadata metadata) {
      TransportUDFMetadataJson metadataJson = new TransportUDFMetadataJson();
      for (String topLevelClass : metadata.getTopLevelClasses()) {
        metadataJson.udfs.add(
            new TransportUDFMetadataJson.UDFInfo(
                new TransportUDFMetadataJson.UDFInfo.ClazzInfo(topLevelClass, metadata.isTypeParameterizedClass(topLevelClass)),
                new TransportUDFMetadataJson.UDFInfo.ClazzInfo()
                metadata.getStdUDFImplementations(topLevelClass),
                metadata.isTypeParameterizedClass(topLevelClass)
            ));
      }
      return metadataJson;
    }

    private static TransportUDFMetadata toUDFMetadata(TransportUDFMetadataJson metadataJson) {
      TransportUDFMetadata metadata = new TransportUDFMetadata();
      for (TransportUDFMetadataJson.UDFInfo udf : metadataJson.udfs) {
        metadata.addUDF(udf.topLevelClass, udf.stdUDFImplementations);
        if (udf.isTypeParameterizedTopLevelClass) {
          metadata.setTypeParameterizedClass(udf.topLevelClass);
        }
      }
      return metadata;
    }
  }*/
}
