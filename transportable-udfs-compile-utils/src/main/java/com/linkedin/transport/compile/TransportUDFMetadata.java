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
import java.util.Map;
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
    GSON.toJson(TransportUDFMetadataSerDe.serialize(this), writer);
  }

  public static TransportUDFMetadata fromJsonFile(File jsonFile) {
    try (FileReader reader = new FileReader(jsonFile)) {
      return fromJson(reader);
    } catch (IOException e) {
      throw new RuntimeException("Could not read UDF properties file: " + jsonFile, e);
    }
  }

  public static TransportUDFMetadata fromJson(Reader reader) {
    return TransportUDFMetadataSerDe.deserialize(new JsonParser().parse(reader));
  }

  private static class TransportUDFMetadataSerDe {

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
}
