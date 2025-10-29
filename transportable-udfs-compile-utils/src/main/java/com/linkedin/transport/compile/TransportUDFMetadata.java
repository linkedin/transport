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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * Maintains all metadata of Transport UDFs to be included in the UDF metadata file
 */
public class TransportUDFMetadata {
  private static final Gson GSON;
  private Multimap<String, String> _udfs;

  static {
    GSON = new GsonBuilder().setPrettyPrinting().create();
  }

  public TransportUDFMetadata() {
    _udfs = LinkedHashMultimap.create();
  }

  public void addUDF(String topLevelClass, String stdUDFImplementation) {
    _udfs.put(topLevelClass, stdUDFImplementation);
  }

  public void addUDF(String topLevelClass, Collection<String> stdUDFImplementations) {
    _udfs.putAll(topLevelClass, stdUDFImplementations);
  }

  public Set<String> getTopLevelClasses() {
    return _udfs.keySet();
  }

  public Collection<String> getStdUDFImplementations(String topLevelClass) {
    return _udfs.get(topLevelClass);
  }

  public void toJson(Writer writer) throws IOException {
    String json = GSON.toJson(TransportUDFMetadataSerDe.fromUDFMetadata(this));
    // Gson uses \n for newlines, but on Windows we need \r\n to match test expectations
    String jsonWithSystemLineSeparator = json.replace("\n", System.lineSeparator());
    writer.write(jsonWithSystemLineSeparator);
  }

  public static TransportUDFMetadata fromJsonFile(File jsonFile) {
    try (FileReader reader = new FileReader(jsonFile)) {
      return fromJson(reader);
    } catch (IOException e) {
      throw new RuntimeException("Could not read UDF properties file: " + jsonFile, e);
    }
  }

  public static TransportUDFMetadata fromJson(Reader reader) {
    return TransportUDFMetadataSerDe.toUDFMetadata(GSON.fromJson(reader, TransportUDFMetadataJson.class));
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
      private String topLevelClass;
      private Collection<String> stdUDFImplementations;

      UDFInfo(String topLevelClass, Collection<String> stdUDFImplementations) {
        this.topLevelClass = topLevelClass;
        this.stdUDFImplementations = stdUDFImplementations;
      }
    }
  }

  /**
   * Converts objects between {@link TransportUDFMetadata} and {@link TransportUDFMetadataJson}
   */
  private static class TransportUDFMetadataSerDe {

    private static TransportUDFMetadataJson fromUDFMetadata(TransportUDFMetadata metadata) {
      TransportUDFMetadataJson metadataJson = new TransportUDFMetadataJson();
      for (String topLevelClass : metadata.getTopLevelClasses()) {
        metadataJson.udfs.add(
            new TransportUDFMetadataJson.UDFInfo(topLevelClass, metadata.getStdUDFImplementations(topLevelClass)));
      }
      return metadataJson;
    }

    private static TransportUDFMetadata toUDFMetadata(TransportUDFMetadataJson metadataJson) {
      TransportUDFMetadata metadata = new TransportUDFMetadata();
      for (TransportUDFMetadataJson.UDFInfo udf : metadataJson.udfs) {
        metadata.addUDF(udf.topLevelClass, udf.stdUDFImplementations);
      }
      return metadata;
    }
  }
}
