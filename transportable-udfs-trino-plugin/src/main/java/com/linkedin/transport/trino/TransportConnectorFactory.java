/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.FunctionId;
import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static java.util.Objects.*;


public class TransportConnectorFactory implements ConnectorFactory {

  private static final String DEFAULT_TRANSPORT_UDF_REPO = "transport-udf-repo";
  private static final String TRANSPORT_UDF_REPO_CONFIG_NAME = "transport.udf.repo";
  private static final Logger log = Logger.get(TransportConnectorFactory.class);

  private static final FileFilter TRANSPORT_UDF_JAR_FILTER = (file) ->
     file.isFile() && file.getName().endsWith(".jar") && !file.getName().startsWith("transportable-udfs");

  @Override
  public String getName() {
    return "TRANSPORT";
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    requireNonNull(config, "config  is null");
    ClassLoader classLoaderForFactory = TransportConnectorFactory.class.getClassLoader();
    List<URL> jarUrlList = getUDFJarUrls(config);
    log.info("The URLs of Transport UDF jars: " + jarUrlList);
    TransportUDFClassLoader classLoaderForUdf = new TransportUDFClassLoader(classLoaderForFactory, jarUrlList);
    ServiceLoader<StdUdfWrapper> serviceLoader = ServiceLoader.load(StdUdfWrapper.class, classLoaderForUdf);
    List<StdUdfWrapper> stdUdfWrappers = ImmutableList.copyOf(serviceLoader);
    Map<FunctionId, StdUdfWrapper> functions = new HashMap<>();
    for (StdUdfWrapper wrapper : stdUdfWrappers) {
      log.info("Loading Transport UDF class: " + wrapper.getFunctionMetadata().getFunctionId().toString());
      functions.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
    }
    return new TransportConnector(functions);
  }

  private static List<URL> getUDFJarUrls(Map<String, String> config) {
    String udfDir = config.getOrDefault(TRANSPORT_UDF_REPO_CONFIG_NAME, DEFAULT_TRANSPORT_UDF_REPO);
    if (!Paths.get(udfDir).isAbsolute()) {
      Path workingDirPath = Paths.get("").toAbsolutePath();
      udfDir = Paths.get(workingDirPath.toString(), udfDir).toString();
    }
    File[] udfSubDirs = new File(udfDir).listFiles(File::isDirectory);
    return Arrays.stream(udfSubDirs).flatMap(e -> getUDFJarUrlFromDir(e).stream()).collect(Collectors.toList());
  }

  private static List<URL> getUDFJarUrlFromDir(File path) {
    List<URL> urlList = new ArrayList<>();
    File[] files = path.listFiles(TRANSPORT_UDF_JAR_FILTER);
    for (File file : files) {
      try {
        if (file != null) {
          urlList.add(file.toURI().toURL());
        }
      } catch (MalformedURLException ex) {
        log.error("Fail to parsing the URL of the given jar file ", ex);
        throw new RuntimeException(ex);
      }
    }
    return urlList;
  }
}
