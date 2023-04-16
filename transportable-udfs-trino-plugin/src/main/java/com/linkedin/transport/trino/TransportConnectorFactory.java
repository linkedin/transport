/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.FunctionId;
import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.Objects.*;


public class TransportConnectorFactory implements ConnectorFactory {

  private static final String TRANSPORT_UDF_MP = "/transport-udf-mp";
  private static final Logger log = Logger.get(TransportConnectorFactory.class);

  private static final FileFilter TRANSPORT_UDF_JAR_FILTER = (file) -> {
    return file.isFile() && file.getName().endsWith(".jar") && !file.getName().startsWith("transportable-udfs");
  };

  private Connector connector;
  private final Class<? extends Module> module;


  // test only
  public TransportConnectorFactory(Connector connector) {
    this.connector = connector;
    this.module = TransportModule.class;
  }

  public TransportConnectorFactory(Class<? extends Module> module) {
    this.connector = null;
    this.module = module;
  }

  @Override
  public String getName() {
    return "TRANSPORT";
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    requireNonNull(config, "config  is null");
    if (this.connector == null) {
      ClassLoader classLoaderForFactory = TransportConnectorFactory.class.getClassLoader();
      List<URL> allUrlList = getUDFJarUrls();
      TransportUDFClassLoader classLoaderForUdf = new TransportUDFClassLoader(classLoaderForFactory, allUrlList);
      ServiceLoader<StdUdfWrapper> serviceLoader = ServiceLoader.load(StdUdfWrapper.class, classLoaderForUdf);
      List<StdUdfWrapper> stdUdfWrappers = ImmutableList.copyOf(serviceLoader);
      Map<FunctionId, StdUdfWrapper> functions = new HashMap<>();
      for (StdUdfWrapper wrapper : stdUdfWrappers) {
        log.info("Loading Transport UDF class: " + wrapper.getFunctionMetadata().getFunctionId().toString());
        functions.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
      }
      this.connector = new TransportConnector(functions);
    }
    return this.connector;
  }

  private List<URL> getUDFJarUrls() {
    String workingDir = System.getProperty("user.dir");
    log.info(workingDir);
    String udfDir = workingDir + TRANSPORT_UDF_MP;
    File[] udfSubDirs = new File(udfDir).listFiles(File::isDirectory);
    log.info(Arrays.toString(udfSubDirs));
    List<URL> urlList = new ArrayList<>();
    for (File subDirPath : udfSubDirs) {
      getUDFJarUrlFromDir(subDirPath, urlList);
    }
    return urlList;
  }

  private void getUDFJarUrlFromDir(File path, List<URL> urlList) {
    File[] files = path.listFiles(TRANSPORT_UDF_JAR_FILTER);
    for (File file : files) {
      try {
        urlList.add(file.toURI().toURL());
      } catch (MalformedURLException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
