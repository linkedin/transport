/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.transaction.IsolationLevel;
import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import javax.inject.Inject;

import static java.util.Objects.*;


/**
 * This class implements the interface of Connector from Trino SPI as a part of Trino plugin
 * to load UDF classes in Trino server following the development guideline
 * in https://trino.io/docs/current/develop/spi-overview.html
 */
public class TransportConnector implements Connector {

  private static final Logger log = Logger.get(TransportConnector.class);
  private static final FileFilter TRANSPORT_UDF_JAR_FILTER = (file) ->
      file.isFile() && file.getName().endsWith(".jar") && !file.getName().startsWith("transportable-udfs-api")
      && !file.getName().startsWith("transportable-udfs-trino")
      && !file.getName().startsWith("transportable-udfs-type-system")
      && !file.getName().startsWith("transportable-udfs-utils");

  private final ConnectorMetadata connectorMetadata;
  private final FunctionProvider functionProvider;

  public TransportConnector(ConnectorMetadata connectorMetadata, FunctionProvider functionProvider) {
    this.connectorMetadata = requireNonNull(connectorMetadata, "connector metadata is null");
    this.functionProvider = requireNonNull(functionProvider, "function provider is null");
  }

  @Inject
  public TransportConnector(TransportConfig config) {
    ClassLoader classLoaderForFactory = TransportConnectorFactory.class.getClassLoader();
    List<List<URL>> allJarUrlList = getUDFJarUrls(config);
    log.info("The URLs of Transport UDF jars: " + allJarUrlList);
    List<StdUdfWrapper> stdUdfWrappers = new ArrayList<>();
    // create one TransportUDFClassLoader to load jars for one sub directory under Transport UDF repo
    for (List<URL> jarUrlList : allJarUrlList) {
      TransportUDFClassLoader classLoaderForUdf = new TransportUDFClassLoader(classLoaderForFactory, jarUrlList);
      ServiceLoader<StdUdfWrapper> serviceLoader = ServiceLoader.load(StdUdfWrapper.class, classLoaderForUdf);
      stdUdfWrappers.addAll(ImmutableList.copyOf(serviceLoader));
    }

    ImmutableMap.Builder<FunctionId, StdUdfWrapper> functionIdStdUdfWrapperBuilder = ImmutableMap.builder();
    for (StdUdfWrapper wrapper : stdUdfWrappers) {
      log.info("Loading Transport UDF class: " + wrapper.getFunctionMetadata().getFunctionId().toString());
      functionIdStdUdfWrapperBuilder.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
    }

    Map<FunctionId, StdUdfWrapper> functions = functionIdStdUdfWrapperBuilder.build();
    this.connectorMetadata = new TransportConnectorMetadata(functions);
    this.functionProvider = new TransportFunctionProvider(functions);
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return this.connectorMetadata;
  }

  @Override
  public Optional<FunctionProvider> getFunctionProvider() {
    return Optional.of(this.functionProvider);
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
      boolean autoCommit) {
    return TransportTransactionHandle.INSTANCE;
  }

  private static List<List<URL>> getUDFJarUrls(TransportConfig config) {
    String udfDir =  config.getTransportUdfRepo();
    if (!Paths.get(udfDir).isAbsolute()) {
      Path workingDirPath = Paths.get("").toAbsolutePath();
      udfDir = Paths.get(workingDirPath.toString(), udfDir).toString();
    }
    File[] udfSubDirs = new File(udfDir).listFiles(File::isDirectory);
    return Arrays.stream(udfSubDirs).map(e -> getUDFJarUrlFromDir(e)).collect(Collectors.toList());
  }

  protected static List<URL> getUDFJarUrlFromDir(File path) {
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
