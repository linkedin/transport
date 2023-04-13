/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.*;


public class TransportConnectorFactory implements ConnectorFactory {

  private static final String TRANSPORT_UDF_CLASSES = "transport.udf.classes";
  private static final String TRANSPORT_UDF_MP = "/transport-udf-mp";
  private static final Logger log = Logger.get(TransportConnectorFactory.class);
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
      try {
        ClassLoader classLoaderForFactory = TransportConnectorFactory.class.getClassLoader();
        List<URL> allUrlList = getUDFJarUrls();
        TransportUDFClassLoader classLoaderForUdf = new TransportUDFClassLoader(classLoaderForFactory, allUrlList);

        Map<FunctionId, StdUdfWrapper> functions = new HashMap<>();
        if (config.containsKey(TRANSPORT_UDF_CLASSES)) {
          String []udfClassPaths = config.get(TRANSPORT_UDF_CLASSES).split(",");
          Set<String> udfClassPathSet = new HashSet<>(Arrays.asList(udfClassPaths));
          for (String classPath : udfClassPathSet) {
            Class<?> udfClass = classLoaderForUdf.loadClass(classPath, false);
            Object udfObj = udfClass.getConstructor().newInstance();
            log.info(udfObj.toString());
            StdUdfWrapper wrapper = (StdUdfWrapper) udfObj;
            functions.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
          }
        }
        this.connector = new TransportConnector(functions);
      } catch (Exception ex) {
        log.error(ex);
        throw new RuntimeException(ex.getCause());
      }
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
    FileFilter fileFilter = (file) -> {
      return file.isFile() && file.getName().endsWith(".jar") && !file.getName().startsWith("transportable-udfs");
    };
    File[] files = path.listFiles(fileFilter);
    for (File file : files) {
      try {
        urlList.add(file.toURI().toURL());
      } catch (MalformedURLException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
