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
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.*;


public class TransportConnectorFactory implements ConnectorFactory {

  private static final String UDF_JAR_PATH  =
      "file:///Users/yiqding/workspace/trino/core/trino-server-main/plugin/transportable-udfs-trino-plugin-0.0.93/udf/";
  private static final Logger log = Logger.get(TransportConnectorFactory.class);
  private Connector connector;
  private final Class<? extends Module> module;


  public TransportConnectorFactory(Connector connector) {
    this.connector = connector;
    this.module = TransportModule.class;
  }

  public TransportConnectorFactory(Class<? extends Module> module) {
    connector = null;
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
      ClassLoader classLoaderForUdfWrapper = StdUdfWrapper.class.getClassLoader();
      log.info(classLoaderForUdfWrapper.toString());
      ClassLoader classLoaderForFactory = TransportConnectorFactory.class.getClassLoader();
      log.info(classLoaderForFactory.toString());
      try {
        List<URL> urlList = ImmutableList.of(new URL(UDF_JAR_PATH + "transportable-udfs-example-udfs.jar"),
            new URL(UDF_JAR_PATH + "transportable-udfs-example-udfs-trino-dist-thin.jar"),
            new URL(UDF_JAR_PATH + "transportable-udfs-api-0.0.93.jar"),
            new URL(UDF_JAR_PATH + "transportable-udfs-trino-0.0.93.jar"),
            new URL(UDF_JAR_PATH + "transportable-udfs-type-system-0.0.93.jar"),
            new URL(UDF_JAR_PATH + "transportable-udfs-utils-0.0.93.jar"));

        TransportUDFClassLoader classLoaderForUdf = new TransportUDFClassLoader(classLoaderForFactory, urlList);
        Class<?> udfClass = classLoaderForUdf.loadClass("com.linkedin.transport.examples.trino.ArrayElementAtFunction", false);
        Object udfObj = udfClass.getConstructor().newInstance();
        log.info(udfObj.toString());
        Map<FunctionId, StdUdfWrapper> functions = new HashMap<>();
        log.info("adding wrapper");
        StdUdfWrapper wrapper = (StdUdfWrapper) udfObj;
        functions.put(wrapper.getFunctionMetadata().getFunctionId(), wrapper);
        this.connector = new TransportConnector(functions);
      } catch (Exception ex) {
        log.error(ex);
        throw new RuntimeException(ex.getCause());
      }
    }
    return this.connector;
  }
}
