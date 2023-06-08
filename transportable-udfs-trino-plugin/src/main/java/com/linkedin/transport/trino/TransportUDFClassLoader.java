/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;


/**
 * The approach of Trino plugin is used to dynamically load UDF classes into Trino server.
 * The infrastructure classes of this Trino plugin (e.g. TransportConnectorFactory, TransportConnector and so on) are
 * loaded by PluginClassLoader defined in Trino during the initialization of plugins. In current implementation of Trino,
 * only the URLs of the jars immediately under the directory where TransportPlugin deployed are passed into PluginClassLoader.
 * However, the jars containing actual UDF classes cannot be deployed in the same directory. As the URLs of jars with actual UDF classes
 * are not visible to PluginClassLoader, PluginClassLoader cannot be used to load UDF classes.
 * Therefore, TransportUDFClassLoader is built with the URLs to the jars with UDF classes to load all UDF classes inside those jars.
 * Also, PluginClassLoader is used as the parent of TransportClassLoader. It can help prevent TransportUDFClassLoader load some base classes
 * defined in Transport (e.g. com.linkedin.transport.trino.StdUdfWrapper) which have been already loaded by PluginClassLoader
 */
public class TransportUDFClassLoader extends URLClassLoader {
  private final ClassLoader parent;

  public TransportUDFClassLoader(ClassLoader parent, List<URL> urls) {
    super(urls.toArray(new URL[0]));
    this.parent = parent;
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      // Check if class is in the loaded classes cache
      Class<?> cachedClass = findLoadedClass(name);
      if (cachedClass != null) {
        return resolveClass(cachedClass, resolve);
      }

      if (name.equals("com.linkedin.transport.trino.StdUdfWrapper")
          || name.startsWith("com.linkedin.transport.api")
          || name.startsWith("org.apache.hadoop")) {
        return resolveClass(parent.loadClass(name), resolve);
      }

      // Look for class locally
      return super.loadClass(name, resolve);
    }
  }

  private Class<?> resolveClass(Class<?> clazz, boolean resolve) {
    if (resolve) {
      resolveClass(clazz);
    }
    return clazz;
  }
}