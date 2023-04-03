/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;


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

      if (name.endsWith("StdUdfWrapper") || name.endsWith("TopLevelStdUDF") || name.endsWith("StdUDF")) {
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