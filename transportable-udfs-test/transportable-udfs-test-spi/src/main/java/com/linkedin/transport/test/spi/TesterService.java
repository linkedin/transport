/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;


public class TesterService {

  private static TesterService service;
  private Class<? extends Tester> testerClass;

  private TesterService() {
    try {
      List<Class<? extends Tester>> testerClasses = new LinkedList<>();
      ServiceLoader<Tester> loader = ServiceLoader.load(Tester.class);
      for (Tester tester : loader) {
        testerClasses.add(tester.getClass());
      }
      if (testerClasses.size() == 0) {
        throw new RuntimeException("Could not find Tester class on the classpath");
      } else if (testerClasses.size() > 1) {
        throw new RuntimeException(
            "Expected only one Tester class on the classpath, found " + testerClasses.size() + ".\nFound: "
                + testerClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
      }
      testerClass = testerClasses.get(0);
    } catch (ServiceConfigurationError e) {
      throw new RuntimeException(e);
    }
  }

  private static synchronized TesterService getInstance() {
    if (service == null) {
      service = new TesterService();
    }
    return service;
  }

  /**
   * Returns the {@link Tester} available on the classpath
   */
  public static Tester getTester() {
    TesterService service = getInstance();
    try {
      return service.testerClass.newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw (new RuntimeException(e));
    }
  }
}