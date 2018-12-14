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


public class StdTesterService {

  private static StdTesterService service;
  private Class<? extends StdTester> stdTesterClass;

  private StdTesterService() {
    try {
      List<Class<? extends StdTester>> stdTesterClasses = new LinkedList<>();
      ServiceLoader<StdTester> loader = ServiceLoader.load(StdTester.class);
      for (StdTester tester : loader) {
        stdTesterClasses.add(tester.getClass());
      }
      if (stdTesterClasses.size() == 0) {
        throw new RuntimeException("Could not find StdTester class on the classpath");
      } else if (stdTesterClasses.size() > 1) {
        throw new RuntimeException(
            "Expected only one StdTester class on the classpath, found " + stdTesterClasses.size() + ".\nFound: "
                + stdTesterClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
      }
      stdTesterClass = stdTesterClasses.get(0);
    } catch (ServiceConfigurationError e) {
      throw new RuntimeException(e);
    }
  }

  private static synchronized StdTesterService getInstance() {
    if (service == null) {
      service = new StdTesterService();
    }
    return service;
  }

  /**
   * Returns the {@link StdTester} available on the classpath
   */
  public static StdTester getTester() {
    StdTesterService service = getInstance();
    try {
      return service.stdTesterClass.newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw (new RuntimeException(e));
    }
  }
}