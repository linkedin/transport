/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

/**
 * Represents a dependency to be applied to a certain sourceset configuration (e.g. implementation, compileOnly, etc.)
 * In the future can expand to incorporate exclude rules, dependency substitutions, etc.
 */
public class DependencyConfiguration {
  private ConfigurationType _configurationType;
  private String _dependencyString;

  public DependencyConfiguration(ConfigurationType configurationType, String dependencyString) {
    _configurationType = configurationType;
    _dependencyString = dependencyString;
  }

  public ConfigurationType getConfigurationType() {
    return _configurationType;
  }

  public String getDependencyString() {
    return _dependencyString;
  }
}