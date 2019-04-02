/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

/**
 * Represents a dependency to be applied to a certain dependency configuration (e.g. implementation, compileOnly, etc.)
 * In the future can expand to incorporate exclude rules, dependency substitutions, etc.
 */
public class DependencyConfiguration {
  private DependencyConfigurationName _configurationName;
  private String _dependencyString;

  public DependencyConfiguration(DependencyConfigurationName configurationName, String dependencyString) {
    _configurationName = configurationName;
    _dependencyString = dependencyString;
  }

  public DependencyConfigurationName getConfigurationName() {
    return _configurationName;
  }

  public String getDependencyString() {
    return _dependencyString;
  }
}