/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.linkedin.transport.codegen.WrapperGenerator;
import com.linkedin.transport.plugin.packaging.Packaging;
import java.util.List;


/**
 * Represents the information required to configure a given platform inside the {@link TransportPlugin}
 */
public class Platform {

  private final String _name;
  private final Language _language;
  private final Class<? extends WrapperGenerator> _wrapperGeneratorClass;
  private final List<DependencyConfiguration> _defaultWrapperDependencyConfigurations;
  private final List<DependencyConfiguration> _defaultTestDependencyConfigurations;
  private final Packaging _packaging;

  public Platform(String name, Language language, Class<? extends WrapperGenerator> wrapperGeneratorClass,
      List<DependencyConfiguration> defaultWrapperDependencyConfigurations,
      List<DependencyConfiguration> defaultTestDependencyConfigurations, Packaging packaging) {
    _name = name;
    _language = language;
    _wrapperGeneratorClass = wrapperGeneratorClass;
    _defaultWrapperDependencyConfigurations = defaultWrapperDependencyConfigurations;
    _defaultTestDependencyConfigurations = defaultTestDependencyConfigurations;
    _packaging = packaging;
  }

  public String getName() {
    return _name;
  }

  public Language getLanguage() {
    return _language;
  }

  public Class<? extends WrapperGenerator> getWrapperGeneratorClass() {
    return _wrapperGeneratorClass;
  }

  public List<DependencyConfiguration> getDefaultWrapperDependencyConfigurations() {
    return _defaultWrapperDependencyConfigurations;
  }

  public List<DependencyConfiguration> getDefaultTestDependencyConfigurations() {
    return _defaultTestDependencyConfigurations;
  }

  public Packaging getPackaging() {
    return _packaging;
  }
}
