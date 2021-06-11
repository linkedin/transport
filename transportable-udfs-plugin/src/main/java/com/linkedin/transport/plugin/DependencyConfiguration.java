/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * Represents a dependency to be applied to a certain sourceset configuration (e.g. implementation, compileOnly, etc.)
 * In the future can expand to incorporate exclude rules, dependency substitutions, etc.
 */
public class DependencyConfiguration {
  private static final String GROUP_KEY = "group";
  private static final String MODULE_KEY = "module";

  private final ConfigurationType _configurationType;
  private final String _module;
  private final String _version;
  private final String _classifier;
  private final Set<Map<String, String>> _excludedProperties;

  private DependencyConfiguration(Builder builder) {
    this._configurationType = builder._configurationType;
    this._module = builder._module;
    this._version = builder._version;
    this._classifier = builder._classifier;
    this._excludedProperties = builder._excludedProperties;
  }

  public ConfigurationType getConfigurationType() {
    return _configurationType;
  }

  public String getDependencyString() {
    return _module + ":" + _version + Optional.ofNullable(_classifier).map(v -> ":" + v).orElse("");
  }

  public Set<Map<String, String>> getExcludedProperties() {
    return _excludedProperties;
  }

  public static Builder builder(final ConfigurationType configurationType, final String module, final String version) {
    return new Builder(configurationType, module, version);
  }

  public static class Builder {
    private final ConfigurationType _configurationType;
    private final String _module;
    private String _version;
    private String _classifier;
    private Set<Map<String, String>> _excludedProperties;

    public Builder(final ConfigurationType configurationType, final String module, final String version) {
      Objects.requireNonNull(configurationType);
      Objects.requireNonNull(module);
      Objects.requireNonNull(version);
      this._configurationType = configurationType;
      this._module = module;
      this._version = version;
    }

    public Builder classifier(final String classifier) {
      Objects.requireNonNull(classifier);
      _classifier = classifier;
      return this;
    }

    public Builder exclude(final String group) {
      return exclude(group, null);
    }

    public Builder exclude(final String group, final String module) {
      Objects.requireNonNull(group);
      if (_excludedProperties == null) {
        _excludedProperties = new HashSet<>();
      }
      _excludedProperties.add((module == null)
          ? ImmutableMap.of(GROUP_KEY, group)
          : ImmutableMap.of(GROUP_KEY, group, MODULE_KEY, module)
      );
      return this;
    }

    public DependencyConfiguration build() {
      return new DependencyConfiguration(this);
    }
  }
}