/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.plugin;

public enum Language {
  JAVA("Java"),
  SCALA("Scala");

  private String _language;

  Language(String language) {
    this._language = language;
  }

  @Override
  public String toString() {
    return _language;
  }
}
