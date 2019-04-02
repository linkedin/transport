/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.codegen;

/**
 * Interface to be implemented by classes which generate platform-specific wrappers for Transport UDFs
 */
public interface WrapperGenerator {

  /**
   * Generates wrappers for Transport UDFs in the given {@link WrapperGeneratorContext}
   */
  void generateWrappers(WrapperGeneratorContext context);
}
