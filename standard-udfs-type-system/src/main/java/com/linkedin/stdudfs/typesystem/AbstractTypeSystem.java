/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.typesystem;

import java.util.List;


public abstract class AbstractTypeSystem<T> {

  protected abstract T getArrayElementType(T dataType);

  protected abstract T getMapKeyType(T dataType);

  protected abstract T getMapValueType(T dataType);

  protected abstract List<T> getStructFieldTypes(T dataType);

  protected abstract boolean isUnknownType(T dataType);

  protected abstract boolean isBooleanType(T dataType);

  protected abstract boolean isIntegerType(T dataType);

  protected abstract boolean isLongType(T dataType);

  protected abstract boolean isStringType(T dataType);

  protected abstract boolean isArrayType(T dataType);

  protected abstract boolean isMapType(T dataType);

  protected abstract boolean isStructType(T dataType);

  protected abstract T createBooleanType();

  protected abstract T createIntegerType();

  protected abstract T createLongType();

  protected abstract T createStringType();

  protected abstract T createUnknownType();

  protected abstract T createArrayType(T elementType);

  protected abstract T createMapType(T keyType, T valueType);

  protected abstract T createStructType(List<String> fieldNames, List<T> fieldTypes);
}
