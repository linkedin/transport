/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

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

  protected abstract boolean isFloatType(T dataType);

  protected abstract boolean isDoubleType(T dataType);

  protected abstract boolean isBytesType(T dataType);

  protected abstract boolean isArrayType(T dataType);

  protected abstract boolean isMapType(T dataType);

  protected abstract boolean isStructType(T dataType);

  protected abstract T createBooleanType();

  protected abstract T createIntegerType();

  protected abstract T createLongType();

  protected abstract T createStringType();

  protected abstract T createFloatType();

  protected abstract T createDoubleType();

  protected abstract T createBytesType();

  protected abstract T createUnknownType();

  protected abstract T createArrayType(T elementType);

  protected abstract T createMapType(T keyType, T valueType);

  protected abstract T createStructType(List<String> fieldNames, List<T> fieldTypes);
}
