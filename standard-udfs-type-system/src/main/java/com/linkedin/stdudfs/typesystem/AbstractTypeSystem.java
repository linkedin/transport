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
