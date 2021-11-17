/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.typesystem;

import com.google.common.base.Preconditions;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


public abstract class AbstractTypeInference<T> {
  private T[] _inputDataTypes;
  private T _outputDataType;
  private UDF _udf;
  private TypeFactory _typeFactory;
  private AbstractTypeSystem<T> _typeSystem;

  public AbstractTypeInference() {
    _typeSystem = getTypeSystem();
  }

  private static String signaturesToString(Collection<List<String>> signatures) {
    return signatures.stream().map(l -> String.join(", ", l)).collect(Collectors.joining("\n"));
  }

  protected abstract AbstractTypeSystem<T> getTypeSystem();

  private boolean isUnknownType(T dataType) {
    return _typeSystem.isUnknownType(dataType);
  }

  private boolean isBooleanType(T dataType) {
    return _typeSystem.isBooleanType(dataType);
  }

  private boolean isIntegerType(T dataType) {
    return _typeSystem.isIntegerType(dataType);
  }

  private boolean isLongType(T dataType) {
    return _typeSystem.isLongType(dataType);
  }

  private boolean isStringType(T dataType) {
    return _typeSystem.isStringType(dataType);
  }

  private boolean isFloatType(T dataType) {
    return _typeSystem.isFloatType(dataType);
  }

  private boolean isDoubleType(T dataType) {
    return _typeSystem.isDoubleType(dataType);
  }

  private boolean isBinaryType(T dataType) {
    return _typeSystem.isBinaryType(dataType);
  }

  private boolean isArrayType(T dataType) {
    return _typeSystem.isArrayType(dataType);
  }

  private boolean isMapType(T dataType) {
    return _typeSystem.isMapType(dataType);
  }

  private boolean isStructType(T dataType) {
    return _typeSystem.isStructType(dataType);
  }

  private T getArrayElementType(T dataType) {
    return _typeSystem.getArrayElementType(dataType);
  }

  private T getMapKeyType(T dataType) {
    return _typeSystem.getMapKeyType(dataType);
  }

  private T getMapValueType(T dataType) {
    return _typeSystem.getMapValueType(dataType);
  }

  private List<T> getStructFieldTypes(T dataType) {
    return _typeSystem.getStructFieldTypes(dataType);
  }

  public void compile(
      T[] dataTypes,
      List<? extends UDF> udfImplementations,
      Class<? extends TopLevelUDF> topLevelUdfClass) {
    Preconditions.checkArgument(udfImplementations.size() > 0,
        "Empty Standard UDF Implementations list");
    AbstractBoundVariables<T> boundVariables = null;
    boolean atLeastOneInputParametersSignaturesBindingSuccess = false;
    for (UDF udf : udfImplementations) {
      List<String> inputParameterSignatures = udf.getInputParameterSignatures();
      if (inputParameterSignatures.size() != dataTypes.length) {
        continue;
      }
      boundVariables = createBoundVariables();
      boolean currentInputParametersSignaturesBindingSuccess = true;
      for (int i = 0; i < inputParameterSignatures.size(); i++) {
        currentInputParametersSignaturesBindingSuccess =
            currentInputParametersSignaturesBindingSuccess
                && boundVariables.bind(TypeSignature.parse(inputParameterSignatures.get(i)), dataTypes[i]);
      }
      if (currentInputParametersSignaturesBindingSuccess) {
        _inputDataTypes = dataTypes;
        _typeFactory = createTypeFactory(boundVariables);
        _udf = udf;
        atLeastOneInputParametersSignaturesBindingSuccess = true;
        break;
      }
    }

    if (!atLeastOneInputParametersSignaturesBindingSuccess) {
      throw new RuntimeException("Error processing UDF of type: "
          + topLevelUdfClass.getName()
          + ". Received UDF inputs of type "
          + dataTypesToString(dataTypes)
          + " while expecting one of the following type signatures:\n"
          + signaturesToString(udfImplementations.stream().map(UDF::getInputParameterSignatures)
          .collect(Collectors.toList())));
    }

    _outputDataType = getTypeFactoryFacade().createType(
        TypeSignature.parse(_udf.getOutputParameterSignature()),
        boundVariables
    );
  }

  private String dataTypesToString(T[] dataTypes) {
    return Arrays.stream(dataTypes).map(t -> dataTypeToString(t)).collect(Collectors.joining(", "));
  }

  private String dataTypeToString(T dataType) {
    if (isBooleanType(dataType)) {
      return "boolean";
    } else if (isIntegerType(dataType)) {
      return "integer";
    } else if (isLongType(dataType)) {
      return "bigint";
    } else if (isStringType(dataType)) {
      return "varchar";
    } else if (isFloatType(dataType)) {
      return "real";
    } else if (isDoubleType(dataType)) {
      return "double";
    } else if (isBinaryType(dataType)) {
      return "varbinary";
    } else if (isUnknownType(dataType)) {
      return "unknown";
    } else if (isArrayType(dataType)) {
      return "array("
          + dataTypeToString(getArrayElementType(dataType))
          + ")";
    } else if (isMapType(dataType)) {
      return "map("
          + dataTypeToString(getMapKeyType(dataType)) + ", "
          + dataTypeToString(getMapValueType(dataType))
          + ")";
    } else if (isStructType(dataType)) {
      return "row("
          + getStructFieldTypes(dataType).stream().map(f -> dataTypeToString(f)).collect(Collectors.joining(", "))
          + ")";
    }
    throw new IllegalArgumentException("Unrecognized data type: " + dataType.getClass());
  }

  protected abstract AbstractBoundVariables<T> createBoundVariables();

  protected abstract TypeFactory createTypeFactory(AbstractBoundVariables<T> boundVariables);

  protected abstract AbstractTypeFactory<T> getTypeFactoryFacade();

  public TypeFactory getTypeFactory() {
    return _typeFactory;
  }

  public T[] getInputDataTypes() {
    return _inputDataTypes;
  }

  public T getOutputDataType() {
    return _outputDataType;
  }

  public UDF getUdf() {
    return _udf;
  }
}
