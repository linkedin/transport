/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.testing.DistributedQueryRunner;
import java.util.List;


public class TrinoTestFunctionDependencies implements FunctionDependencies {
  private final TypeManager typeManager;
  private final DistributedQueryRunner queryRunner;

  public TrinoTestFunctionDependencies(TypeManager typeManager, DistributedQueryRunner queryRunner) {
    this.typeManager = typeManager;
    this.queryRunner = queryRunner;
  }

  @Override
  public Type getType(TypeSignature typeSignature) {
    return typeManager.getType(typeSignature);
  }

  @Override
  public FunctionNullability getFunctionNullability(CatalogSchemaFunctionName name, List<Type> parameterTypes) {
    return null;
  }

  @Override
  public FunctionNullability getOperatorNullability(OperatorType operatorType, List<Type> parameterTypes) {
    return null;
  }

  @Override
  public FunctionNullability getCastNullability(Type fromType, Type toType) {
    return null;
  }

  @Override
  public ScalarFunctionImplementation getScalarFunctionImplementation(CatalogSchemaFunctionName name,
      List<Type> parameterTypes, InvocationConvention invocationConvention) {
    return null;
  }

  @Override
  public ScalarFunctionImplementation getScalarFunctionImplementationSignature(CatalogSchemaFunctionName name,
      List<TypeSignature> parameterTypes, InvocationConvention invocationConvention) {
    return null;
  }

  @Override
  public ScalarFunctionImplementation getOperatorImplementation(OperatorType operatorType, List<Type> parameterTypes,
      InvocationConvention invocationConvention) {
    var planner   = queryRunner.getCoordinator().getPlannerContext();
    var metadata  = planner.getMetadata();

    ResolvedFunction resolved = metadata.resolveOperator(operatorType, parameterTypes);
    return planner.getFunctionManager()
        .getScalarFunctionImplementation(resolved, invocationConvention);
  }

  @Override
  public ScalarFunctionImplementation getOperatorImplementationSignature(OperatorType operatorType,
      List<TypeSignature> parameterTypes, InvocationConvention invocationConvention) {
    return null;
  }

  @Override
  public ScalarFunctionImplementation getCastImplementation(Type fromType, Type toType,
      InvocationConvention invocationConvention) {
    return null;
  }

  @Override
  public ScalarFunctionImplementation getCastImplementationSignature(TypeSignature fromType, TypeSignature toType,
      InvocationConvention invocationConvention) {
    return null;
  }
}
