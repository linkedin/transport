/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive;

import com.google.common.base.Preconditions;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.hive.HiveFactory;
import com.linkedin.transport.hive.typesystem.HiveBoundVariables;
import com.linkedin.transport.test.hive.udf.MapFromEntriesWrapper;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.SqlStdTester;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.testng.Assert;


public class HiveTester implements SqlStdTester {

  private StdFactory _stdFactory;
  private CLIService _client;
  private SessionHandle _sessionHandle;
  private Registry _functionRegistry;
  private Method _functionRegistryAddFunctionMethod;
  private SqlFunctionCallGenerator _sqlFunctionCallGenerator;
  private ToPlatformTestOutputConverter _platformOutputDataConverter;

  public HiveTester() {
    _stdFactory = new HiveFactory(new HiveBoundVariables());
    _sqlFunctionCallGenerator = new HiveSqlFunctionCallGenerator();
    _platformOutputDataConverter = new ToHiveTestOutputConverter();
    createHiveServer();
  }

  private void createHiveServer() {
    HiveServer2 server = new HiveServer2();
    server.init(new HiveConf());
    for (Service service : server.getServices()) {
      if (service instanceof CLIService) {
        _client = (CLIService) service;
      }
    }
    Preconditions.checkNotNull(_client, "CLI service not found in local Hive server");
    try {
      _sessionHandle = _client.openSession(null, null, null);
      _functionRegistry = SessionState.getRegistryForWrite();
      // "map_from_entries" UDF is required to create maps with non-primitive key types
      _functionRegistry.registerGenericUDF("map_from_entries", MapFromEntriesWrapper.class);
      // TODO: This is a hack. Hive's public API does not have a way to register an already created GenericUDF object
      // It only accepts a class name after which the parameterless constructor of the class is called to create a
      // GenericUDF object. This does not work for HiveTestStdUDFWrapper as it accepts the UDF classes as parameters.
      // However, Hive has an internal method which does allow passing GenericUDF objects instead of classes.
      _functionRegistryAddFunctionMethod =
          _functionRegistry.getClass().getDeclaredMethod("addFunction", String.class, FunctionInfo.class);
      _functionRegistryAddFunctionMethod.setAccessible(true);
    } catch (HiveSQLException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(
      Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> topLevelStdUDFClassesAndImplementations) {

    topLevelStdUDFClassesAndImplementations.forEach((topLevelStdUDF, stdUDFImplementations) -> {
      HiveTestStdUDFWrapper wrapper = new HiveTestStdUDFWrapper(topLevelStdUDF, stdUDFImplementations);
      try {
        String functionName =
            ((TopLevelStdUDF) stdUDFImplementations.get(0).getConstructor().newInstance()).getFunctionName();
        _functionRegistryAddFunctionMethod.invoke(_functionRegistry, functionName,
            new FunctionInfo(false, functionName, wrapper));
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e) {
        throw new RuntimeException("Error registering UDF " + topLevelStdUDF.getName() + " with Hive Server", e);
      }
    });
  }

  @Override
  public StdFactory getStdFactory() {
    return _stdFactory;
  }

  @Override
  public SqlFunctionCallGenerator getSqlFunctionCallGenerator() {
    return _sqlFunctionCallGenerator;
  }

  @Override
  public ToPlatformTestOutputConverter getToPlatformTestOutputConverter() {
    return _platformOutputDataConverter;
  }

  @Override
  public void assertFunctionCall(String functionCallString, Object expectedOutputData, Object expectedOutputType) {
    String query = "SELECT " + functionCallString;
    try {
      // Execute the SQL statement and fetch the result
      OperationHandle handle = _client.executeStatement(_sessionHandle, query, null);
      if (handle.hasResultSet()) {
        RowSet rowSet = _client.fetchResults(handle);
        if (rowSet.numRows() > 1 || rowSet.numColumns() > 1) {
          throw new RuntimeException(
              "Expected 1 row and 1 column in query output. Received " + rowSet.numRows() + " rows and "
                  + rowSet.numColumns() + " columns.\nQuery: \"" + query + "\"");
        }
        Object[] row = rowSet.iterator().next();
        Object result = row[0];

        Assert.assertEquals(result, expectedOutputData, "UDF output does not match");
        // Get the output data type and convert them to TypeInfo to compare
        ColumnDescriptor outputColumnDescriptor = _client.getResultSetMetadata(handle).getColumnDescriptors().get(0);
        Assert.assertEquals(TypeInfoUtils.getTypeInfoFromTypeString(outputColumnDescriptor.getTypeName().toLowerCase()),
            TypeInfoUtils.getTypeInfoFromObjectInspector((ObjectInspector) expectedOutputType),
            "UDF output type does not match");
      } else {
        throw new RuntimeException("Query did not return any rows. Query: \"" + query + "\"");
      }
    } catch (HiveSQLException e) {
      throw new RuntimeException("Error running Hive query: \"" + query + "\"", e);
    }
  }
}