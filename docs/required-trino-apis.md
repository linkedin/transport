# Why is modifying the Trino SPI interface necessary for Transport to work?
Transport requires applying this [patch](transport-udf-trino.patch) before being able to use Transport with Trino.
This patch makes some of the internal UDF classes be visible at the SPI layer. 
Below we explain why some Transport APIs cannot leverage the APIs offered by the [public SPI UDF model](https://trino.io/docs/current/develop/functions.html).

## [init() method](https://github.com/linkedin/transport/blob/09a89508296a2491f43cc8866d47952c911313ab/transportable-udfs-api/src/main/java/com/linkedin/transport/api/udf/StdUDF.java#L45) is hard to implement on top of Trino-SPI
The `init()` method allows users to perform necessary initializations for their Transport UDFs.
Conceptually, it is called once at the UDF initialization time before processing any records. It sets the [StdFactory](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-api/src/main/java/com/linkedin/transport/api/StdFactory.java#L36) to be used by the
`StdUDF`, and can be used to create Java types that correspond to the type signatures provided by the user.
Due to the lack of a similar API in the SPI UDF model, in the current approach, `init()` is called inside
overridden [specialize()](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/StdUdfWrapper.java#L136) method in [StdUdfWrapper](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/StdUdfWrapper.java#L72)
which extends [SqlScalarFunction](https://github.com/trinodb/trino/blob/54d8154037dfe5f6f65709dbafeb92f5506af2ac/core/trino-main/src/main/java/io/trino/metadata/SqlScalarFunction.java#L18).
That way, we can implement the
 semantics of init():

## [TrinoFactory](https://github.com/linkedin/transport/blob/92dfbbfd989367418bdd14f9ac4cc2bcf1e7c777/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/TrinoFactory.java#L52) requires `FunctionBinding` and `FunctionDependencies` which are not provided by the Trino-SPI
[TrinoFactory](https://github.com/linkedin/transport/blob/92dfbbfd989367418bdd14f9ac4cc2bcf1e7c777/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/TrinoFactory.java#L52)
is designed to convert Transport data types and their required operators (e.g., the equals function of map keys)
to Trino native data type and operators. This serves implementing the 
 [createStdType()](https://github.com/linkedin/transport/blob/92dfbbfd989367418bdd14f9ac4cc2bcf1e7c777/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/TrinoFactory.java#L139)
in [StdFactory](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-api/src/main/java/com/linkedin/transport/api/StdFactory.java#L36), which is a standard
API across all engines. 
The TrinoFactory factory implementaiton of the StdFactory requires Trino classes [FunctionBinding](https://github.com/trinodb/trino/blob/54d8154037dfe5f6f65709dbafeb92f5506af2ac/core/trino-main/src/main/java/io/trino/metadata/FunctionBinding.java#L26)
and [FunctionDependencies](https://github.com/trinodb/trino/blob/0b1a1b9fa036bac132c80c990166096abc1b2552/core/trino-main/src/main/java/io/trino/metadata/FunctionDependencies.java#L47)
to implement its basic functionality; however those classes are not provided by the Trino SPI UDF model.
In the current integration approach, TrinoFactory is initialized inside the overridden [specialize()](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/StdUdfWrapper.java#L136) method
in [StdUdfWrapper](https://github.com/linkedin/transport/blob/d919f96dc1485ccb8b58e4faed3a5589a5966236/transportable-udfs-trino/src/main/java/com/linkedin/transport/trino/StdUdfWrapper.java#L72)
which extends [SqlScalarFunction](https://github.com/trinodb/trino/blob/54d8154037dfe5f6f65709dbafeb92f5506af2ac/core/trino-main/src/main/java/io/trino/metadata/SqlScalarFunction.java#L18)
, and gets access to those two classes from there.

The snippet below shows how the Transport Trino implementation uses the `SqlScalarFunction#specialize()` method
to call `StdUF#init()` and pass the `FunctionDependencies` and `FunctionBinding` objects to the TrinoFactory.
```java
@Override
public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies) {
  StdFactory stdFactory = new TrinoFactory(functionBinding, functionDependencies);
  StdUDF stdUDF = getStdUDF();
  stdUDF.init(stdFactory);
  ...
}
```

