# Using Transport UDFs

This guide describes the procedure for using Transport UDFs in various supported platforms.
For information about the project in general please refer to the [documentation index](/README.md#documentation)

The Transport framework automatically generates UDF artifacts for each supported platform. These artifacts are distinguished by maven classifiers in addition to the original UDF artifact coordinates. Follow the below sections on how to identify the correct artifact/class for your platform and consequently how to use it in the platform.

- [Identifying platform-specific UDF artifacts](#identifying-platform-specific-udf-artifacts)
    - [Platform-specific artifact file](#platform-specific-artifact-file)
    - [Platform-specific UDF class](#platform-specific-udf-class)
- [Using the UDF artifacts](#using-the-udf-artifacts)
    - [Hive](#hive)
    - [Spark](#spark)
    - [Trino](#trino)

## Identifying platform-specific UDF artifacts

### Platform-specific artifact file

As mentioned above, the Transport Plugin will automatically generate artifacts for each platform. Once these artifacts are published to a ivy repository, you can consume them using the corresponding ivy coordinates using the platform name as a maven classifier. E.g. if the UDF has an ivy coordinate `com.linkedin.transport-example:example-udf:1.0.0`, then the coordinate for the platform-specific UDF would be `com.linkedin.transport-example:example-udf:1.0.0?classifier=PLATFORM-NAME` where `PLATFORM-NAME` is `hive`, `trino` or `spark`.

If you are building the UDF project locally, the platform-specific artifacts are built alongside the UDF artifact in the output directory with the platform name as a file suffix. If the built UDF is located at `/path/to/example-udf.ext` then the platform-specific artifact is located at `/path/to/example-udf-PLATFORM-NAME.ext` where `PLATFORM-NAME` is `hive`, `trino` or `spark`.

### Platform-specific UDF class

If the UDF class is `com.linkedin.transport.example.ExampleUDF` then the platform-specific UDF class will be `com.linkedin.transport.example.PLATFORM-NAME.ExampleUDF` where `PLATFORM-NAME` is `hive`, `trino` or `spark`.

## Using the UDF artifacts

### Hive

1. Add the UDF jar to the Hive session  
    For adding the jar from a local file
    ```
    hive (default)> ADD JAR /path/to/example-udf-hive.jar;
    ```
    For adding the jar from an ivy repository
    ```
    hive (default)> ADD JAR ivy://com.linkedin.transport-example:example-udf:1.0.0?classifier=hive;
    ```

2. Register the UDF with the function registry
    ```
    hive (default)> CREATE TEMPORARY FUNCTION example_udf AS 'com.linkedin.transport.example.hive.ExampleUDF';
    ```

3. Call the UDF in a query
    ```
    hive (default)> SELECT example_udf(some_column, 'some_constant');
    ```

### Spark

1. Add the UDF jar to the classpath of the Spark application.  
    If you are launching Spark through the Spark shell, use the `--jars` option to include the local UDF jar file. If you are writing the Spark application using Scala code, use the dependency management solution of your build tool (e.g. Gradle/Maven) to include the UDF's Spark jar as a compile-time dependency.

2. Register the UDF with the function registry  
    ```
    import com.linkedin.transport.example.spark.ExampleUDF
    val exampleUDF = ExampleUDF.register("example_udf")
    ```

3. Call the UDF  
    You can use the UDF either through Spark SQL or the Spark Dataframe API
    - Spark SQL:
        ```scala
        spark.sql("""SELECT example_udf(some_column, 'some_constant')""")
        ```
    - Dataframe API
        ```scala
        dataframe.withColumn("result",
            exampleUDF(col("some_column"), lit("some_constant"))
        )
        ```
        OR
        ```scala
        import org.apache.spark.sql.functions.callUDF
        dataframe.withColumn("result",
            callUDF("example_udf", col("some_column"), lit("some_constant"))
        )
        ```

### Trino

1. Add the UDF to the Trino installation
Unlike Hive and Spark, Trino currently does not allow dynamically loading jar files once the Trino server has started.
In Trino, the jar is deployed to the `plugin` directory.
However, a small patch is required for the Trino engine to recognize the jar as a plugin, since the generated Trino UDFs implement the `SqlScalarFunction` API, which is currently not part of Trino's SPI architecture.
You can find the patch [here](transport-udfs-trino.patch) and apply it before deploying your UDFs jar to the Trino engine.

2. Call the UDF in a query  
    To call the UDF, you will need to use the function name defined in the Transport UDF definition.
    ```
    trino-cli> SELECT example_udf(some_column, 'some_constant');
    ```
