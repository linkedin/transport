# Authoring Transport UDFs

This guide takes you through writing a simple UDF in a Gradle project.
For the API documentation, please refer to the [Transport UDFs API](transport-udfs-api.md).
For information about the project in general please refer to the [documentation index](/README.md#documentation)

## Adding Dependency

Add the following to the `build.gradle` of the Gradle module in which you wish to develop your UDF.
Get the latest version of "com.linkedin.transport.plugin" plugin from [Gradle Plugin Portal](https://plugins.gradle.org/plugin/com.linkedin.transport.plugin)

```Gradle
plugins {
    id "java"
    id "com.linkedin.transport.plugin" version "TODO"
}
```

<details>
    <summary>For advanced users, if you need to use the traditional way of configuring Gradle plugins</summary>

```Gradle
buildscript {
    repositories {
        maven { url "https://plugins.gradle.org/m2/" }
    }

    dependencies {
        classpath "com.linkedin.transport:transportable-udfs-plugin:TODO"
    }
}

apply plugin: "java"
apply plugin: "com.linkedin.transport.plugin"
```
</details>

## Writing the UDF

Let's write a UDF to multiply two integers. Paste the following into `src/main/java/transport/example/Multiply.java` inside your Transport UDF module.

```java
package transport.example;

import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.Arrays;
import java.util.List;


public class Multiply extends StdUDF2<StdInteger, StdInteger, StdInteger>
    implements TopLevelStdUDF {

  @Override
  public List<String> getInputParameterSignatures() {
    return Arrays.asList("integer", "integer");
  }

  @Override
  public String getOutputParameterSignature() {
    return "integer";
  }

  @Override
  public StdInteger eval(StdInteger first, StdInteger second) {
    return getStdFactory().createInteger(first.get() * second.get());
  }

  @Override
  public String getFunctionName() {
    return "multiply";
  }

  @Override
  public String getFunctionDescription() {
    return "Multiplies two integers";
  }
}
```

In the example above, `StdInteger` is an interface that provides high-level integer operations to its objects.
Depending on the engine where this UDF is executed, this interface is implemented differently to deal with native data types used by that engine.
`getStdFactory()` is a method used to create objects that conform to a given data type.
`StdUDF2` is an abstract class to express a UDF that takes two parameters.
It is parametrized by the UDF input types and the UDF output type.
For a more detailed documentation of the API usage, see [Transport UDFs API](transport-udfs-api.md).

## Building the UDF

Run `gradle build` from the terminal (use `./gradlew build` if you are using the Gradle wrapper).
Now you should be able to see the UDF jar as well as platform-specific artifacts being built in the `build/libs` folder inside the module.
For instructions on how to use these artifacts, see [Using Transport UDFs](using-transport-udfs.md).

## More Complex UDF Examples

- Complex types (maps, arrays, structs) with generics
    - Transport UDFs can accept/return complex types. E.g. the input parameter signature for a UDF which accepts a list of integers would be `array(integer)`. You can also use generic types to derive types at runtime. E.g. you can accept a generic type `K` as input and return an `array(K)` in which case the type of `K` will be derived at query compile time.
    - Example: [MapFromTwoArraysFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/MapFromTwoArraysFunction.java) and [StructCreateByNameFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/StructCreateByNameFunction.java)
- UDF overloading
    - You can define multiple Transport UDFs which share the same name but accept different input parameter signatures using the [`TopLevelStdUDF` Interface](transport-udfs-api.md#toplevelstdudf-interface).
    - Example:  [NumericAddFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddFunction.java) is the interface that defines the UDF name which is then shared by two of its overloadings viz. [NumericAddIntFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddIntFunction.java) and  [NumericAddLongFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddLongFunction.java).
- Accessing HDFS files in the UDF
    - Transport UDF API provides a standard way to access and process HDFS files in the UDFs (details - [`StdUDF` File Processing](transport-udfs-api.md#stdudf-file-processing)). 
    - Example: One common usage of this feature is to build hash tables (or bitmaps) from files that can then be used as lookup tables inside the UDF. Such usage is demonstrated in [FileLookupFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/FileLookupFunction.java).
