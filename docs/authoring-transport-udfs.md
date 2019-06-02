# Authoring Transport UDFs

Thank you for using Transport UDFs. See also [documentation index](/README.md#documentation)

This guide takes you through writing a simple UDF in a Gradle project.
For better understanding, it is suggested that you read [Transport UDFs API](transport-udfs-api.md) either prior to or alongside this guide.

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

Let's write a UDF to multiply two integers. Paste the following into `src/main/java/transport/example/Multiply.java` of your Transport UDF module.

```java
package transport.example;

import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class Multiply extends StdUDF2<StdInteger, StdInteger, StdInteger>
    implements TopLevelStdUDF {

  @Override
  public List<String> getInputParameterSignatures() {
    // TODO: Don't use Guava here
    return ImmutableList.of("integer", "integer");
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
    - [MapFromTwoArraysFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/MapFromTwoArraysFunction.java)
    - [StructCreateByIndexFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/StructCreateByIndexFunction.java)
- UDF overloading
    - [`TopLevelStdUDF` Interface](transport-udfs-api.md#toplevelstdudf-interface)
    - [NumericAddFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddFunction.java)
    - [NumericAddIntFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddIntFunction.java)
    - [NumericAddLongFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/NumericAddLongFunction.java)
- Accessing side files in the UDF
    - [`StdUDF` File Processing](transport-udfs-api.md#stdudf-file-processing)
    - [FileLookupFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/main/java/com/linkedin/transport/examples/FileLookupFunction.java)
