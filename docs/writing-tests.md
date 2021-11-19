# Writing Tests for Transport UDFs

This guide takes you through writing tests for Transport UDFs using the unified testing framework. For information about the project in general please refer to the [documentation index](/README.md#documentation)

This framework allows you to write your tests using a standard API and the framework will execute these test on all the supported platforms.
Here we will be writing tests for the Multiply UDF which we developed in [Authoring Transport UDFs](authoring-transport-udfs.md).

## Writing the Test

Paste the following into `src/test/java/transport/example/TestMultiply.java` inside your Transport UDF module.

```java
package transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.test.AbstractUDFTest;
import com.linkedin.transport.test.spi.Tester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestMultiply extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(TestMultiply.class,
        ImmutableList.of(TestMultiply.class));
  }

  @Test
  public void testMultiply() {
    StdTester tester = getTester();
    tester.check(functionCall("multiply", 2, 3), 6, "integer");
    tester.check(functionCall("multiply", -2, 3), -6, "integer");
  }

  @Test
  public void testMultiply() {
    StdTester tester = getTester();
    tester.check(functionCall("multiply", Integer.MAX_INT, Integer.MAX_INT), null, "integer");
  }
}
```

In the example above, `AbstractStdUDFTest` is an abstract class which contains helper methods for testing UDFs.
`getTopLevelStdUDFClassesAndImplementations()` method provides information regarding the UDFs required for running the tests in this class.
`StdTester` is an interface that provides high-level testing capabilities to its objects.
Depending on the engine where this test is executed, this interface is implemented differently to deal with test methodology and native data types used by that engine.
The first argument to `StdTester.check()` is a function call which consists of the name of the function to be tested followed by input arguments.
The second and third arguments are the expected output and expected output type respectively.

## Running the Test

Running `gradle build` from the terminal will build the UDF artifacts as well as run the tests for all platforms (use `./gradlew build` if you are using the Gradle wrapper).

## More Complex Tests

To figure out Java equivalents of StdData types to be used in the test framework, please see the documentation of [AbstractStdUDFTest](../transportable-udfs-test/transportable-udfs-test-api/src/main/java/com/linkedin/transport/test/AbstractStdUDFTest.java).
- Tests with complex types (maps, arrays, structs)
    - [TestMapFromTwoArraysFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/test/java/com/linkedin/transport/examples/TestMapFromTwoArraysFunction.java)
    - [TestStructCreateByIndexFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/test/java/com/linkedin/transport/examples/TestStructCreateByIndexFunction.java)
- Testing overloaded UDFs
    - [TestNumericAddFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/test/java/com/linkedin/transport/examples/TestNumericAddFunction.java)
- Testing UDFs which access HDFS files
    - [TestFileLookupFunction](../transportable-udfs-examples/transportable-udfs-example-udfs/src/test/java/com/linkedin/transport/examples/TestFileLookupFunction.java)
