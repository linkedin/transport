![logo](transportable-udfs-documentation/logo.png)
# Transport UDFs

**Transport** is a framework for writing performant user-defined
functions (UDFs) that are portable across a variety of engines
including [Apache Spark](https://spark.apache.org/), [Apache Hive](https://hive.apache.org/), and
[Presto](https://prestodb.io/). UDFs written with Transport are also
capable of directly processing data stored in formats such as
Avro. With Transport, developers only need to implement their UDF
logic once using the Transport API. Transport then takes care of
translating the UDF to a native version targeted at a specific engine
or format. Currently, the Transport framework is capable of generating
engine-artifacts for Spark, Hive, and Presto, and format-artifacts for
Avro. Further details on Transport can be found in this [LinkedIn Engineering blog post](https://engineering.linkedin.com/blog/2018/11/using-translatable-portable-UDFs).

## Example
This is an example of what it takes to define a Transportable UDF. It is
simple and quite self-explanatory.

```java
public class MapFromTwoArraysFunction extends StdUDF2<StdArray, StdArray, StdMap> implements TopLevelStdUDF {

  private StdType _mapType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "array(K)",
        "array(V)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "map(K,V)";
  }

  @Override
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _mapType = getStdFactory().createStdType(getOutputParameterSignature());
  }

  @Override
  public StdMap eval(StdArray a1, StdArray a2) {
    if (a1.size() != a2.size()) {
      return null;
    }
    StdMap map = getStdFactory().createMap(_mapType);
    for (int i = 0; i < a1.size(); i++) {
      map.put(a1.get(i), a2.get(i));
    }
    return map;
  }

  @Override
  public String getFunctionName() {
    return "map_from_two_arrays";
  }

  @Override
  public String getFunctionDescription() {
    return "A function to create a map out of two arrays";
  }
}
```

In the example above, `StdMap` and `StdArray` are interfaces that
provide high-level map and array operations to their
objects. Depending on the engine where this UDF is executed, those
interfaces are implemented differently to deal with native data types
used by that engine. `getStdFactory()` is a method used to create
objects that conform to a given data type (such as a map whose keys
are of the type of elements in the first array and values are of the
type of elements in the second array). `StdUDF2` is an abstract class
to express a UDF that takes two parameters. It is parametrized by the
UDF input types and the UDF output type. A more detailed documentation
of the API usage can be found in the [Transportable UDF API user
guide](transportable-udfs-documentation/user-guide.md).

## How to Build
Clone the repository:
```bash
git clone https://github.com/linkedin/transport.git
```
Change directory to `transport`:
```bash
cd transport
```

Build:
```bash
gradle build
```

This project requires Java `1.8.0_151` or higher.
Either set `JAVA_HOME` to the home of an appropriate version and use `gradle build` as described above, or use the `gradlew` and set `org.gradle.java.home` to the Java home
of an appropriate version:
```bash
./gradlew -Dorg.gradle.java.home=/path/to/java/home build
```
## License

    BSD 2-CLAUSE LICENSE

    Copyright 2018 LinkedIn Corporation.
    All Rights Reserved.
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

