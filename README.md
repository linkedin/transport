![logo](transportable-udfs-documentation/logo.png)
# Transport UDFs

**Transport UDFs** is an API and framework for writing user-defined
functions (UDFs) that can run across many engines, such as Spark,
Hive, Presto, and process different data formats such as Avro, ORC,
etc. Developers can use the API to implement their UDF logic once as a
"Transportable UDF", and the framework takes care of translating the
Transportable UDF to a native UDF for a specific engine or
format. Currently, Transportable UDFs support producing engine-artifacts
for Spark, Hive, and Presto, and format-artifacts for Avro. Translated
UDFs seem to the target engine as if they were written in that
engine's native API in the first place.

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

