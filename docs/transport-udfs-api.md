# The Transport UDFs API

This guide takes you through the various interfaces in the Transport UDFs API that
enable users to express data types, data objects, type signatures, and UDFs.
For information about the project in general please refer to the [documentation index](/README.md#documentation)

## `StdType` Interface
The `StdType` interface is the parent class of all type objects that
are used to describe the schema of the data objects that can be
manipulated by `StdUDFs`. Sub-interfaces of this interface include
`StdIntegerType`, `StdBooleanType`, `StdLongType`, `StdStringType`,
`StdDoubleType`, `StdFloatType`, `StdBinaryType`, `StdArrayType`,
`StdMapType`, and `StdStructType`. Each sub-interface is
defined by methods that are specific to the corresponding type. For
example, `StdMapType` interface is defined by the two methods shown
below. The `keyType()` and `valueType()` methods can be used to obtain
the key and value types of a `StdMapType` object.
 
```java
public interface StdMapType extends StdType {
  StdType keyType();
  StdType valueType();
}
```
Similarly, the rest of the `StdType` sub-types look like the following:

```java
public interface StdArrayType extends StdType {
  StdType elementType();
}
```

```java
public interface StdStructType extends StdType {
  List<? extends StdType> fieldTypes();
}
```
## `StdData` Interface
`StdData` is a top-level interface for describing data that can be
manipulated by Transport UDFs. As a top-level interface, `StdData`
itself does not contain any methods. A number of type-specific
interfaces extend `StdData`, such as `StdInteger`, `StdLong`,
`StdBoolean`, `StdString`, `StdDouble`, `StdFloat`, `StdBinary`,
`StdArray`, `StdMap`, and `StdStruct` to represent `INTEGER`,
`LONG`, `BOOLEAN`, `VARCHAR`, `DOUBLE`, `REAL`, `VARBINARY`, `ARRAY`, `MAP`,
and `STRUCT` SQL types respectively. Each of those interfaces exposes
operations that can manipulate that type of data. For example,
`StdMap` interface is defined by the following methods:

```java
public interface StdMap extends StdData {
  int size();
  StdData get(StdData key);
  void put(StdData key, StdData value);
  Set<StdData> keySet();
  Collection<StdData> values();
  boolean containsKey(StdData key);
}
```

The `StdArray` interface is defined by the following methods:

```java
public interface StdArray extends StdData, Iterable<StdData> {
  int size();
  StdData get(int idx);
  void add(StdData e);
}
```

The `StdStruct` interface is defined by the following methods:

```java
public interface StdStruct extends StdData {
  StdData getField(int index);
  StdData getField(String name);
  void setField(int index, StdData value);
  void setField(String name, StdData value);
  List<StdData> fields();
}
```

As an example of primitive types, the `StdString` interface is defined
as follows. Other primitive types follow the same pattern:

```java
public interface StdString extends StdData {
  String get();
}
```

## Type Signatures
The Transport UDFs framework provides a way to declare what data types
a UDF expects through type signatures. Type signatures in Transport
UDFs support generic types, including type verification and
inference. For example, a user can state that a UDF expects two
arguments, one of type `"array(K)"` and the other of type `"array(V)"`
and returns an object of type `"map(K,V)"`. While type signatures are
exposed to the end user as strings, internally, `TypeSignature` class
is used to represent type signature information. Type signatures can
represent types with type parameters, which can be both concrete or
generic. The following are type signature strings along with their
definition:

* `"varchar"`: to represent SQL Varchar type. The respective Standard
  Type is StdString.
* `"bigint"`: to represent SQL BigInt/Long types. The respective
  Standard Type is StdLong.
* `"integer"`: to represent SQL Int type. The respective Standard Type
  is StdInteger.
* `"boolean"`: to represent SQL Boolean type. The respective Standard
  Type is StdBoolean.
* `"double"`: to represent SQL Double type. The respective Standard
  Type is StdDouble.
* `"real"`: to represent SQL Real type. The respective Standard
  Type is StdFloat.
* `"varbinary"`: to represent SQL Binary type. The respective Standard
  Type is StdBinary.
* `"array(T)"`: to represent SQL Array type, with elements of type
  T. The respective Standard Type is StdArray.
* `"map(K,V)"`: to represent SQL Map type, with keys of type K and
  values of type V. The respective Standard Type is StdMap.
* `"row(f1 T1,.., fn Tn)"`: to represent SQL Struct type with field
  types T1.. Tn with names f1.. fn, respectively. The respective
  Standard Type is StdStruct.

## The `StdFactory` Interface
`StdFactory` is used to create new standard objects of different
types. Similar to `StdData` and `StdType`, `StdFactory`
implementations hide all the platforms-specific details from the user
and expose only standard methods. `StdFactory` definition is shown
below. As we can see, it contains methods for creating primitive types
using their primitive values, creating container types (arrays, maps,
and structs) using their type information (encapsulated as
`StdTypes`), and method to create `StdTypes` from type signatures.

```java
public interface StdFactory {
  StdInteger createInteger(int value);
  StdLong createLong(long value);
  StdBoolean createBoolean(boolean value);
  StdString createString(String value);
  StdDouble createDouble(double value);
  StdFloat createFloat(float value);
  StdBinary createBinary(ByteBuffer value);
  StdArray createArray(StdType dataType, int expectedSize);
  StdArray createArray(StdType dataType);
  StdMap createMap(StdType dataType);
  StdStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes);
  StdStruct createStruct(List<StdType> fieldTypes);
  StdStruct createStruct(StdType dataType);
  StdType createStdType(String typeSignature);
}
```

## The `StdUDF` API
All Transport UDF implementations (expressing UDF logic) extend the
`StdUDF` abstract class. `StdUDF` abstract class is the base class for
more specific `StdUDF` abstract sub-classes that are specific to the
number of UDF arguments, i.e., `StdUDF0`, `StdUDF1`, `StdUDF2`,
etc. `StdUDF(i)` is an abstract class for UDFs expecting `i`
arguments. Similar to lambda expressions, `StdUDF(i)` abstract classes
are type-parameterized by the input types and output type of the eval
function. Each class is type parameterized by `(i + 1)` type
parameters: `i` type parameters for the UDF input types, and one type
parameter for the output type.  All types (both input and output
types) must extend the `StdData` interface, i.e., `StdLong`,
`StdBoolean`, `StdArray`, etc. Below we list the definition of
`StdUDF` base class.

```java
public abstract class StdUDF {
  private StdFactory _stdFactory;
  public abstract List<String> getInputParameterSignatures();
  public abstract String getOutputParameterSignature();
  public void init(StdFactory typeFactory) {
    _stdFactory = typeFactory;
  }
  public void processRequiredFiles(String[] localFiles) {
  }
  public boolean[] getNullableArguments() {
    return new boolean[numberOfArguments()];
  }
  protected abstract int numberOfArguments();
  public StdFactory getStdFactory() {
    return _stdFactory;
  }
}
```

The `init()` method is called at the UDF initialization time before
processing any records. It sets the `StdFactory` to be used by the
`StdUDF` and it can be used to perform necessary UDF
initializations. The methods `getInputParameterSignatures()` and
`getOutputParameterSignature()` are used to specify the input type
signatures and output type signature of the UDF, respectively. Input
type signatures are represented by a list of strings (that are parsed
into `TypeSignature` objects), where each element of the list
represents the signature of a UDF argument. As mentioned above, type
signatures provide the ability to use generic types. The method
`numberOfArguments()` has a default implementation in each `StdUDF(i)`
sub-interface based on the value of `i`. We discuss
`processRequiredFiles()` and `getNullableArguments()` in the following
subsections.

As an example of `StdUDF(i)`, we show `StdUDF2` definition below. The
`eval()` method is the main method of this API, and this is where all
UDF user logic should be expressed. As we can see, it takes two input
types, `I1`, and `I2`, and return an output type `O`, all of which
extend `StdData`. Return values of the `eval()` method can be
instantiated using the `StdFactory` if necessary.

```java
public abstract class StdUDF2<I1 extends StdData, I2 extends StdData, O extends StdData> extends StdUDF {
  public abstract O eval(I1 arg1, I2 arg2);
  public String[] getRequiredFiles(I1 arg1, I2 arg2) {
    return new String[]{};
  }
  protected final int numberOfArguments() {
    return 2;
  }
}
```

### `StdUDF` File Processing
The `StdUDF` API provides a standard way for accessing HDFS files and
processing them in UDFs. The API exposes two file handling methods:
`getRequiredFiles()`, and
`processRequiredFiles()`. `getRequiredFiles()` is implemented by the
`StdUDF` implementation and returns a list of files that are required
to be localized to the workers. `processRequiredFiles()` expects a
list of paths of the localized files corresponding to the list of
files requested in `getRequiredFiles()`, and hence this list is passed
to the method implementation and is available to the user to parse
those files. Therefore, `processRequiredFiles()` provides a mechanism
for users to process the localized files before the execution of the
`eval()` method. One common usage is to build hash tables (or bitmaps)
from the files that can be used as lookup tables in the `eval()`
method.

### Nullable Arguments
Nullable arguments are arguments that can receive a null value. When
an argument is declared nullable, the user can implement logic to
specify the behavior of the UDF once an argument takes a null
value. If an argument is non-nullable, the UDF returns null by default
if that argument is null. Users can specify that behavior by
implementing the `getNullableArguments()` method. The default
implementation is to set all arguments to be non-nullable.

## `TopLevelStdUDF` Interface
`TopLevelStdUDF` API is an interface that has only two methods:
`getFunctionName()` and `getFunctionDescription()`. It is used as a
means to enable UDF overloading if necessary. Transport UDFs enable
overloading such that UDFs with the same name can have different type
input parameter type signatures. If a UDF does not require
overloading, it simply implements `TopLevelStdUDF` in addition to
extending `StdUDF(i)`. That way, the UDF will implement
`getFunctionName()` and `getFunctionDescription()` in addition to
other methods inherited from `StdUDF(i)`, and hence the UDF definition
becomes "flat". On the other hand, if the UDF requires overloading,
then each overloading class implements an interface that extends
`TopLevelStdUDF` providing the common name and description of the UDF,
in addition to extending its respective `StdUDF(i)`. That way, the UDF
name and description is kept in one place (in the interface extending
`TopLevelStdUDF`), but the overloading-specific information is kept in
separate classes.

## Putting it all together
The example below shows how it becomes so simple to express a UDF by
combining all the APIs above together:

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
  public void init(StdFactory typeFactory) {
    super.init(typeFactory);
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

More examples can be found in the [examples module](../transportable-udfs-examples/transportable-udfs-example-udfs/).
