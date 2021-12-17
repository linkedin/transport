# The Transport UDFs API

This guide takes you through the various interfaces in the Transport UDFs API that
enable users to express data types, data objects, type signatures, and UDFs.
For information about the project in general please refer to the [documentation index](/README.md#documentation)

## `DataType` Interface
The `DataType` interface is the parent class of all type objects that
are used to describe the schema of the data objects that can be
manipulated by `UDFs`. Sub-interfaces of this interface include
`IntegerType`, `BooleanType`, `LongType`, `StringType`,
`DoubleType`, `FloatType`, `BinaryType`, `ArrayType`,
`MapType`, and `StructType`. Each sub-interface is
defined by methods that are specific to the corresponding type. For
example, `MapType` interface is defined by the two methods shown
below. The `keyType()` and `valueType()` methods can be used to obtain
the key and value types of a `MapType` object.
 
```java
public interface MapType extends DataType {
  DataType keyType();
  DataType valueType();
}
```
Similarly, the rest of the `DataType` sub-types look like the following:

```java
public interface ArrayType extends DataType {
  DataType elementType();
}
```

```java
public interface StructType extends DataType {
  List<? extends DataType> fieldTypes();
}
```
## `ArrayData` Interface
A type-specific interface represents array.

```java
public interface ArrayData<E> extends Iterable<E> {
  int size();
  E get(int idx);
  void add(E e);
}
```

## `MapData` Interface
A type-specific interface represents map.

```java
public interface MapData<K, V> {
  int size();
  K get(K key);
  void put(K key, V value);
  Set<K> keySet();
  Collection<V> values();
  boolean containsKey(K key);
}
```

## `RowData` Interface
A type-specific interface represents structure.

```java
public interface RowData {
  Object getField(int index);
  Object getField(String name);
  void setField(int index, Object value);
  void setField(String name, Object value);
  List<Object> fields();
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
  Type is StringType.
* `"bigint"`: to represent SQL BigInt/Long types. The respective
  Standard Type is LongType.
* `"integer"`: to represent SQL Int type. The respective Standard Type
  is IntegerType.
* `"boolean"`: to represent SQL Boolean type. The respective Standard
  Type is BooleanType.
* `"double"`: to represent SQL Double type. The respective Standard
  Type is DoubleType.
* `"real"`: to represent SQL Real type. The respective Standard
  Type is FloatType.
* `"varbinary"`: to represent SQL Binary type. The respective Standard
  Type is BinaryType.
* `"array(T)"`: to represent SQL Array type, with elements of type
  T. The respective Standard Type is ArrayType.
* `"map(K,V)"`: to represent SQL Map type, with keys of type K and
  values of type V. The respective Standard Type is MapType.
* `"row(f1 T1,.., fn Tn)"`: to represent SQL Struct type with field
  types T1.. Tn with names f1.. fn, respectively. The respective
  Standard Type is RowType.

## The `TypeFactory` Interface
`TypeFactory` is used to create new standard objects of different
types. `TypeFactory` definition is shown
below. As we can see, it contains methods for creating container types (arrays, maps,
and structs) using their type information (encapsulated as
`DataType`), and method to create `DataType` from type signatures.

```java
public interface TypeFactory {
  ArrayData createArray(DataType dataType, int expectedSize);
  ArrayData createArray(DataType dataType);
  MapData createMap(DataType dataType);
  RowData createStruct(List<String> fieldNames, List<DataType> fieldTypes);
  RowData createStruct(List<DataType> fieldTypes);
  RowData createStruct(DataType dataType);
  DataType createDataType(String typeSignature);
}
```

## The `UDF` API
All Transport UDF implementations (expressing UDF logic) extend the
`UDF` abstract class. `UDF` abstract class is the base class for
more specific `UDF` abstract sub-classes that are specific to the
number of UDF arguments, i.e., `UDF0`, `UDF1`, `UDF2`,
etc. `UDF(i)` is an abstract class for UDFs expecting `i`
arguments. Similar to lambda expressions, `UDF(i)` abstract classes
are type-parameterized by the input types and output type of the eval
function. Each class is type parameterized by `(i + 1)` type
parameters: `i` type parameters for the UDF input types, and one type
parameter for the output type.  All types (both input and output
types) must extend the `DataType` interface, i.e., `LongType`,
`BooleanType`, `ArrayType`, etc. Below we list the definition of
`UDF` base class.

```java
public abstract class UDF {
  private TypeFactory _typeFactory;
  public abstract List<String> getInputParameterSignatures();
  public abstract String getOutputParameterSignature();
  public void init(TypeFactory typeFactory) {
    _typeFactory = typeFactory;
  }
  public void processRequiredFiles(String[] localFiles) {
  }
  public boolean[] getNullableArguments() {
    return new boolean[numberOfArguments()];
  }
  protected abstract int numberOfArguments();
  public TypeFactory getTypeFactory() {
    return _typeFactory;
  }
}
```

The `init()` method is called at the UDF initialization time before
processing any records. It sets the `TypeFactory` to be used by the
`UDF` and it can be used to perform necessary UDF
initializations. The methods `getInputParameterSignatures()` and
`getOutputParameterSignature()` are used to specify the input type
signatures and output type signature of the UDF, respectively. Input
type signatures are represented by a list of strings (that are parsed
into `TypeSignature` objects), where each element of the list
represents the signature of a UDF argument. As mentioned above, type
signatures provide the ability to use generic types. The method
`numberOfArguments()` has a default implementation in each `UDF(i)`
sub-interface based on the value of `i`. We discuss
`processRequiredFiles()` and `getNullableArguments()` in the following
subsections.

As an example of `UDF(i)`, we show `UDF2` definition below. The
`eval()` method is the main method of this API, and this is where all
UDF user logic should be expressed. As we can see, it takes two input
types, `I1`, and `I2`, and return an output type `O`, all of which
extend `DataType`. Return values of the `eval()` method can be
instantiated using the `TypeFactory` if necessary.

```java
public abstract class UDF2<I1, I2, O> extends UDF {
  public abstract O eval(I1 arg1, I2 arg2);
  public String[] getRequiredFiles(I1 arg1, I2 arg2) {
    return new String[]{};
  }
  protected final int numberOfArguments() {
    return 2;
  }
}
```

### `UDF` File Processing
The `UDF` API provides a standard way for accessing HDFS files and
processing them in UDFs. The API exposes two file handling methods:
`getRequiredFiles()`, and
`processRequiredFiles()`. `getRequiredFiles()` is implemented by the
`UDF` implementation and returns a list of files that are required
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

## `TopLevelUDF` Interface
`TopLevelUDF` API is an interface that has only two methods:
`getFunctionName()` and `getFunctionDescription()`. It is used as a
means to enable UDF overloading if necessary. Transport UDFs enable
overloading such that UDFs with the same name can have different type
input parameter type signatures. If a UDF does not require
overloading, it simply implements `TopLevelUDF` in addition to
extending `UDF(i)`. That way, the UDF will implement
`getFunctionName()` and `getFunctionDescription()` in addition to
other methods inherited from `UDF(i)`, and hence the UDF definition
becomes "flat". On the other hand, if the UDF requires overloading,
then each overloading class implements an interface that extends
`TopLevelUDF` providing the common name and description of the UDF,
in addition to extending its respective `UDF(i)`. That way, the UDF
name and description is kept in one place (in the interface extending
`TopLevelUDF`), but the overloading-specific information is kept in
separate classes.

## Putting it all together
The example below shows how it becomes so simple to express a UDF by
combining all the APIs above together:

```java
import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class MapFromTwoArraysFunction<K, V> extends UDF2<ArrayData<K>, ArrayData<V>, MapData<K, V>>
        implements TopLevelUDF {

  private DataType _mapType;

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
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    // Note: we create the _mapType once in init() and then reuse it to create MapData objects
    _mapType = getTypeFactory().createDataType(getOutputParameterSignature());
  }

  @Override
  public MapData<K, V> eval(ArrayData<K> a1, ArrayData<V> a2) {
    if (a1.size() != a2.size()) {
      return null;
    }
    MapData<K, V> map = getTypeFactory().createMap(_mapType);
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
    return "Create a map out of two arrays.";
  }
}
```

More examples can be found in the [examples module](../transportable-udfs-examples/transportable-udfs-example-udfs/).
