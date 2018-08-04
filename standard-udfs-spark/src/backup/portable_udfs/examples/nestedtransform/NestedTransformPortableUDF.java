package portable_udfs.examples.nestedtransform;

import portable_udfs.api.Platform;
import portable_udfs.api.data.*;
import portable_udfs.api.types.*;
import portable_udfs.spark.data.SparkPlatform;

/**
 * Updates each element in a nested record
 * Strings are append to with 'modified'
 * numeric fields are incremented;
 */
class NestedTransformPortableUDF {
    // to be instantiated through Service loader
    private Platform _platform = new SparkPlatform();

    /**
     * The UDF is completely polymorphic,
     * it will work for any type (struct/array/map etc..)
     * @param stdType
     * @return
     */
    StdType getOutputType(StdType stdType) {
        // same input and output types
        return stdType;
    }

    StdData eval(StdData input) {
        if (input == null)
            return null;

        final StdData output = recursiveTransform(input);
        return output;
    }

    private StdData recursiveTransform(StdData input) {
        final StdType type = input.getType();
        if (type instanceof StdStructType) {
            final StdStruct s = input.asStruct();
            for (int i = 0; i < s.size(); i++) {
                StdData fi = s.get(i);
                StdData fo = recursiveTransform(fi);
                // This seems like an Anti pattern, we should actually not modify input data
                // but create new data, but for prototype's purposes i Åt is OK.
                s.update(i, fo);
            }
            return s;

        } else if (type instanceof StdArrayType) {
            final StdArray s = input.asArray();
            for (int i = 0; i < s.size(); i++) {
                StdData fi = s.get(i);
                StdData fo = recursiveTransform(fi);
                // as above
                s.set(i, fo);
            }
            return s;

        } else if (type instanceof StdMapType) {
            final StdMap s = input.asMap();
            for (StdData k : s.keys()) {
                StdData fo = recursiveTransform(s.get(k));
                // as above
                s.put(k, fo);
            }
            return s;

        } else if (type instanceof StdIntegerType) {
            // these apis could be simplified
            int v = ((StdInteger) input).primitive();
            // int transformation is simple increment
            return _platform.createInt(v + 1);

        } else if (type instanceof StdLongType) {
            // these apis could be simplified
            long v = ((StdLong)input).primitive();
            return _platform.createLong(v + 1);

        } else if (type instanceof StdStringType) {
            String v = ((StdString) input).asString();
            return _platform.createString(v + ".modified");

        } else {
            System.out.println("Skipping... " + type.getClass().getName());
            return input;
        }
    }
}
