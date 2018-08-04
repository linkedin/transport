package portable_udfs.examples.nestedsum;

import portable_udfs.api.Platform;
import portable_udfs.api.data.*;
import portable_udfs.api.types.*;
import portable_udfs.spark.data.SparkPlatform;

import java.io.Serializable;

/**
 * computes a sum across all numeric fields in the input record
 */
class NestedSumPortableUDF implements Serializable {
    // to be instantiated through Service loader
    private Platform _platform = new SparkPlatform();

    /**
     * determines the output schema, given input schema(s)
     * Using this our method can be made polymorphic
     *
     * @param ignored
     * @return
     */
    StdType getOutputType(StdType ignored) {
        return _platform.createIntType();
    }


    StdInteger eval(StdData input) {
        if (input == null) {
            return _platform.createInt(0);
        }

        int r = recursiveSum(input);
        return _platform.createInt(r);
    }

    private static int recursiveSum(StdData input) {
        if (input == null) {
            return 0;
        }
        int r = 0;
        final StdType type = input.getType();
        if (type instanceof StdStructType) {
            final StdStruct s = input.asStruct();
            for (StdData f : s.fields()) {
                r += recursiveSum(f);
            }
        } else if (type instanceof StdArrayType) {
            final StdArray s = input.asArray();
            for (StdData e : s) {
                r += recursiveSum(e);
            }
        } else if (type instanceof StdMapType) {
            final StdMap s = input.asMap();
            for (StdData k : s.keys()) {
                r += recursiveSum(s.get(k));
            }
        } else if (type instanceof StdIntegerType) {
            // these apis could be simplified
            r += ((StdInteger) input).primitive();

        } else if (type instanceof StdLongType) {
            // these apis could be simplified
            r += ((StdLong) input).primitive();

        } else {
            //System.out.println("Skipping... " + type.getClass().getName());
        }
        return r;
    }
}
