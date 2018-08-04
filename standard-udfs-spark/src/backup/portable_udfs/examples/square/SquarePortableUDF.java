package portable_udfs.examples.square;

import portable_udfs.api.Platform;
import portable_udfs.api.data.*;
import portable_udfs.api.types.*;
import portable_udfs.spark.data.SparkPlatform;

public class SquarePortableUDF {
    // to be instantiated through Service loader
    private Platform _platform = new SparkPlatform();

    /**
     * using the input types (schema) we are
     * able to make this method polymorphic
     * by programmatically generating the output schema (type)
     */
    StdType getOutputType(StdType inputType) {
        return inputType;
    }

    StdData eval(StdData input) {
        if (input instanceof StdInteger) {
            int x = ((StdInteger) input).primitive();
            return _platform.createInt(x * x);
        } else if (input instanceof StdLong) {
            long y = ((StdLong) input).primitive();
            return _platform.createLong(y * y);
        } else {
            throw new RuntimeException("Not an int or long");
        }
    }
}
