/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.hive.types.objectinspector;

import java.sql.Date;
import java.sql.Timestamp;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


/**
 * CacheablePrimitiveObjectInspectorConverter.
 *
 */
public class CacheablePrimitiveObjectInspectorConverter {

  /**
   * A converter for the byte type.
   */
  public static class BooleanConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableBooleanObjectInspector outputOI;

    public BooleanConverter(PrimitiveObjectInspector inputOI,
        SettableBooleanObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(false);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getBoolean(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the byte type.
   */
  public static class ByteConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableByteObjectInspector outputOI;

    public ByteConverter(PrimitiveObjectInspector inputOI,
        SettableByteObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create((byte) 0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getByte(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the short type.
   */
  public static class ShortConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableShortObjectInspector outputOI;

    public ShortConverter(PrimitiveObjectInspector inputOI,
        SettableShortObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create((short) 0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getShort(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the int type.
   */
  public static class IntConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableIntObjectInspector outputOI;

    public IntConverter(PrimitiveObjectInspector inputOI,
        SettableIntObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getInt(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the long type.
   */
  public static class LongConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableLongObjectInspector outputOI;

    public LongConverter(PrimitiveObjectInspector inputOI,
        SettableLongObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getLong(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the float type.
   */
  public static class FloatConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableFloatObjectInspector outputOI;

    public FloatConverter(PrimitiveObjectInspector inputOI,
        SettableFloatObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getFloat(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  /**
   * A converter for the double type.
   */
  public static class DoubleConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableDoubleObjectInspector outputOI;

    public DoubleConverter(PrimitiveObjectInspector inputOI,
        SettableDoubleObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(0);
      try {
        return outputOI.set(r, PrimitiveObjectInspectorUtils.getDouble(input,
            inputOI));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  public static class DateConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableDateObjectInspector outputOI;

    public DateConverter(PrimitiveObjectInspector inputOI,
        SettableDateObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(new Date(0));
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getDate(input,
          inputOI));
    }
  }

  public static class TimestampConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableTimestampObjectInspector outputOI;

    public TimestampConverter(PrimitiveObjectInspector inputOI,
        SettableTimestampObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(new Timestamp(0));
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getTimestamp(input,
          inputOI));
    }
  }

  public static class HiveDecimalConverter implements Converter {

    PrimitiveObjectInspector inputOI;
    SettableHiveDecimalObjectInspector outputOI;

    public HiveDecimalConverter(PrimitiveObjectInspector inputOI,
        SettableHiveDecimalObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(HiveDecimal.ZERO);
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getHiveDecimal(input, inputOI));
    }
  }

  public static class BinaryConverter implements Converter {

    PrimitiveObjectInspector inputOI;
    SettableBinaryObjectInspector outputOI;

    public BinaryConverter(PrimitiveObjectInspector inputOI,
        SettableBinaryObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object r = outputOI.create(new byte[]{});
      return outputOI.set(r, PrimitiveObjectInspectorUtils.getBinary(input,
          inputOI));
    }
  }

  /**
   * A helper class to convert any primitive to Text.
   */
  public static class TextConverter implements Converter {
    private static byte[] trueBytes = {'T', 'R', 'U', 'E'};
    private static byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};
    private final PrimitiveObjectInspector inputOI;
    private final ByteStream.Output out = new ByteStream.Output();

    public TextConverter(PrimitiveObjectInspector inputOI) {
      // The output ObjectInspector is writableStringObjectInspector.
      this.inputOI = inputOI;
    }

    public Text convert(Object input) {
      if (input == null) {
        return null;
      }
      Text t = new Text();

      switch (inputOI.getPrimitiveCategory()) {
        case VOID:
          return null;
        case BOOLEAN:
          t.set(((BooleanObjectInspector) inputOI).get(input) ? trueBytes
              : falseBytes);
          return t;
        case BYTE:
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((ByteObjectInspector) inputOI).get(input));
          t.set(out.getData(), 0, out.getLength());
          return t;
        case SHORT:
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((ShortObjectInspector) inputOI).get(input));
          t.set(out.getData(), 0, out.getLength());
          return t;
        case INT:
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((IntObjectInspector) inputOI).get(input));
          t.set(out.getData(), 0, out.getLength());
          return t;
        case LONG:
          out.reset();
          LazyLong.writeUTF8NoException(out, ((LongObjectInspector) inputOI).get(input));
          t.set(out.getData(), 0, out.getLength());
          return t;
        case FLOAT:
          t.set(String.valueOf(((FloatObjectInspector) inputOI).get(input)));
          return t;
        case DOUBLE:
          t.set(String.valueOf(((DoubleObjectInspector) inputOI).get(input)));
          return t;
        case STRING:
          if (inputOI.preferWritable()) {
            t.set(((StringObjectInspector) inputOI).getPrimitiveWritableObject(input));
          } else {
            t.set(((StringObjectInspector) inputOI).getPrimitiveJavaObject(input));
          }
          return t;
        case CHAR:
          // when converting from char, the value should be stripped of any trailing spaces.
          if (inputOI.preferWritable()) {
            // char text value is already stripped of trailing space
            t.set(((HiveCharObjectInspector) inputOI).getPrimitiveWritableObject(input)
                .getStrippedValue());
          } else {
            t.set(((HiveCharObjectInspector) inputOI).getPrimitiveJavaObject(input).getStrippedValue());
          }
          return t;
        case VARCHAR:
          if (inputOI.preferWritable()) {
            t.set(((HiveVarcharObjectInspector) inputOI).getPrimitiveWritableObject(input)
                .toString());
          } else {
            t.set(((HiveVarcharObjectInspector) inputOI).getPrimitiveJavaObject(input).toString());
          }
          return t;
        case DATE:
          t.set(((DateObjectInspector) inputOI).getPrimitiveWritableObject(input).toString());
          return t;
        case TIMESTAMP:
          t.set(((TimestampObjectInspector) inputOI)
              .getPrimitiveWritableObject(input).toString());
          return t;
        case BINARY:
          BinaryObjectInspector binaryOI = (BinaryObjectInspector) inputOI;
          if (binaryOI.preferWritable()) {
            BytesWritable bytes = binaryOI.getPrimitiveWritableObject(input);
            t.set(bytes.getBytes(), 0, bytes.getLength());
          } else {
            t.set(binaryOI.getPrimitiveJavaObject(input));
          }
          return t;
        case DECIMAL:
          t.set(((HiveDecimalObjectInspector) inputOI).getPrimitiveWritableObject(input).toString());
          return t;
        default:
          throw new RuntimeException("Hive 2 Internal error: type = " + inputOI.getTypeName());
      }
    }
  }

  /**
   * A helper class to convert any primitive to String.
   */
  public static class StringConverter implements Converter {
    PrimitiveObjectInspector inputOI;

    public StringConverter(PrimitiveObjectInspector inputOI) {
      // The output ObjectInspector is writableStringObjectInspector.
      this.inputOI = inputOI;
    }

    @Override
    public Object convert(Object input) {
      return PrimitiveObjectInspectorUtils.getString(input, inputOI);
    }
  }

  public static class HiveVarcharConverter implements Converter {

    PrimitiveObjectInspector inputOI;
    SettableHiveVarcharObjectInspector outputOI;

    public HiveVarcharConverter(PrimitiveObjectInspector inputOI,
        SettableHiveVarcharObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      // unfortunately we seem to get instances of varchar object inspectors without params
      // when an old-style UDF has an evaluate() method with varchar arguments.
      // If we disallow varchar in old-style UDFs and only allow GenericUDFs to be defined
      // with varchar arguments, then we might be able to enforce this properly.
      //if (typeParams == null) {
      //  throw new RuntimeException("varchar type used without type params");
      //}
      HiveVarcharWritable hc = new HiveVarcharWritable();
      switch (inputOI.getPrimitiveCategory()) {
        case BOOLEAN:
          return outputOI.set(hc,
              ((BooleanObjectInspector) inputOI).get(input)
                  ? new HiveVarchar("TRUE", -1) : new HiveVarchar("FALSE", -1));
        default:
          return outputOI.set(hc, PrimitiveObjectInspectorUtils.getHiveVarchar(input, inputOI));
      }
    }
  }

  public static class HiveCharConverter implements Converter {
    PrimitiveObjectInspector inputOI;
    SettableHiveCharObjectInspector outputOI;

    public HiveCharConverter(PrimitiveObjectInspector inputOI,
        SettableHiveCharObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      HiveCharWritable hc = new HiveCharWritable();
      switch (inputOI.getPrimitiveCategory()) {
        case BOOLEAN:
          return outputOI.set(hc,
              ((BooleanObjectInspector) inputOI).get(input)
                  ? new HiveChar("TRUE", -1) : new HiveChar("FALSE", -1));
        default:
          return outputOI.set(hc, PrimitiveObjectInspectorUtils.getHiveChar(input, inputOI));
      }
    }
  }
}
