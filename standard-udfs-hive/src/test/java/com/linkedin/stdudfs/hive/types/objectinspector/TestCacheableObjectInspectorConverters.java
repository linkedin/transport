package com.linkedin.stdudfs.hive.types.objectinspector;

import com.linkedin.stdudfs.hive.types.objectinspector.CacheableObjectInspectorConverters.MapConverter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;


public class TestCacheableObjectInspectorConverters {

  @Test
  public void testCaching() {
    // Should return cached OIConverter on subsequent invocations
    CacheableObjectInspectorConverters cacheableObjectInspectorConverters = new CacheableObjectInspectorConverters();

    Converter c1 =
        cacheableObjectInspectorConverters.getConverter(writableStringObjectInspector, javaStringObjectInspector);
    Converter c2 =
        cacheableObjectInspectorConverters.getConverter(writableStringObjectInspector, javaStringObjectInspector);
    Assert.assertSame(c1, c2);

    MapConverter c3 = (MapConverter) cacheableObjectInspectorConverters.getConverter(
        ObjectInspectorFactory.getStandardMapObjectInspector(writableStringObjectInspector,
            writableStringObjectInspector),
        ObjectInspectorFactory.getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector)
    );
    Assert.assertSame(c1, c3.keyConverter);
    Assert.assertSame(c1, c3.valueConverter);
  }

  @Test
  public void testReturnValue() {
    // Should create new return objects inside the converter
    CacheableObjectInspectorConverters cacheableObjectInspectorConverters = new CacheableObjectInspectorConverters();

    Converter c1 =
        cacheableObjectInspectorConverters.getConverter(javaStringObjectInspector, writableStringObjectInspector);
    String s1 = "Test_STR";
    Object o1 = c1.convert(s1);
    Object o2 = c1.convert(s1);
    Assert.assertNotSame(o1, o2);
  }

  @Test
  public void testObjectInspectorConverters() throws Throwable {
    try {
      CacheableObjectInspectorConverters cacheableObjectInspectorConverters = new CacheableObjectInspectorConverters();

      // Boolean
      Converter booleanConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableBooleanObjectInspector);
      Assert.assertEquals(new BooleanWritable(false), booleanConverter.convert(Integer.valueOf(0)), "BooleanConverter");
      Assert.assertEquals(new BooleanWritable(true), booleanConverter.convert(Integer.valueOf(1)), "BooleanConverter");
      Assert.assertEquals(null, booleanConverter.convert(null), "BooleanConverter");

      // Byte
      Converter byteConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableByteObjectInspector);
      Assert.assertEquals(new ByteWritable((byte) 0), byteConverter.convert(Integer.valueOf(0)), "ByteConverter");
      Assert.assertEquals(new ByteWritable((byte) 1), byteConverter.convert(Integer.valueOf(1)), "ByteConverter");
      Assert.assertEquals(null, byteConverter.convert(null), "ByteConverter");

      // Short
      Converter shortConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableShortObjectInspector);
      Assert.assertEquals(new ShortWritable((short) 0), shortConverter.convert(Integer.valueOf(0)), "ShortConverter");
      Assert.assertEquals(new ShortWritable((short) 1), shortConverter.convert(Integer.valueOf(1)), "ShortConverter");
      Assert.assertEquals(null, shortConverter.convert(null), "ShortConverter");

      // Int
      Converter intConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableIntObjectInspector);
      Assert.assertEquals(new IntWritable(0), intConverter.convert(Integer.valueOf(0)), "IntConverter");
      Assert.assertEquals(new IntWritable(1), intConverter.convert(Integer.valueOf(1)), "IntConverter");
      Assert.assertEquals(null, intConverter.convert(null), "IntConverter");

      // Long
      Converter longConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableLongObjectInspector);
      Assert.assertEquals(new LongWritable(0), longConverter.convert(Integer.valueOf(0)), "LongConverter");
      Assert.assertEquals(new LongWritable(1), longConverter.convert(Integer.valueOf(1)), "LongConverter");
      Assert.assertEquals(null, longConverter.convert(null), "LongConverter");

      // Float
      Converter floatConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableFloatObjectInspector);
      Assert.assertEquals(new FloatWritable(0), floatConverter.convert(Integer.valueOf(0)), "FloatConverter");
      Assert.assertEquals(new FloatWritable(1), floatConverter.convert(Integer.valueOf(1)), "FloatConverter");
      Assert.assertEquals(null, floatConverter.convert(null), "FloatConverter");

      // Double
      Converter doubleConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableDoubleObjectInspector);
      Assert.assertEquals(new DoubleWritable(0), doubleConverter.convert(Integer.valueOf(0)), "DoubleConverter");
      Assert.assertEquals(new DoubleWritable(1), doubleConverter.convert(Integer.valueOf(1)), "DoubleConverter");
      Assert.assertEquals(null, doubleConverter.convert(null), "DoubleConverter");

      // Text
      Converter textConverter =
          cacheableObjectInspectorConverters.getConverter(javaIntObjectInspector, writableStringObjectInspector);
      Assert.assertEquals(new Text("0"), textConverter.convert(Integer.valueOf(0)), "TextConverter");
      Assert.assertEquals(new Text("1"), textConverter.convert(Integer.valueOf(1)), "TextConverter");
      Assert.assertEquals(null, textConverter.convert(null), "TextConverter");

      textConverter =
          cacheableObjectInspectorConverters.getConverter(writableBinaryObjectInspector, writableStringObjectInspector);
      Assert.assertEquals(new Text("hive"), textConverter
          .convert(new BytesWritable(new byte[]{(byte) 'h', (byte) 'i', (byte) 'v', (byte) 'e'})), "TextConverter");
      Assert.assertEquals(null, textConverter.convert(null), "TextConverter");

      textConverter =
          cacheableObjectInspectorConverters.getConverter(writableStringObjectInspector, writableStringObjectInspector);
      Assert.assertEquals(new Text("hive"), textConverter.convert(new Text("hive")), "TextConverter");
      Assert.assertEquals(null, textConverter.convert(null), "TextConverter");

      textConverter =
          cacheableObjectInspectorConverters.getConverter(javaStringObjectInspector, writableStringObjectInspector);
      Assert.assertEquals(new Text("hive"), textConverter.convert(new String("hive")), "TextConverter");
      Assert.assertEquals(null, textConverter.convert(null), "TextConverter");

      textConverter = cacheableObjectInspectorConverters.getConverter(javaHiveDecimalObjectInspector,
          writableStringObjectInspector);
      Assert.assertEquals(new Text("100.001"), textConverter.convert(HiveDecimal.create("100.001")), "TextConverter");
      Assert.assertEquals(null, textConverter.convert(null), "TextConverter");

      // Binary
      Converter baConverter =
          cacheableObjectInspectorConverters.getConverter(javaStringObjectInspector, writableBinaryObjectInspector);
      Assert.assertEquals(new BytesWritable(new byte[]{(byte) 'h', (byte) 'i', (byte) 'v', (byte) 'e'}),
          baConverter.convert("hive"), "BAConverter");
      Assert.assertEquals(null, baConverter.convert(null), "BAConverter");

      baConverter =
          cacheableObjectInspectorConverters.getConverter(writableStringObjectInspector, writableBinaryObjectInspector);
      Assert.assertEquals(new BytesWritable(new byte[]{(byte) 'h', (byte) 'i', (byte) 'v', (byte) 'e'}),
          baConverter.convert(new Text("hive")), "BAConverter");
      Assert.assertEquals(null, baConverter.convert(null), "BAConverter");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
