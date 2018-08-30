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
package com.linkedin.stdudfs.hive.common;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class StandardObjectInspectorFactory {
  private StandardObjectInspectorFactory() {
  }

  final public static ObjectInspector INTEGER = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  final public static ObjectInspector LONG = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  final public static ObjectInspector BOOLEAN = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  final public static ObjectInspector STRING = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  final public static ObjectInspector NULL = PrimitiveObjectInspectorFactory.javaVoidObjectInspector;

  public static ObjectInspector array(ObjectInspector elementObjectInspector) {
    return ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
  }

  public static ObjectInspector map(ObjectInspector keyObjectInspector, ObjectInspector valueObjectInspector) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
  }

  public static ObjectInspector struct(ObjectInspector... fieldObjectInspectors) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        IntStream.range(0, fieldObjectInspectors.length).mapToObj(i -> "f" + i).collect(Collectors.toList()),
        Arrays.asList(fieldObjectInspectors)
    );
  }
}
