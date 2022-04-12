/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.typesystem.TypeSignature;
import com.linkedin.transport.typesystem.TypeSignatureElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.transport.typesystem.ConcreteTypeSignatureElement.*;


/**
 * Trino Keywords Converter maintains the Trino Keywords and related utility functions
 */
public final class StdUDFUtils {

  private StdUDFUtils() {

  }

  // Reserved keywords from https://trino.io/docs/current/language/reserved.html
  private static final Set<String> RESERVED_KEYWORDS =
      ImmutableSet.of("ALTER", "AND", "AS", "BETWEEN", "BY", "CASE", "CAST", "CONSTRAINT", "CREATE", "CROSS", "CUBE",
          "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
          "CURRENT_TIMESTAMP", "CURRENT_USER", "DEALLOCATE", "DELETE", "DESCRIBE", "DISTINCT", "DROP", "ELSE", "END",
          "ESCAPE", "EXCEPT", "EXECUTE", "EXISTS", "EXTRACT", "FALSE", "FOR", "FROM", "FULL", "GROUP", "GROUPING",
          "HAVING", "IN", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LISTAGG", "LOCALTIME",
          "LOCALTIMESTAMP", "NATURAL", "NORMALIZE", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "PREPARE", "RECURSIVE",
          "RIGHT", "ROLLUP", "SELECT", "SKIP", "TABLE", "THEN", "TRUE", "UESCAPE", "UNION", "UNNEST", "USING", "VALUES",
          "WHEN", "WHERE", "WITH");

  /**
   * Quote the reserved keywords which might appear as field names in the type signatures
   * i.e. `row(key varchar, values varchar)` will be converted to `row(key varchar, "values" varchar)`
   * if they are not quoted, `io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature` will throw exception
   *
   * @param signature input type signature
   * @return converted type signature which is properly quoted
   */
  static String quoteReservedKeywords(String signature) {
    return toTrinoTypeSignatureString(TypeSignature.parse(signature));
  }

  private static String toTrinoTypeSignatureString(TypeSignature typeSignature) {
    final TypeSignatureElement typeSignatureBase = typeSignature.getBase();
    if (BOOLEAN.equals(typeSignatureBase)) {
      return "boolean";
    } else if (INTEGER.equals(typeSignatureBase)) {
      return "integer";
    } else if (LONG.equals(typeSignatureBase)) {
      return "bigint";
    } else if (STRING.equals(typeSignatureBase)) {
      return "varchar";
    } else if (FLOAT.equals(typeSignatureBase)) {
      return "real";
    } else if (DOUBLE.equals(typeSignatureBase)) {
      return "double";
    } else if (BINARY.equals(typeSignatureBase)) {
      return "varbinary";
    } else if (UNKNOWN.equals(typeSignatureBase)) {
      return "unknown";
    } else if (ARRAY.equals(typeSignatureBase)) {
      return getTrinoArraySignatureString(typeSignature);
    } else if (MAP.equals(typeSignatureBase)) {
      return getTrinoMapSignatureString(typeSignature);
    } else if (STRUCT.equals(typeSignatureBase)) {
      return getTrinoStructSignatureString(typeSignature);
    }
    return typeSignatureBase.toString();
  }

  private static String getTrinoArraySignatureString(TypeSignature typeSignature) {
    return String.format("array(%s)", toTrinoTypeSignatureString(typeSignature.getParameters().get(0)));
  }

  private static String getTrinoMapSignatureString(TypeSignature typeSignature) {
    return String.format("map(%s,%s)", toTrinoTypeSignatureString(typeSignature.getParameters().get(0)),
        toTrinoTypeSignatureString(typeSignature.getParameters().get(1)));
  }

  private static String getTrinoStructSignatureString(TypeSignature typeSignature) {
    final List<String> parameterNames = typeSignature.getParameterNames();
    final List<TypeSignature> parameters = typeSignature.getParameters();
    if (parameterNames == null) {
      return "row(" + parameters.stream().map(StdUDFUtils::toTrinoTypeSignatureString).collect(Collectors.joining(","))
          + ")";
    } else {
      final List<String> quotedParameterNames = parameterNames.stream()
          .map(parameterName -> RESERVED_KEYWORDS.contains(parameterName.toUpperCase(Locale.ROOT)) ? ("\""
              + parameterName + "\"") : parameterName)
          .collect(Collectors.toList());
      List<String> parameterNameWithType = new ArrayList<>();
      for (int i = 0; i < parameters.size(); ++i) {
        parameterNameWithType.add(quotedParameterNames.get(i) + " " + toTrinoTypeSignatureString(parameters.get(i)));
      }
      return "row(" + String.join(",", parameterNameWithType) + ")";
    }
  }
}
