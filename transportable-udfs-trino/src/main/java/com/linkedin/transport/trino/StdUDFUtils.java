/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
  public static String quoteReservedKeywords(String signature) {
    for (String keyword : RESERVED_KEYWORDS) {
      String lowercaseKeyword = keyword.toLowerCase(Locale.ROOT);
      // The preserved keyword may only appear as a field name in `row` type
      // in the following scenarios (`values` is the preserved keyword):
      // (1) row(field1 type1, values type2)
      // (2) row(field1 type1,values type2)
      // (3) row(values type1, field2 type2)
      // therefore, the previous character must be one of `,`, `(` or space(s)
      // and there must be the type name following the keyword after space(s)
      Pattern pattern = Pattern.compile("(\\(|\\s+|,)" + lowercaseKeyword + "\\s+\\w+");
      final Matcher matcher = pattern.matcher(signature);
      while (matcher.find()) {
        final String group = matcher.group();
        final String replacedGroup = group.replaceFirst(lowercaseKeyword, "\"" + lowercaseKeyword + "\"");
        signature = signature.replace(group, replacedGroup);
      }
    }
    return signature;
  }
}
