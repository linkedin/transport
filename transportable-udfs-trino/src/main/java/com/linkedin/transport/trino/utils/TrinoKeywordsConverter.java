/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Trino Keywords Converter maintains the Trino Keywords and related utility functions
 */
public final class TrinoKeywordsConverter {

  private TrinoKeywordsConverter() {

  }

  /**
   * Map of Trino Reserved Keywords with the mark to distinguish if the word is reserved SQL:2016 keyword ONLY
   */
  private static final Map<String, Boolean> RESERVED_KEYWORDS =
      ImmutableMap.<String, Boolean>builder().put("ALTER", Boolean.FALSE)
          .put("AND", Boolean.FALSE)
          .put("AS", Boolean.FALSE)
          .put("BETWEEN", Boolean.FALSE)
          .put("BY", Boolean.FALSE)
          .put("CASE", Boolean.FALSE)
          .put("CAST", Boolean.FALSE)
          .put("CONSTRAINT", Boolean.FALSE)
          .put("CREATE", Boolean.FALSE)
          .put("CROSS", Boolean.FALSE)
          .put("CUBE", Boolean.TRUE)
          .put("CURRENT_DATE", Boolean.FALSE)
          .put("CURRENT_TIME", Boolean.FALSE)
          .put("CURRENT_TIMESTAMP", Boolean.FALSE)
          .put("CURRENT_USER", Boolean.TRUE)
          .put("DEALLOCATE", Boolean.FALSE)
          .put("DELETE", Boolean.FALSE)
          .put("DESCRIBE", Boolean.FALSE)
          .put("DISTINCT", Boolean.FALSE)
          .put("DROP", Boolean.FALSE)
          .put("ELSE", Boolean.FALSE)
          .put("END", Boolean.FALSE)
          .put("ESCAPE", Boolean.FALSE)
          .put("EXCEPT", Boolean.FALSE)
          .put("EXECUTE", Boolean.FALSE)
          .put("EXISTS", Boolean.FALSE)
          .put("EXTRACT", Boolean.FALSE)
          .put("FALSE", Boolean.FALSE)
          .put("FOR", Boolean.FALSE)
          .put("FROM", Boolean.FALSE)
          .put("FULL", Boolean.FALSE)
          .put("GROUP", Boolean.FALSE)
          .put("GROUPING", Boolean.TRUE)
          .put("HAVING", Boolean.FALSE)
          .put("IN", Boolean.FALSE)
          .put("INNER", Boolean.FALSE)
          .put("INSERT", Boolean.FALSE)
          .put("INTERSECT", Boolean.FALSE)
          .put("INTO", Boolean.FALSE)
          .put("IS", Boolean.FALSE)
          .put("JOIN", Boolean.FALSE)
          .put("LEFT", Boolean.FALSE)
          .put("LIKE", Boolean.FALSE)
          .put("LOCALTIME", Boolean.TRUE)
          .put("LOCALTIMESTAMP", Boolean.TRUE)
          .put("NATURAL", Boolean.FALSE)
          .put("NORMALIZE", Boolean.TRUE)
          .put("NOT", Boolean.FALSE)
          .put("NULL", Boolean.FALSE)
          .put("ON", Boolean.FALSE)
          .put("OR", Boolean.FALSE)
          .put("ORDER", Boolean.FALSE)
          .put("OUTER", Boolean.FALSE)
          .put("PREPARE", Boolean.FALSE)
          .put("RECURSIVE", Boolean.TRUE)
          .put("RIGHT", Boolean.FALSE)
          .put("ROLLUP", Boolean.TRUE)
          .put("SELECT", Boolean.FALSE)
          .put("TABLE", Boolean.FALSE)
          .put("THEN", Boolean.FALSE)
          .put("TRUE", Boolean.FALSE)
          .put("UESCAPE", Boolean.TRUE)
          .put("UNION", Boolean.FALSE)
          .put("UNNEST", Boolean.TRUE)
          .put("USING", Boolean.FALSE)
          .put("VALUES", Boolean.FALSE)
          .put("WHEN", Boolean.FALSE)
          .put("WHERE", Boolean.FALSE)
          .put("WITH", Boolean.FALSE)
          .build();

  /**
   * Quote the preserved keywords which might appear as field names in the type signatures
   * i.e. `row(key varchar, values varchar)` will be converted to `row(key varchar, "values" varchar)`
   * if they are not quoted, `io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature` will throw exception
   *
   * @param signature input type signature
   * @return converted type signature which is properly quoted
   */
  public static String quoteReservedKeywordsInTypeSignature(String signature) {
    signature = signature.toLowerCase(Locale.ROOT);
    for (String keyword : RESERVED_KEYWORDS.keySet()) {
      String lowercaseKeyword = keyword.toLowerCase(Locale.ROOT);
      // the preserved keyword may only appear as a field name in `row` type
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
        final String replacedGroup = group.replaceAll(lowercaseKeyword, "\"" + lowercaseKeyword + "\"");
        signature = signature.replace(group, replacedGroup);
      }
    }
    return signature;
  }
}
