/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.linkedin.transport.api.udf.TopLevelStdUDF;


public interface UserInterfaceLookup extends TopLevelStdUDF {
  @Override
  default String getFunctionName() {
    return "user_interface_lookup";
  }

  @Override
  default String getFunctionDescription() {
    return "_FUNC_(String mobileHeaderOsName, String pageKey, String userAgent, String trackingInfo, "
        + " String/Int/Long default_platform_sk,  const String platformFile, const String clientTypeRegexFile, const"
        + " String pageKeyMetadataFile) "
        + "See https://iwww.corp.linkedin.com/wiki/cf/pages/viewpage.action?pageId=131706093 for user interface "
        + "fetching logic. Basically, we will need osName, pageKey and userAgent to make a decision. If osName is "
        + "empty or null, we will try to use trackingInfo as the substitution. If we cannot figure out platform_id via "
        + "pageKey, we will try to use the default platform_id if provided. If we still don't know platform_id or both "
        + "os name and userAgent, the function will return UNKNOWN. Normal returned values can be: [ Native App, "
        + "Mobile Web,Phone WWW, Tablet WWW, Desktop WWW ].";
  }
}
