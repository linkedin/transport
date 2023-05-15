package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.StdTester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class UserInterfaceLookupTest extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(UserInterfaceLookup.class, ImmutableList.of(UserInterfaceLookupString.class));
  }

  @Test
  public void testUserInterfaceLookupStringFunction() {
    StdTester tester = getTester();
    tester.check(functionCall("user_interface_lookup", null, null, null, null, null, resource("platform.properties"),
        resource("clientTypeREGEX.properties"), resource("pagekey_metadata_sample.avro")), "UNKNOWN", "varchar");

    tester.check(functionCall("user_interface_lookup", null, "m_sim1_profile_tap_discussion_down",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; "
            + ".NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; MDDC; OfficeLiveConnector.1.3; OfficeLivePatch.0.0)",
        "Android OS/4.4.2", "1", resource("platform.properties"), resource("clientTypeREGEX.properties"),
        resource("pagekey_metadata_sample.avro")), "NATIVE APP", "varchar");

    tester.check(functionCall("user_interface_lookup", null, null,
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; Tablet PC 2.0)", null, "1",
        resource("platform.properties"), resource("clientTypeREGEX.properties"),
        resource("pagekey_metadata_sample.avro")), "DESKTOP WWW", "varchar");

    tester.check(functionCall("user_interface_lookup", "", null,
        "mozilla/5.0 (x11; linux; ko-kr) applewebkit/534.26+ (khtml, like gecko) version/5.0 safari/534.26+", null, "1",
        resource("platform.properties"), resource("clientTypeREGEX.properties"),
        resource("pagekey_metadata_sample.avro")), "DESKTOP WWW", "varchar");

    tester.check(
        functionCall("user_interface_lookup", "iPhone OS", "m_tab2_updates_tap_popoveractionsheet_you", "ipad4_2", null,
            "2", resource("platform.properties"), resource("clientTypeREGEX.properties"),
            resource("pagekey_metadata_sample.avro")), "NATIVE APP", "varchar");

    tester.check(functionCall("user_interface_lookup", null, null,
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; Tablet PC 2.0)", null, "1",
        resource("platform.properties"), resource("clientTypeREGEX.properties"),
        resource("pagekey_metadata_sample.avro")), "DESKTOP WWW", "varchar");
  }
}
