/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF8;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;


public class UserInterfaceLookupString
    extends StdUDF8<StdString, StdString, StdString, StdString, StdString, StdString, StdString, StdString, StdString>
    implements UserInterfaceLookup {
  private Map<String, String> _platformProperties;
  private Map<String, Pattern> _platformPatterns;
  private Map<String, String> _pageKeyToPlatformId;

  private Pattern _phoneWWWPattern = null;
  private Pattern _tabletWWWPattern = null;

  private final static String PROPERTIES_FILE_DELIM = "=";
  public final static String UNKNOWN = "UNKNOWN";

  private final static Pattern PHONE_WEB_TABLET_WEB_PATTERN =
      Pattern.compile(".*(phone_web|tablet_web).*", Pattern.CASE_INSENSITIVE);
  private final static Pattern TRACKING_INFO_PATTERN =
      Pattern.compile(".*" + "(android|iphone|series40|blackberry|windows|nokia).*", Pattern.CASE_INSENSITIVE);

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar");
  }

  @Override
  public String getOutputParameterSignature() {
    return "varchar";
  }

  @Override
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _platformProperties = new LinkedHashMap();
    _platformPatterns = new LinkedHashMap<String, Pattern>();
  }

  @Override
  public StdString eval(StdString stdString1, StdString stdString2, StdString stdString3, StdString stdString4,
      StdString stdString5, StdString platformPropFilePath, StdString clientTypePatternFilePath,
      StdString pageKeyMetadataFilePath) {

    String osName = null;
    if (stdString1 != null) {
      osName = stdString1.get();
    }

    String pageKey = null;
    if (stdString2 != null) {
      pageKey = stdString2.get();
    }

    String userAgent = null;
    if (stdString3 != null) {
      userAgent = stdString3.get();
    }

    String trackingInfo = null;
    if (stdString4 != null) {
      trackingInfo = stdString4.get();
    }

    String defaultPlatformSK = null;
    if (stdString5 != null) {
      defaultPlatformSK = stdString5.get();
    }

    String userInterface = UNKNOWN;

    // Try fetch osName from trackingInfo if mobileHeaderOsName is not presented
    if (osName == null || osName.trim().isEmpty()) {
      if (trackingInfo != null && TRACKING_INFO_PATTERN.matcher(trackingInfo).matches()) {
        osName = trackingInfo;
      } else {
        osName = "";
      }
    }

    String platformSK = _pageKeyToPlatformId.get(pageKey);
    if (platformSK == null) {
      if (defaultPlatformSK == null) {
        // Assign an empty string and continue if the platform_sk is null and no default is presented.
        // When osName is native app, event platform_sk is not prestented, we can still categorize it as native app
        platformSK = "";
      } else {
        platformSK = defaultPlatformSK;
      }
    }

    // Do not proceed if osName and userAgent are both empty
    if (osName.isEmpty() && (userAgent == null || userAgent.isEmpty())) {
      return getStdFactory().createString(userInterface);
    }

    /*  When Mobile Header is present, apply additional checks:
     *  Ignore mobile header if it contains phone_web and tablet_web. In the past, mobile header was
     *  emitted with these strings and did not correspond to native app.
     */
    if (!osName.isEmpty() && !PHONE_WEB_TABLET_WEB_PATTERN.matcher(osName).matches()) {
      // Presence of a mobileHeader.osName field indicates it's native app
      //If platform = 1 then we instead map to desktop app for differentiation from windows mobile app without needing to rely on platform.
      if (platformSK.trim().equals("1")) {
        userInterface = "Desktop App";
      } else {
        userInterface = "Native App";
      }
    } else {

      // Fetch platform group
      String platformGroup = "Uncategorized Platform";
      if (StringUtils.isNotBlank(platformSK)) {
        for (String p : _platformProperties.keySet()) {
          Matcher platformMatcher = _platformPatterns.get(p).matcher(platformSK);
          if (platformMatcher.find()) {
            platformGroup = _platformProperties.get(p);
            break;
          }
        }
      }

      // Platform page key is for mobile optimized page
      if (platformGroup.contentEquals("MobileWeb")) {
        userInterface = "Mobile Web";
      } else if (platformGroup.contentEquals("UserAgent") && _phoneWWWPattern != null && _tabletWWWPattern != null) {
        /* If regex pattern read from property file is empty, the following code will be skipped and UNKNOWN will
         * be returned.
         */

        if (userAgent != null && _phoneWWWPattern.matcher(userAgent).matches()) {
          userInterface = "Phone WWW";
        } else if (userAgent != null && _tabletWWWPattern.matcher(userAgent).matches()) {
          userInterface = "Tablet WWW";
        } else {
          userInterface = "Desktop WWW";
        }
      }
    }
    return getStdFactory().createString(userInterface.toUpperCase());
  }

  @Override
  public String[] getRequiredFiles(StdString stdString1, StdString stdString2, StdString stdString3,
      StdString stdString4, StdString stdString5, StdString platformPropFilePath, StdString clientTypePatternFilePath,
      StdString pageKeyMetadataFilePath) {

    return new String[]{platformPropFilePath.get(), clientTypePatternFilePath.get(), pageKeyMetadataFilePath.get()};
  }

  @Override
  public void processRequiredFiles(String[] files) {
    try {
      initPlatformProperties(files[0]);
      initClientTypePatterns(files[1]);
      initPageKeyMetadata(files[2]);
    } catch (IOException e) {
      throw new RuntimeException("Error processing the required files for UDF: UserInterfaceLookup");
    }
  }

  /**
   * Load platform values from the file
   * This is used to check the platform based on platform_sk
   */
  private void initPlatformProperties(String file) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(new File(file)));
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#") && !line.isEmpty()) {
          String[] tokens = line.split(PROPERTIES_FILE_DELIM);
          String key = tokens[0].trim();
          _platformProperties.put(key, tokens[1]);
          _platformPatterns.put(key, Pattern.compile(key, Pattern.CASE_INSENSITIVE));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(br);
    }
  }

  /**
   * Load REGEX patterns that will be used to check User Agent strings and derive the clientType
   */
  private void initClientTypePatterns(String file) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(new File(file)));
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#") && !line.isEmpty()) {
          String[] tokens = line.split(PROPERTIES_FILE_DELIM);
          String key = tokens[0].trim();
          String val = tokens[1];

          if ("PhoneWWW".equals(key)) {
            _phoneWWWPattern =
                Pattern.compile(".*(" + (val == null ? "" : val.trim()) + ").*", Pattern.CASE_INSENSITIVE);
          } else if ("TabletWWW".equals(key)) {
            _tabletWWWPattern =
                Pattern.compile(".*(" + (val == null ? "" : val.trim()) + ").*", Pattern.CASE_INSENSITIVE);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(br);
    }
  }

  /**
   * Load the map between the page key and the platform id from the avro file
   */
  private void initPageKeyMetadata(String file) throws IOException {
    DataFileStream<GenericRecord> datumReader = new DataFileStream(new FileInputStream(file), new GenericDatumReader());

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (GenericRecord datum : datumReader) {
      Object key = datum.get("page_key");
      Object value = datum.get("platform_id");
      if (key == null || value == null) {
        continue;
      }
      builder.put(key.toString(), value.toString());
    }
    datumReader.close();
    _pageKeyToPlatformId = builder.build();
  }

  @Override
  public boolean[] getNullableArguments() {
    return new boolean[]{true, true, true, true, true, false, false, false};
  }
}
