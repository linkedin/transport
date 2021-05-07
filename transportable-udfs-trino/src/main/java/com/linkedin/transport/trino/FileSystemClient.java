/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.linkedin.transport.utils.FileSystemUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;


public class FileSystemClient {

  final static String PROPERTIES_FILE_PATH = "etc/udf-config.properties";
  /***
   * Properties to customize the behaviour of this client. These properties are read from "etc/udf-config.properties"
   * The following keys are used:
   * udf.hdfs-cache-dir: indicates where the file required by this UDF is downloaded locally
   * udf.authentication: indicates the method used to authenticate access to HDFS. Possible values are "password",
   *                     which simply relies on running kinit from the shell before calling the UDF, and "keytab",
   *                     which authenticates using a given principal and keytab file path.
   * udf.kerberos.principal: the prinicipal to use with keberos (keytab) authentication
   * udf.kerberos.keytab.file: the path of the keytab file of the princinpal in "udf.kerberos.principal"
   */
  final Properties _properties;

  public FileSystemClient() {
    _properties = new Properties();
  }

  private static void loginHeadlessAccount(String principal, String keytabFile) {
    try {
      UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
    } catch (IOException e) {
      throw new RuntimeException("Error authenticating user \"" + principal + "\" using keytab file: " + keytabFile, e);
    }
  }

  public String copyToLocalFile(String remoteFilename) {
    try {
      loadProperties();
      Configuration conf = getConfiguration();
      login(conf);

      Path remotePath = new Path(remoteFilename);
      Path localPath = new Path(Paths.get(getAndCreateLocalDir(), new File(remoteFilename).getName()).toString());
      FileSystem fs = remotePath.getFileSystem(conf);
      // It is important to pass the custom configuration object to FileSystemUtils since we load some extra
      // properties from etc/**.xml in getConfiguration() for Trino
      String resolvedRemoteFilename = FileSystemUtils.resolveLatest(remoteFilename, conf);
      Path resolvedRemotePath = new Path(resolvedRemoteFilename);
      fs.copyToLocalFile(resolvedRemotePath, localPath);
      return localPath.toString();
    } catch (Exception e) {
      throw new RuntimeException("Error downloading HDFS file: " + remoteFilename, e);
    }
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    File hdfsSite = new File("etc/hdfs-site.xml");
    if (hdfsSite.exists()) {
      conf.addResource(new Path(hdfsSite.toString()));
    }
    File coreSite = new File("etc/core-site.xml");
    if (coreSite.exists()) {
      conf.addResource(new Path(coreSite.toString()));
    }
    return conf;
  }

  private void login(Configuration conf) {
    String authenticationMethod = _properties.getProperty("udf.authentication", "password");
    if (authenticationMethod.equalsIgnoreCase("password")) {
      return;
    } else if (authenticationMethod.equalsIgnoreCase("keytab")) {
      String principal = _properties.getProperty("udf.kerberos.principal");
      String keytab = _properties.getProperty("udf.kerberos.keytab.file");
      if (principal == null || keytab == null) {
        throw new RuntimeException("Keytab authentication specified, but principal or keytab information missing");
      }
      conf.setStrings("hadoop.security.authentication", "KERBEROS");
      UserGroupInformation.setConfiguration(conf);
      loginHeadlessAccount(principal, keytab);
    } else {
      throw new RuntimeException("Unrecognized authentication method: " + authenticationMethod);
    }
  }

  private void loadProperties() {
    File propertiesFile = new File(PROPERTIES_FILE_PATH);
    if (propertiesFile.exists()) {
      try (FileInputStream in = new FileInputStream(PROPERTIES_FILE_PATH)) {
        _properties.load(in);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to load properties from properties file: " + propertiesFile.getAbsolutePath());
      }
    }
  }

  private String getAndCreateLocalDir() {
    String localDir = _properties.getProperty("udf.hdfs-cache-dir", "hdfs_cache");
    new File(localDir).mkdirs();
    return localDir;
  }
}
