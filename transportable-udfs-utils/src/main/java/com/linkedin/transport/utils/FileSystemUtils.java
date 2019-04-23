/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * This Utils class handles multiple utilities methods related with Hadoop FileSystem.
 *
 */
public class FileSystemUtils {
  public static final String MAPREDUCE_FRAMEWORK_NAME = "mapreduce.framework.name";
  public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
  public static final String LOCAL = "local";

  private FileSystemUtils() {
    // Empty on purpose
  }

  /**
   * Checks if the current UDF is running in local environment or something else.
   *
   * @param conf the Hadoop configuration
   * @return true if it is in local mode
   */
  public static boolean isLocalEnvironment(Configuration conf) {
    return conf.get(MAPREDUCE_FRAMEWORK_NAME, conf.get(MAPRED_JOB_TRACKER, LOCAL)).equals(LOCAL);
  }

  /**
   * Get the HDFS FileSystem
   *
   * @return the HDFS FileSystem if we are not in local mode, local FileSystem if we are.
   */
  public static FileSystem getHDFSFileSystem() {
    FileSystem fs;
    JobConf conf = new JobConf();
    try {
      // Checks if currently we are in local mode, which is basically when running unit tests
      if (isLocalEnvironment(conf)) {
        fs = FileSystem.getLocal(conf);
      } else {
        fs = FileSystem.get(conf);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load the HDFS file system.", e);
    }

    return fs;
  }

  /**
   * Get the local FileSystem, useful when reading DistributedCache
   *
   * @return the local file system
   */
  public static FileSystem getLocalFileSystem() {
    FileSystem fs;
    try {
      fs = FileSystem.getLocal(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException("Failed to load the local file system.", e);
    }

    return fs;
  }


  /***
   * This method is a utility method that address a common use case with UDFs that depend on files in HDFS.
   * Most of those UDFs depend on files that are updated on a timely basis, but each snapshot of the file does
   * not override the existing file, but rather writes to a new directory whose name is a timestamp. Hence, this
   * utility method gets the most recent version of the file by replacing the pattern "#LATEST" in the given path
   * with the most recent directory. If the input path does not contain the keyword "#LATEST" then it simply returns
   * the same path.
   *
   * @param path the path to resolve
   * @param fs the filesystem used to resolve the path
   * @return the resolved path
   * @throws IOException when the filesystem could not resolve the path
   */
  public static String resolveLatest(String path, FileSystem fs) throws IOException {
    if (!StringUtils.isBlank(path)) {
      path = path.trim();
      String[] split = path.split("#LATEST");
      String retval = split[0];

      for (int i = 1; i < split.length; ++i) {
        retval = resolveLatestHelper(retval, fs, true) + split[i];
      }

      //if the path ends with #LATEST, get the latest candidate regardless of file or directory
      if (path.endsWith("#LATEST")) {
        retval = resolveLatestHelper(retval, fs, false);
      }

      return retval;
    } else {
      throw new IllegalArgumentException("The path to resolve is an empty string.");
    }
  }

  private static String resolveLatestHelper(String path, FileSystem fs, boolean excludeFiles) throws IOException {
    if (!StringUtils.isBlank(path)) {
      path = path.trim();
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }

      FileStatus[] filesAndDirectories = fs.listStatus(new Path(path));
      List<FileStatus> candidates = Arrays.stream(filesAndDirectories).filter(s -> !excludeFiles || s.isDirectory())
          .sorted().collect(Collectors.toList());
      if (candidates != null && candidates.size() != 0) {
        String retval = path + "/" + candidates.get(candidates.size() - 1).getPath().getName();
        return retval;
      } else {
        throw new IOException("The path to resolve does not exist: [" + path + "]");
      }
    } else {
      throw new IllegalArgumentException("The path to resolve is an empty string.");
    }
  }
}
