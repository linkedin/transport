/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.transport.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FileSystemUtilsTest {

  @Test
  public void testResolveLatest() throws IOException, URISyntaxException {
    // Use proper file URI format: file:/// for local files
    String resourcePath = java.nio.file.Paths.get(getPathForResource("root")).toUri().toString().replaceAll("/$", "");

    // Test cases to resolve #LATEST
    // Normalize paths for cross-platform compatibility by replacing backslashes with forward slashes
    String filePath = FileSystemUtils.resolveLatest(resourcePath + "/2018/11/02.dat");
    Assert.assertTrue(
        normalizePath(FileSystemUtils.resolveLatest(resourcePath + "/2018/11/02.dat")).endsWith("/root/2018/11/02.dat"),
        "Expected path to end with /root/2018/11/02.dat but was: " + normalizePath(FileSystemUtils.resolveLatest(resourcePath + "/2018/11/02.dat")));
    Assert.assertTrue(
        normalizePath(FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/11/#LATEST")).endsWith("/root/2019/11/02.dat"));
    Assert.assertTrue(
        normalizePath(FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/#LATEST/#LATEST")).endsWith("/root/2019/12/02.dat"));
    Assert.assertTrue(
        normalizePath(FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/#LATEST")).endsWith("/root/2019/13.dat"));
  }

  private String normalizePath(String path) {
    // Replace backslashes with forward slashes for consistent path comparison
    return path.replace('\\', '/');
  }

  private String getPathForResource(String resource) throws URISyntaxException {
    String path = Paths.get(
        Thread.currentThread().getContextClassLoader().getResource(resource).toURI()).toFile().getAbsolutePath();
    return path;
  }
}