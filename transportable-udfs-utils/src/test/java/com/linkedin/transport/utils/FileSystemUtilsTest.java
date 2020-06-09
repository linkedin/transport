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
    String resourcePath = "file://" + getPathForResource("root");

    // Test cases to resolve #LATEST
    String filePath = FileSystemUtils.resolveLatest(resourcePath + "/2018/11/02.dat");
    Assert.assertTrue(
        FileSystemUtils.resolveLatest(resourcePath + "/2018/11/02.dat").endsWith("/root/2018/11/02.dat"));
    Assert.assertTrue(
        FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/11/#LATEST").endsWith("/root/2019/11/02.dat"));
    Assert.assertTrue(
        FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/#LATEST/#LATEST").endsWith("/root/2019/12/02.dat"));
    Assert.assertTrue(
        FileSystemUtils.resolveLatest(resourcePath + "/#LATEST/#LATEST").endsWith("/root/2019/13.dat"));
  }

  private String getPathForResource(String resource) throws URISyntaxException {
    String path = Paths.get(
        Thread.currentThread().getContextClassLoader().getResource(resource).toURI()).toFile().getAbsolutePath();
    return path;
  }
}