/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;


public class FileLookupFunction extends StdUDF2<StdString, StdInteger, StdBoolean> implements TopLevelStdUDF {

  private Set<Integer> ids;

  @Override
  public StdBoolean eval(StdString filename, StdInteger intToCheck) {
    return getStdFactory().createBoolean(ids.contains(intToCheck.get()));
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("varchar", "integer");
  }

  @Override
  public String getOutputParameterSignature() {
    return "boolean";
  }

  @Override
  public String getFunctionName() {
    return "file_lookup";
  }

  @Override
  public String getFunctionDescription() {
    return "Looks up the integer in the file passed as an argument";
  }

  @Override
  public String[] getRequiredFiles(StdString filename, StdInteger intToCheck) {
    return new String[]{filename.get()};
  }

  public void processRequiredFiles(String[] localPaths) {
    loadIdFile(localPaths[0]);
  }

  private void loadIdFile(String localFilename) {
    BufferedReader bufferedReader = null;
    try {
      ids = new HashSet<>();
      bufferedReader = new BufferedReader(new FileReader(localFilename));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        if (!line.startsWith("#") && !line.isEmpty()) {
          Integer id = Integer.valueOf(line);
          ids.add(id);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(bufferedReader);
    }
  }
}
