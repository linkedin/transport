/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package udfs;

import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF0;
import com.linkedin.transport.api.udf.TopLevelStdUDF;


public abstract class AbstractUDF extends StdUDF0<StdString> implements TopLevelStdUDF {

}