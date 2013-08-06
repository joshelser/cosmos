/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.options;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public abstract class Defaults {
  public static final boolean LOCK_ON_UPDATES = false;
  public static final String DATA_TABLE = "cosmos";
  public static final String METADATA_TABLE = "metacosmos";
  
  public static final String NULL_BYTE_STR = "\u0000";
  public static final String EIN_BYTE_STR = "\u0001";
  
  public static final String DOCID_FIELD_NAME = "COSMOS_DOCID";
  public static final Text DOCID_FIELD_NAME_TEXT = new Text(DOCID_FIELD_NAME);
  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  public static final String CURATOR_PREFIX = "/cosmos/";
  
  public static final String CONTENTS_LG_NAME = "contents";
  public static final String CONTENTS_COLFAM = "CONTENTS";
  public static final Text CONTENTS_COLFAM_TEXT = new Text(CONTENTS_COLFAM);
}
