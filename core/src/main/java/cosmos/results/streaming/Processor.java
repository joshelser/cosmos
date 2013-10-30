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
 *  Copyright 2013 
 *
 */
package cosmos.results.streaming;

import java.io.IOException;
import java.util.Map.Entry;

import cosmos.results.Column;
import cosmos.results.QueryResult;
import cosmos.results.SValue;

/**
 * @author phrocker
 *
 */
public interface Processor {

	public void flush() throws IOException;
	
	public void execute(QueryResult<?> result);
	
	public void execute(Entry<Column,SValue> entry);
}