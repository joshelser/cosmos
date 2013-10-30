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

import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import cosmos.results.QueryResult;


public class StreamProcessor<K extends QueryResult<?>> implements Iterator<K>{

	
	protected Iterator<K> resultIter;
	
	protected Set<Processor> processors;
	
	public  static <C extends QueryResult<?>> StreamProcessor<C> createStream(Iterable<C> subIter)
	{
		return new StreamProcessor<C>(subIter);
	}
	
	public StreamProcessor<K> attach(Processor proc)
	{
		processors.add(proc);
		return this;
	}
	
	protected StreamProcessor()
	{
		processors = Sets.newHashSet();
	}
	
	protected StreamProcessor(Iterable<K> subIter)
	{
		this();
		Preconditions.checkNotNull(subIter);
		System.out.println("creating");
		this.resultIter = subIter.iterator();
		
	}
	
	protected StreamProcessor(Iterator<K> subIter)
	{
		this();
		Preconditions.checkNotNull(subIter);
		this.resultIter = subIter;
	}
	
	
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return resultIter.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public K next() {
		return executePlan( resultIter.next() );
	}
	
	public void update()
	{
		
	}

	/**
	 * @param next
	 * @return
	 */
	private K executePlan(K nextResult) {
		for(Processor processor : processors)
		{
			processor.execute(nextResult);
		}
		return nextResult;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		resultIter.remove();
	}

	
}
