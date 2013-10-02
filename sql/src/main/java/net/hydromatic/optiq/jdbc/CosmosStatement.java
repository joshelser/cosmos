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
package net.hydromatic.optiq.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema.TableInSchema;
import net.hydromatic.optiq.runtime.ColumnMetaData;
import net.hydromatic.optiq.runtime.Cursor;
import net.hydromatic.optiq.runtime.Cursor.Accessor;

import com.google.common.collect.Lists;

/**
 * @author phrocker
 *
 */
public class CosmosStatement implements Statement{

	private OptiqStatement parent;

	public CosmosStatement(Statement parent)
	{
		
		this.parent = (OptiqStatement)parent;
	}
	/* (non-Javadoc)
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return parent.isWrapperFor(iface);
	}

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return parent.unwrap(iface);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	@Override
	public void addBatch(String sql) throws SQLException {
		 parent.addBatch(sql);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#cancel()
	 */
	@Override
	public void cancel() throws SQLException {
		parent.cancel();
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#clearBatch()
	 */
	@Override
	public void clearBatch() throws SQLException {
		parent.clearBatch();
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#clearWarnings()
	 */
	@Override
	public void clearWarnings() throws SQLException {
		parent.clearWarnings();
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#close()
	 */
	@Override
	public void close() throws SQLException {
		parent.close();
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	@Override
	public boolean execute(String sql) throws SQLException {
		return parent.execute(sql);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	@Override
	public boolean execute(String sql, int autogenKeys) throws SQLException {
		return parent.execute(sql,autogenKeys);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	@Override
	public boolean execute(String sql, int[] keys) throws SQLException {
		return parent.execute(sql,keys);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	@Override
	public boolean execute(String sql, String[] keys) throws SQLException {
		return parent.execute(sql,keys);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeBatch()
	 */
	@Override
	public int[] executeBatch() throws SQLException {
		return parent.executeBatch();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		if(sql.equals("show tables"))
		{
			ColumnMetaData metadata = new ColumnMetaData(1, false, true, false, false, 0, false, 256, "Tables", "Tables", "Tables", 0, 0, "Tables", "", 0, "varchar", true, false, false, "Tables",  (Class)String.class);
			List<ColumnMetaData> metadataList = Lists.newArrayList();
			metadataList.add(metadata);
			
			final MutableSchema schema = parent.getConnection().getRootSchema();
			
			final Iterator<Entry<String,TableInSchema>> blahIter = schema.getSubSchema("cosmos").getTables().entrySet().iterator();
			
			
			
			OptiqResultSetMetaData resultData = new OptiqResultSetMetaData(parent, sql, metadataList);
			OptiqResultSet newSet = new OptiqResultSet(parent,metadataList,resultData, new Function0<Cursor>() {
	            public Cursor apply() {
	                return new Cursor() {
	                  public List<Accessor> createAccessors(
	                      List<ColumnMetaData> types) {
	                    
	                    List<Accessor> accessor = Lists.newArrayList();

	                    	accessor.add(new MyGetter( blahIter ));
	                    return accessor;
	                  }

	                  public boolean next() {
	                	  
	                	  return blahIter.hasNext();
	                  }

	                  public void close() {
	                    // no resources to release
	                  }
	                };
	              }
	            });
			
			return newSet.execute();
			
			
		}
		return parent.executeQuery(sql);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String sql) throws SQLException {
		return parent.executeUpdate(sql);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	@Override
	public int executeUpdate(String sql, int autoGenKey) throws SQLException {
		return parent.executeUpdate(sql,autoGenKey);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	@Override
	public int executeUpdate(String sql, int[] keys) throws SQLException {
		return parent.executeUpdate(sql,keys);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	@Override
	public int executeUpdate(String sql, String[] keys) throws SQLException {
		return parent.executeUpdate(sql,keys);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return parent.getConnection();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getFetchDirection()
	 */
	@Override
	public int getFetchDirection() throws SQLException {
		return parent.getFetchDirection();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getFetchSize()
	 */
	@Override
	public int getFetchSize() throws SQLException {
		return parent.getFetchSize();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return parent.getGeneratedKeys();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	@Override
	public int getMaxFieldSize() throws SQLException {
		return parent.getMaxFieldSize();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getMaxRows()
	 */
	@Override
	public int getMaxRows() throws SQLException {
		return parent.getMaxRows();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getMoreResults()
	 */
	@Override
	public boolean getMoreResults() throws SQLException {
		return parent.getMoreResults();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	@Override
	public boolean getMoreResults(int sql) throws SQLException {
		return parent.getMoreResults(sql);
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	@Override
	public int getQueryTimeout() throws SQLException {
		return parent.getQueryTimeout();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getResultSet()
	 */
	@Override
	public ResultSet getResultSet() throws SQLException {
		return parent.getResultSet();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	@Override
	public int getResultSetConcurrency() throws SQLException {
		return parent.getResultSetConcurrency();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	@Override
	public int getResultSetHoldability() throws SQLException {
		return parent.getResultSetHoldability();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getResultSetType()
	 */
	@Override
	public int getResultSetType() throws SQLException {
		return parent.getResultSetType();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getUpdateCount()
	 */
	@Override
	public int getUpdateCount() throws SQLException {
		return parent.getUpdateCount();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#getWarnings()
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return parent.getWarnings();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#isClosed()
	 */
	@Override
	public boolean isClosed() throws SQLException {
		return parent.isClosed();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#isPoolable()
	 */
	@Override
	public boolean isPoolable() throws SQLException {
		return parent.isPoolable();
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	@Override
	public void setCursorName(String sql) throws SQLException {
		parent.setCursorName(sql);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	@Override
	public void setEscapeProcessing(boolean sql) throws SQLException {
		parent.setEscapeProcessing(sql);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	@Override
	public void setFetchDirection(int dir) throws SQLException {
		parent.setFetchDirection(dir);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	@Override
	public void setFetchSize(int size) throws SQLException {
		parent.setFetchSize(size);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	@Override
	public void setMaxFieldSize(int size) throws SQLException {
		parent.setMaxFieldSize(size);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	@Override
	public void setMaxRows(int rows) throws SQLException {
		setMaxRows(rows);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setPoolable(boolean)
	 */
	@Override
	public void setPoolable(boolean pools) throws SQLException {
		parent.setPoolable(pools);
		
	}

	/* (non-Javadoc)
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	@Override
	public void setQueryTimeout(int timeout) throws SQLException {
		parent.setQueryTimeout(timeout);
		
	}

	private class MyGetter implements Accessor
	{
		private String value;
		private Iterator<Entry<String, TableInSchema>> iter;

		public MyGetter(String value)
		{
			this.value = value;
		}
		/**
		 * @param blahIter
		 */
		public MyGetter(Iterator<Entry<String, TableInSchema>> blahIter) {
			iter = blahIter;
		}
		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getString()
		 */
		@Override
		public String getString() {
			return iter.next().getKey();
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBoolean()
		 */
		@Override
		public boolean getBoolean() {
			// TODO Auto-generated method stub
			return false;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getByte()
		 */
		@Override
		public byte getByte() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getShort()
		 */
		@Override
		public short getShort() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getInt()
		 */
		@Override
		public int getInt() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getLong()
		 */
		@Override
		public long getLong() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getFloat()
		 */
		@Override
		public float getFloat() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDouble()
		 */
		@Override
		public double getDouble() {
			// TODO Auto-generated method stub
			return 0;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBigDecimal()
		 */
		@Override
		public BigDecimal getBigDecimal() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBigDecimal(int)
		 */
		@Override
		public BigDecimal getBigDecimal(int scale) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBytes()
		 */
		@Override
		public byte[] getBytes() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDate()
		 */
		@Override
		public Date getDate() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTime()
		 */
		@Override
		public Time getTime() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTimestamp()
		 */
		@Override
		public Timestamp getTimestamp() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getAsciiStream()
		 */
		@Override
		public InputStream getAsciiStream() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getUnicodeStream()
		 */
		@Override
		public InputStream getUnicodeStream() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBinaryStream()
		 */
		@Override
		public InputStream getBinaryStream() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject()
		 */
		@Override
		public Object getObject() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getCharacterStream()
		 */
		@Override
		public Reader getCharacterStream() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject(java.util.Map)
		 */
		@Override
		public Object getObject(Map<String, Class<?>> map) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getRef()
		 */
		@Override
		public Ref getRef() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBlob()
		 */
		@Override
		public Blob getBlob() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getClob()
		 */
		@Override
		public Clob getClob() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getArray()
		 */
		@Override
		public Array getArray() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDate(java.util.Calendar)
		 */
		@Override
		public Date getDate(Calendar cal) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTime(java.util.Calendar)
		 */
		@Override
		public Time getTime(Calendar cal) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTimestamp(java.util.Calendar)
		 */
		@Override
		public Timestamp getTimestamp(Calendar cal) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getURL()
		 */
		@Override
		public URL getURL() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNClob()
		 */
		@Override
		public NClob getNClob() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getSQLXML()
		 */
		@Override
		public SQLXML getSQLXML() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNString()
		 */
		@Override
		public String getNString() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNCharacterStream()
		 */
		@Override
		public Reader getNCharacterStream() {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject(java.lang.Class)
		 */
		@Override
		public <T> T getObject(Class<T> type) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
