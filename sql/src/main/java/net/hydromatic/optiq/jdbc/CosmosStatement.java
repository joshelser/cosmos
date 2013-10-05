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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;

import cosmos.sql.parser.CosmosResultSet;
import cosmos.sql.parser.CosmosSQLLexer;
import cosmos.sql.parser.CosmosSQLParser;
import cosmos.sql.rules.ResultSetRule;

/**
 * @author phrocker
 * 
 */
public class CosmosStatement implements Statement {

  private OptiqStatement parent;

  public CosmosStatement(Statement parent) {

    this.parent = (OptiqStatement) parent;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return parent.isWrapperFor(iface);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return parent.unwrap(iface);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#addBatch(java.lang.String)
   */
  @Override
  public void addBatch(String sql) throws SQLException {
    parent.addBatch(sql);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#cancel()
   */
  @Override
  public void cancel() throws SQLException {
    parent.cancel();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#clearBatch()
   */
  @Override
  public void clearBatch() throws SQLException {
    parent.clearBatch();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {
    parent.clearWarnings();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#close()
   */
  @Override
  public void close() throws SQLException {
    parent.close();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String)
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    return parent.execute(sql);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, int)
   */
  @Override
  public boolean execute(String sql, int autogenKeys) throws SQLException {
    return parent.execute(sql, autogenKeys);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */
  @Override
  public boolean execute(String sql, int[] keys) throws SQLException {
    return parent.execute(sql, keys);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */
  @Override
  public boolean execute(String sql, String[] keys) throws SQLException {
    return parent.execute(sql, keys);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeBatch()
   */
  @Override
  public int[] executeBatch() throws SQLException {
    return parent.executeBatch();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeQuery(java.lang.String)
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {

    CosmosSQLLexer lexer = new CosmosSQLLexer(new ANTLRStringStream(sql));

    CommonTokenStream tokens = new CommonTokenStream(lexer);

    CosmosSQLParser parser = new CosmosSQLParser(tokens);

    try {

      if (parser.failed())
        return parent.executeQuery(sql);
      CosmosSQLParser.cosmos_specific_return r = parser.cosmos_specific();

      CommonTree tree = (CommonTree) r.getTree();

      CommonTreeNodeStream nodes = new CommonTreeNodeStream(tree);

      CosmosResultSet walker = new CosmosResultSet(nodes);

      ResultSetRule rule = walker.cosmos_specific();

      return rule.execute(parent, parent.getConnection().getRootSchema());

    } catch (Exception e) {

      e.printStackTrace();
    }

    return parent.executeQuery(sql);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String)
   */
  @Override
  public int executeUpdate(String sql) throws SQLException {
    return parent.executeUpdate(sql);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */
  @Override
  public int executeUpdate(String sql, int autoGenKey) throws SQLException {
    return parent.executeUpdate(sql, autoGenKey);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */
  @Override
  public int executeUpdate(String sql, int[] keys) throws SQLException {
    return parent.executeUpdate(sql, keys);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */
  @Override
  public int executeUpdate(String sql, String[] keys) throws SQLException {
    return parent.executeUpdate(sql, keys);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getConnection()
   */
  @Override
  public Connection getConnection() throws SQLException {
    return parent.getConnection();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getFetchDirection()
   */
  @Override
  public int getFetchDirection() throws SQLException {
    return parent.getFetchDirection();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getFetchSize()
   */
  @Override
  public int getFetchSize() throws SQLException {
    return parent.getFetchSize();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getGeneratedKeys()
   */
  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return parent.getGeneratedKeys();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMaxFieldSize()
   */
  @Override
  public int getMaxFieldSize() throws SQLException {
    return parent.getMaxFieldSize();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMaxRows()
   */
  @Override
  public int getMaxRows() throws SQLException {
    return parent.getMaxRows();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMoreResults()
   */
  @Override
  public boolean getMoreResults() throws SQLException {
    return parent.getMoreResults();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMoreResults(int)
   */
  @Override
  public boolean getMoreResults(int sql) throws SQLException {
    return parent.getMoreResults(sql);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getQueryTimeout()
   */
  @Override
  public int getQueryTimeout() throws SQLException {
    return parent.getQueryTimeout();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSet()
   */
  @Override
  public ResultSet getResultSet() throws SQLException {
    return parent.getResultSet();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetConcurrency()
   */
  @Override
  public int getResultSetConcurrency() throws SQLException {
    return parent.getResultSetConcurrency();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetHoldability()
   */
  @Override
  public int getResultSetHoldability() throws SQLException {
    return parent.getResultSetHoldability();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetType()
   */
  @Override
  public int getResultSetType() throws SQLException {
    return parent.getResultSetType();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getUpdateCount()
   */
  @Override
  public int getUpdateCount() throws SQLException {
    return parent.getUpdateCount();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getWarnings()
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    return parent.getWarnings();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#isClosed()
   */
  @Override
  public boolean isClosed() throws SQLException {
    return parent.isClosed();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#isPoolable()
   */
  @Override
  public boolean isPoolable() throws SQLException {
    return parent.isPoolable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setCursorName(java.lang.String)
   */
  @Override
  public void setCursorName(String sql) throws SQLException {
    parent.setCursorName(sql);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  @Override
  public void setEscapeProcessing(boolean sql) throws SQLException {
    parent.setEscapeProcessing(sql);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setFetchDirection(int)
   */
  @Override
  public void setFetchDirection(int dir) throws SQLException {
    parent.setFetchDirection(dir);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setFetchSize(int)
   */
  @Override
  public void setFetchSize(int size) throws SQLException {
    parent.setFetchSize(size);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setMaxFieldSize(int)
   */
  @Override
  public void setMaxFieldSize(int size) throws SQLException {
    parent.setMaxFieldSize(size);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setMaxRows(int)
   */
  @Override
  public void setMaxRows(int rows) throws SQLException {
    setMaxRows(rows);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setPoolable(boolean)
   */
  @Override
  public void setPoolable(boolean pools) throws SQLException {
    parent.setPoolable(pools);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setQueryTimeout(int)
   */
  @Override
  public void setQueryTimeout(int timeout) throws SQLException {
    parent.setQueryTimeout(timeout);

  }

}
