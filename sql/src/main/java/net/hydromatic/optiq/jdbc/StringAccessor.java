package net.hydromatic.optiq.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.optiq.Schema.TableInSchema;
import net.hydromatic.optiq.runtime.Cursor.Accessor;

public class StringAccessor implements Accessor {
  private String value;
  private Iterator<Entry<String,TableInSchema>> iter;

  public StringAccessor(String value) {
    this.value = value;
  }

  /**
   * @param blahIter
   */
  public StringAccessor(Iterator<Entry<String,TableInSchema>> blahIter) {
    iter = blahIter;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getString()
   */
  @Override
  public String getString() {
    return iter.next().getKey();
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    // TODO Auto-generated method stub
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getByte()
   */
  @Override
  public byte getByte() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getShort()
   */
  @Override
  public short getShort() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getInt()
   */
  @Override
  public int getInt() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getLong()
   */
  @Override
  public long getLong() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getFloat()
   */
  @Override
  public float getFloat() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDouble()
   */
  @Override
  public double getDouble() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBigDecimal()
   */
  @Override
  public BigDecimal getBigDecimal() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBigDecimal(int)
   */
  @Override
  public BigDecimal getBigDecimal(int scale) {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBytes()
   */
  @Override
  public byte[] getBytes() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDate()
   */
  @Override
  public Date getDate() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTime()
   */
  @Override
  public Time getTime() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTimestamp()
   */
  @Override
  public Timestamp getTimestamp() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getAsciiStream()
   */
  @Override
  public InputStream getAsciiStream() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getUnicodeStream()
   */
  @Override
  public InputStream getUnicodeStream() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBinaryStream()
   */
  @Override
  public InputStream getBinaryStream() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject()
   */
  @Override
  public Object getObject() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getCharacterStream()
   */
  @Override
  public Reader getCharacterStream() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject(java.util.Map)
   */
  @Override
  public Object getObject(Map<String,Class<?>> map) {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getRef()
   */
  @Override
  public Ref getRef() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getBlob()
   */
  @Override
  public Blob getBlob() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getClob()
   */
  @Override
  public Clob getClob() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getArray()
   */
  @Override
  public Array getArray() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getDate(java.util.Calendar)
   */
  @Override
  public Date getDate(Calendar cal) {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTime(java.util.Calendar)
   */
  @Override
  public Time getTime(Calendar cal) {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getTimestamp(java.util.Calendar)
   */
  @Override
  public Timestamp getTimestamp(Calendar cal) {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getURL()
   */
  @Override
  public URL getURL() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNClob()
   */
  @Override
  public NClob getNClob() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getSQLXML()
   */
  @Override
  public SQLXML getSQLXML() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNString()
   */
  @Override
  public String getNString() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getNCharacterStream()
   */
  @Override
  public Reader getNCharacterStream() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.hydromatic.optiq.runtime.Cursor.Accessor#getObject(java.lang.Class)
   */
  @Override
  public <T> T getObject(Class<T> type) {
    // TODO Auto-generated method stub
    return null;
  }
}
