using System;
using System.Data;
using System.Text;
using System.Globalization;

namespace MaxDBDataProvider
{
	public class MaxDBDataReader : IDataReader, IDataRecord
	{
		// The DataReader should always be open when returned to the user.
		private bool m_fOpen = true;

		// Keep track of the results and position
		// within the resultset (starts prior to first record).
		private IntPtr  m_resultset;

		/* 
		 * Keep track of the connection in order to implement the
		 * CommandBehavior.CloseConnection flag. A null reference means
		 * normal behavior (do not automatically close).
		 */
		private MaxDBConnection m_connection = null;

		/*
		 * Because the user should not be able to directly create a 
		 * DataReader object, the constructors are
		 * marked as internal.
		 */
		internal MaxDBDataReader(IntPtr resultset)
		{
			m_resultset   = resultset;
		}

		/****
		 * METHODS / PROPERTIES FROM IDataReader.
		 ****/
		public int Depth 
		{
			/*
			 * Always return a value of zero if nesting is not supported.
			 */
			get { return 0;  }
		}

		public bool IsClosed
		{
			/*
			 * Keep track of the reader state - some methods should be
			 * disallowed if the reader is closed.
			 */
			get  { return !m_fOpen; }
		}

		public int RecordsAffected 
		{
			/*
			 * RecordsAffected is only applicable to batch statements
			 * that include inserts/updates/deletes. The sample always
			 * returns -1.
			 */
			get { return -1; }
		}

		public void Close()
		{
			/*
			 * Close the reader. The sample only changes the state,
			 * but an actual implementation would also clean up any 
			 * resources used by the operation. For example,
			 * cleaning up any resources waiting for data to be
			 * returned by the server.
			 */
			m_fOpen = false;
			SQLDBC.SQLDBC_ResultSet_close(m_resultset);
		}

		public bool NextResult()
		{
			SQLDBC_Retcode rc = SQLDBC.SQLDBC_ResultSet_next(m_resultset);

			switch (rc)
			{
				case SQLDBC_Retcode.SQLDBC_OK:
					return true;
				case SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND:
					return false;
				default:
					throw new MaxDBException("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
			}
		}

		public bool Read()
		{
			SQLDBC_Retcode rc = SQLDBC.SQLDBC_ResultSet_relative(m_resultset, 0);

			switch (rc)
			{
				case SQLDBC_Retcode.SQLDBC_OK:
					return true;
				case SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND:
					return false;
				default:
					throw new MaxDBException("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
			}
		}

		public DataTable GetSchemaTable()
		{
			DataTable schema = new DataTable("SchemaTable");

			schema.Columns.Add(new DataColumn("ColumnName", typeof(string)));
			schema.Columns.Add(new DataColumn("ColumnOrdinal", typeof(int)));
			schema.Columns.Add(new DataColumn("ColumnSize", typeof(int)));
			schema.Columns.Add(new DataColumn("NumericPrecision", typeof(int)));
			schema.Columns.Add(new DataColumn("NumericScale", typeof(int)));
			schema.Columns.Add(new DataColumn("DataType", typeof(Type)));
			schema.Columns.Add(new DataColumn("IsLong", typeof(bool)));
			schema.Columns.Add(new DataColumn("AllowDBNull", typeof(bool)));
			schema.Columns.Add(new DataColumn("IsReadOnly", typeof(bool)));

			IntPtr meta = SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset);

			for (short cnt = 0; cnt <= FieldCount - 1; cnt++)
			{
				DataRow row = schema.NewRow();
				row["ColumnName"] = GetName(cnt);
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = SQLDBC.SQLDBC_ResultSetMetaData_getPhysicalLength(meta, cnt);
				row["NumericPrecision"] = SQLDBC.SQLDBC_ResultSetMetaData_getPrecision(meta, cnt);
				row["NumericScale"] = SQLDBC.SQLDBC_ResultSetMetaData_getScale(meta, cnt);
				row["DataType"] = GetFieldType(cnt);
				switch(SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(cnt + 1)))
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
						row["IsLong"] = true;
						break;
					default:
						row["IsLong"] = false;
						break;
				}
				row["AllowDBNull"] = (SQLDBC.SQLDBC_ResultSetMetaData_isNullable(meta, cnt) == ColumnNullBehavior.columnNullable);
				row["IsReadOnly"] = (SQLDBC.SQLDBC_ResultSetMetaData_isWritable(meta, cnt) == 0);
				schema.Rows.Add(row);
			}

			return schema;
		}

		/****
		 * METHODS / PROPERTIES FROM IDataRecord.
		 ****/
		public int FieldCount
		{
			// Return the count of the number of columns, which in
			// this case is the size of the column metadata
			// array.
			get {return SQLDBC.SQLDBC_ResultSetMetaData_getColumnCount(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset)); }
		}

		public string GetName(int i)
		{
			byte[] columnName = new byte[0];
			int len = 0;

			SQLDBC_Retcode rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1), columnName, 
				StringEncodingType.UCS2Swapped, len, ref len);

			if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
				throw new MaxDBException("Can't not allocate buffer for the column name");

			len += 2;
			columnName = new byte[len];

			rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), 1, columnName, 
				StringEncodingType.UCS2Swapped, len, ref len);

			if (rc != SQLDBC_Retcode.SQLDBC_OK)
				throw new MaxDBException("Can't not get column name");

			return Encoding.Unicode.GetString(columnName).TrimEnd('\0');
		}

		public string GetDataTypeName(int i)
		{
			/*
			 * Usually this would return the name of the type
			 * as used on the back end, for example 'smallint' or 'varchar'.
			 * The sample returns the simple name of the .NET Framework type.
			 */
			throw new NotImplementedException();
		}

		public Type GetFieldType(int i)
		{
			// Return the actual Type class for the data type.
			switch(SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1)))
			{
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
					return typeof(bool);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					return typeof(byte);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
					return typeof(DateTime);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
					return typeof(double);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
					return typeof(int);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
					return typeof(short);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					return typeof(string);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
					return typeof(char);
				default:
					return typeof(object);
			}
		}

		public Object GetValue(int i)
		{
			throw new NotImplementedException();
		}

		public int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}

		public int GetOrdinal(string name)
		{
			// Throw an exception if the ordinal cannot be found.
			for (short cnt = 0; cnt <= FieldCount - 1; cnt++)
				if (GetName(cnt).Trim().ToUpper() == name.Trim().ToUpper())
					return cnt;
			throw new MaxDBException("Can't find field '" + name + "'");
		}

		public object this [ int i ]
		{
			get
			{
				throw new NotImplementedException();
			}
		}

		public object this [ String name ]
		{
			// Look up the ordinal and return 
			// the value at that position.
			get { throw new NotImplementedException(); }
		}

		public bool GetBoolean(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public byte GetByte(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			// The sample does not support this method.
			throw new NotSupportedException("GetBytes not supported.");
		}

		public char GetChar(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			// The sample does not support this method.
			throw new NotSupportedException("GetChars not supported.");
		}

		public Guid GetGuid(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public Int16 GetInt16(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public Int32 GetInt32(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public Int64 GetInt64(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public float GetFloat(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public double GetDouble(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public String GetString(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public Decimal GetDecimal(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public DateTime GetDateTime(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			*/
			throw new NotImplementedException();
		}

		public IDataReader GetData(int i)
		{
			/*
			 * The sample code does not support this method. Normally,
			 * this would be used to expose nested tables and
			 * other hierarchical data.
			 */
			throw new NotSupportedException("GetData not supported.");
		}

		public bool IsDBNull(int i)
		{
			throw new NotImplementedException();
		}

		/*
		 * Implementation specific methods.
		 */
		private int _cultureAwareCompare(string strA, string strB)
		{
			return CultureInfo.CurrentCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);
		}

		void IDisposable.Dispose() 
		{
			this.Dispose(true);
			System.GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) 
		{
			if (disposing) 
			{
				try 
				{
					this.Close();
				}
				catch (Exception e) 
				{
					throw new SystemException("An exception of type " + e.GetType() + 
						" was encountered while closing the TemplateDataReader.");
				}
			}
		}

	}
}

