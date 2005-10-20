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
		private IntPtr  m_resultset = IntPtr.Zero;

		/* 
		 * Keep track of the connection in order to implement the
		 * CommandBehavior.CloseConnection flag. A null reference means
		 * normal behavior (do not automatically close).
		 */
		//private MaxDBConnection m_connection = null;

		/*
		 * Because the user should not be able to directly create a 
		 * DataReader object, the constructors are
		 * marked as internal.
		 */
		internal MaxDBDataReader(IntPtr resultset)
		{
			m_resultset = resultset;
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
			return false;
		}

		public bool Read()
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

		public DataTable GetSchemaTable()
		{
			DataTable schema = new DataTable("SchemaTable");
			
			DataColumn dcID = new DataColumn("id", typeof(int));
			dcID.AutoIncrement = true;
			dcID.AutoIncrementSeed = 1;
			schema.Columns.Add(dcID);

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

			for (int cnt = 0; cnt <= FieldCount - 1; cnt++)
			{
				DataRow row = schema.NewRow();
				row["ColumnName"] = GetName(cnt);
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = SQLDBC.SQLDBC_ResultSetMetaData_getPhysicalLength(meta, (short)(cnt + 1));
				row["NumericPrecision"] = SQLDBC.SQLDBC_ResultSetMetaData_getPrecision(meta, (short)(cnt + 1));
				row["NumericScale"] = SQLDBC.SQLDBC_ResultSetMetaData_getScale(meta, (short)(cnt + 1));
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
				row["AllowDBNull"] = (SQLDBC.SQLDBC_ResultSetMetaData_isNullable(meta, (short)(cnt + 1)) == ColumnNullBehavior.columnNullable);
				row["IsReadOnly"] = (SQLDBC.SQLDBC_ResultSetMetaData_isWritable(meta, (short)(cnt + 1)) == 0);
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
			return Encoding.Unicode.GetString(GetNameBytes((short)(i + 1))).TrimEnd('\0');
		}

		private unsafe byte[] GetNameBytes(short pos)
		{
			byte[] columnName = new byte[sizeof(char)];
			int len = columnName.Length;

			SQLDBC_Retcode rc;

			fixed(byte *namePtr = columnName)
			{
				rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), pos, 
					new IntPtr(namePtr), StringEncodingType.UCS2Swapped, len, ref len);
				if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
					throw new MaxDBException("Can't not allocate buffer for the column name");
			}

			len += sizeof(char);
			columnName = new byte[len];

			fixed(byte *namePtr = columnName)
			{
				rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), pos, 
					new IntPtr(namePtr), StringEncodingType.UCS2Swapped, len, ref len);

				if (rc != SQLDBC_Retcode.SQLDBC_OK)
					throw new MaxDBException("Can't not get column name");
			}

			return columnName;
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
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					return typeof(string);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					return typeof(byte[]);
				default:
					return typeof(object);
			}
		}

		private unsafe byte[] GetValueBytes(int i, out SQLDBC_SQLType columnType)
		{
			int val_length;
			SQLDBC_Retcode rc;
			columnType = SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1));
			switch(columnType)
			{
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
					byte byte_val;
					val_length = sizeof(byte);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT1, new IntPtr(&byte_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return new byte[]{byte_val};
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
					byte[] dt_val = new byte[sizeof(ODBCDATE)];
					val_length = dt_val.Length;
					fixed(byte *dt_ptr = dt_val)
					{
						if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, new IntPtr(dt_ptr), 
							ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
							throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return dt_val;
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
					byte[] tm_val = new byte[sizeof(ODBCTIME)];
					val_length = tm_val.Length;
					fixed(byte* tm_ptr = tm_val)
					{
						if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, new IntPtr(tm_ptr), 
							ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
							throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return tm_val;
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
					byte[] ts_val = new byte[sizeof(ODBCTIMESTAMP)];
					val_length = ts_val.Length;
					fixed(byte *ts_ptr = ts_val)
					{
						if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, new IntPtr(ts_ptr), 
							ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
							throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return ts_val;
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
					double double_val;
					val_length = sizeof(double);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, new IntPtr(&double_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(double_val);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
					int int_val;
					val_length = sizeof(int);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, new IntPtr(&int_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(int_val);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
					short short_val;
					val_length = sizeof(short);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, new IntPtr(&short_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(short_val);
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
					byte[] columnValue = new byte[sizeof(char)];
					val_length = 0;

					fixed(byte *valuePtr = columnValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (val_length == SQLDBC.SQLDBC_NULL_DATA)
							return null;

						if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
							throw new MaxDBException("Error getObject: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					columnValue = new byte[val_length];

					fixed(byte *valuePtr = columnValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (rc != SQLDBC_Retcode.SQLDBC_OK)
							throw new MaxDBException("Error getObject: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					return columnValue;
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
				case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					byte[] binValue = new byte[1];
					val_length = 0;

					fixed(byte *valuePtr = binValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (val_length == SQLDBC.SQLDBC_NULL_DATA)
							return null;

						if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
							throw new MaxDBException("Error getObject: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					binValue = new byte[val_length];

					fixed(byte *valuePtr = binValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (rc != SQLDBC_Retcode.SQLDBC_OK)
							throw new MaxDBException("Error getObject: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					return binValue;
				default:
					return null;
			}
		}

		public object GetValue(int i)
		{
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return (data[0] == 1);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return new DateTime(dt_val.year, dt_val.month, dt_val.day);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, tm_val.hour, tm_val.minute, tm_val.second);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, (int)(ts_val.fraction/1000000));
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return Encoding.Unicode.GetString(data);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
						return data;
					default:
						return DBNull.Value;
				}
			}
			else
				return DBNull.Value; 
		}

		public int GetValues(object[] values)
		{
			for (int i = 0; i < Math.Min(this.FieldCount, values.Length); i++)
				values[i] = GetValue(i);
			return Math.Min(this.FieldCount, values.Length);
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
				return GetValue(i);
			}
		}

		public object this [ String name ]
		{
			// Look up the ordinal and return 
			// the value at that position.
			get { return GetValue(GetOrdinal(name)); }
		}

		public bool GetBoolean(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return (data[0] == 1);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to boolean");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to boolean");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to boolean");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (BitConverter.ToDouble(data, 0) == 1);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return (BitConverter.ToInt32(data, 0) == 1);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return (BitConverter.ToInt16(data, 0) == 1);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return bool.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the boolean value since column's value is NULL");
		}

		public byte GetByte(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			return (byte)GetValue(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			if (buffer.Length - bufferoffset > length)
				length = buffer.Length - bufferoffset;

			SQLDBC_SQLType columnType;
			byte[] fieldBytes = GetValueBytes(i, out columnType);
			
			long length_to_copy = length;
			
			if (fieldBytes.LongLength - fieldOffset > length_to_copy)
				length_to_copy = fieldBytes.LongLength - fieldOffset;
			Array.Copy(fieldBytes, fieldOffset, buffer, bufferoffset, length_to_copy);
			
			return length_to_copy;
		}

		public char GetChar(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			return (char)GetValue(i);
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			const int char_size = 2;
			byte[] byte_buffer = new byte[buffer.LongLength * char_size];
			long copied_chars = GetBytes(i, fieldoffset * char_size, byte_buffer, bufferoffset * char_size, length * char_size) / char_size;
			for (i = bufferoffset; i < bufferoffset + copied_chars; i++)
				buffer[i] = BitConverter.ToChar(byte_buffer, i * char_size);
			return copied_chars;
		}

		public Guid GetGuid(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			throw new NotImplementedException();
		}

		public short GetInt16(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Int16");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Int16");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int16");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (short)BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return (short)BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return short.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int16 value since column's value is NULL");
		}

		public int GetInt32(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Int32");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Int32");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int32");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (int)BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return int.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int32 value since column's value is NULL");
		}

		public long GetInt64(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Int64");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Int64");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int64");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (long)BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return long.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int64 value since column's value is NULL");
		}

		public float GetFloat(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Float");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Float");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Float");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (float)BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return float.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Float value since column's value is NULL");
		}

		public double GetDouble(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Double");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Double");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Double");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return double.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Double value since column's value is NULL");
		}

		public string GetString(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return (data[0] == 1).ToString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return (new DateTime(dt_val.year, dt_val.month, dt_val.day)).ToShortDateString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return (new DateTime(0, 0, 0, tm_val.hour, tm_val.minute, tm_val.second)).ToShortTimeString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return (new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, 
							(int)(ts_val.fraction/1000000))).ToString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return BitConverter.ToDouble(data, 0).ToString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0).ToString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0).ToString();
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return Encoding.Unicode.GetString(data);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
						return Encoding.ASCII.GetString(data);
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the String value since column's value is NULL");
		}

		public decimal GetDecimal(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						return data[0];
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						throw new InvalidCastException("Can't convert date value to Decimal");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						throw new InvalidCastException("Can't convert time value to Decimal");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Decimal");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						return (decimal)BitConverter.ToDouble(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						return BitConverter.ToInt32(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return decimal.Parse(Encoding.Unicode.GetString(data));
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
						return decimal.Parse(Encoding.ASCII.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Decimal value since column's value is NULL");
		}

		public DateTime GetDateTime(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			SQLDBC_SQLType columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_BOOLEAN:
						throw new InvalidCastException("Can't convert Boolean value to DateTime");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return new DateTime(dt_val.year, dt_val.month, dt_val.day);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return new DateTime(0, 0, 0, tm_val.hour, tm_val.minute, tm_val.second);
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, 
							(int)(ts_val.fraction/1000000));
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FIXED:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_FLOAT:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VFLOAT:
						throw new InvalidCastException("Can't convert Double value to DateTime");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_INTEGER:
						throw new InvalidCastException("Can't convert Int32 value to DateTime");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_SMALLINT:
						throw new InvalidCastException("Can't convert Int16 value to DateTime");
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARUNI:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHA:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_UNICODE:
						return DateTime.Parse(Encoding.Unicode.GetString(data));
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_STRB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_VARCHARB:
					case SQLDBC_SQLType.SQLDBC_SQLTYPE_CHB:
						return DateTime.Parse(Encoding.ASCII.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the DateTime value since column's value is NULL");

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
			return (GetValue(i) == DBNull.Value);
		}

		/*
		 * Implementation specific methods.
		 */

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
					throw new SystemException("An exception of type " + e.GetType() + " was encountered while closing the MaxDBDataReader.");
				}
			}
		}

	}
}

