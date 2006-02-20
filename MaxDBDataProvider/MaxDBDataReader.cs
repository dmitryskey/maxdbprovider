using System;
using System.Data;
using System.Text;
using System.IO;
using System.Globalization;
using System.Collections;

#if NATIVE
using MaxDBDataProvider.MaxDBProtocol;
#endif

namespace MaxDBDataProvider
{
	public class MaxDBDataReader : IDataReader, IDataRecord 
#if NATIVE 
		, ISQLParamController 
#endif
	{
		// The DataReader should always be open when returned to the user.
		private bool m_fOpen = true;

#if NATIVE
		// The default fetch size to use if none is specified.
		public const int DEFAULT_FETCHSIZE = 30000;

		// The fetch details.
		private FetchInfo       fetchInfo;

		// The command that generated this result set.
		private MaxDBCommand    command;

		// The fetch size that is set.
		private int             fetchSize;

		// The data of the last fetch operation.
		private FetchChunk      currentChunk;

		// The status of the position, i.e. one of the <code>POSITION_XXX</code> constants.
		private PositionType	positionState;

		// The status of the current chunk.
		private PositionType    positionStateOfChunk;

		private bool	        empty;                 // is this result set totally empty

		private ArrayList       openStreams;           // a vector of all streams that went outside.
		private int             rowsInResultSet;       // the number of rows in this result set, or -1 if not known

		private int             safeFetchSize;         // The fetch size that is known to be good.
		// This one is used when going backwards in the result set.
		private bool			safeFetchSizeDetermined; 

		private int             largestKnownAbsPos;    // largest known absolute position to be inside.

		private int             modifiedKernelPos;     // contains 0 if the kernel pos is not modified
		// or the current kernel position.

		//connection handle
		private MaxDBConnection m_connection;

		internal MaxDBDataReader()
		{
			empty = true;
			m_connection = null;
		}

		internal MaxDBDataReader(MaxDBConnection connection, FetchInfo fetchInfo, MaxDBCommand  command, MaxDBReplyPacket reply)
		{
			m_connection = connection;
			this.fetchInfo = fetchInfo;
			this.command = command;
			this.fetchSize = DEFAULT_FETCHSIZE;

			m_fOpen = true;
	
			InitializeFields();
			openStreams = new ArrayList(5);
			if (reply != null)
			{
				SetCurrentChunk(new FetchChunk(
					FetchType.FIRST,		// fetch first is forward
					1,						// absolute start position
					reply,					// reply packet
					fetchInfo.RecordSize,	// the size for data part navigation condition in that case
					rowsInResultSet
					));
				positionState = PositionType.BEFORE_FIRST;
			}
		}

		internal MaxDBDataReader(MaxDBConnection connection, string cursorName, DBTechTranslator[] infos, string[] columnNames, 
			MaxDBCommand command, MaxDBReplyPacket reply) :
			this(connection, new FetchInfo(connection, cursorName, infos, columnNames), command, reply)
		{
		}

		private void InitializeFields()
		{
			currentChunk = null;
			positionState = PositionType.BEFORE_FIRST;
			positionStateOfChunk = PositionType.NOT_AVAILABLE;
			empty = false;
			safeFetchSize = 1;
			safeFetchSizeDetermined = false;
			largestKnownAbsPos = 1;
			rowsInResultSet = -1;
			modifiedKernelPos = 0;
		}

		private void SetCurrentChunk(FetchChunk newChunk)
		{
			positionState = positionStateOfChunk = PositionType.INSIDE;
			currentChunk = newChunk;
			int safe_fetchsize = Math.Min(fetchSize, Math.Max(newChunk.Size, safeFetchSize));
			if(safeFetchSize != safe_fetchsize) 
			{
				safeFetchSize = safe_fetchsize;
				safeFetchSizeDetermined = false;
			} 
			else 
				safeFetchSizeDetermined = safe_fetchsize != 1;
			modifiedKernelPos = 0; // clear this out, until someone will de
			updateRowStatistics();
		}

		private void updateRowStatistics()
		{
			if(!RowsInResultSetKnown) 
			{
				// If this is the one and only chunk, yes then we
				// have only the records in this chunk.
				if(currentChunk.IsLast && currentChunk.IsFirst) 
				{
					rowsInResultSet = currentChunk.Size;
					currentChunk.RowsInResultSet = rowsInResultSet;
				}
					// otherwise, we may have navigated through it from start ...
				else if(currentChunk.IsLast && currentChunk.IsForward) 
				{
					rowsInResultSet = currentChunk.End;
					currentChunk.RowsInResultSet = rowsInResultSet;
				}
					// ... or from end
				else if(currentChunk.IsFirst && !currentChunk.IsForward) 
				{
					rowsInResultSet = -currentChunk.Start;
					currentChunk.RowsInResultSet = rowsInResultSet;
				} 
				else if (currentChunk.IsForward) 
					largestKnownAbsPos = Math.Max(largestKnownAbsPos, currentChunk.End);
			}
		}

		private bool RowsInResultSetKnown
		{
			get
			{
				return rowsInResultSet != -1;
			}
		}

		internal bool Empty
		{
			get
			{
				return empty;
			}
			set
			{
				empty = value;
			}
		}

#else
		private IntPtr  m_resultset = IntPtr.Zero;
		internal MaxDBDataReader(IntPtr resultset)
		{
			m_resultset = resultset;
		}
#endif

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
			get
			{ 
				return !m_fOpen; 
			}
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
			m_fOpen = false;
#if NATIVE
			currentChunk = null;
			fetchInfo = null;
#else
			SQLDBC.SQLDBC_ResultSet_close(m_resultset);
#endif
		}

		public bool NextResult()
		{
			return false;
		}

		public bool Read()
		{
#if NATIVE
			AssertNotClosed();
			// if we have nothing, there is nothing to do.
			if(empty) 
			{
				this.positionState = PositionType.AFTER_LAST;
				return false;
			}

			bool result = false;

			// at first we have to close all input streams
			CloseOpenStreams();
        
			// if we are outside, ...
			if(positionState == PositionType.BEFORE_FIRST) 
			{
				// ... check whether we still have it
				if(positionStateOfChunk == PositionType.INSIDE && currentChunk.ContainsRow(1)) 
				{
					currentChunk.setRow(1);
					positionState = PositionType.INSIDE;
					result = true;
				} 
				else 
					result = FetchFirst();
			} 
			else if(positionState == PositionType.INSIDE) 
			{
				if(currentChunk.Move(1)) 
					result = true;
				else 
				{
					if(currentChunk.IsLast) 
					{
						positionState = PositionType.AFTER_LAST;
						return false;
					}
					result = FetchNextChunk();
				}
			} 
			else if(positionState == PositionType.AFTER_LAST) 
			{
				//
			}

			return result;
#else
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
#endif
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

#if NATIVE
			for (int cnt = 0; cnt < FieldCount; cnt++)
			{
				DBTechTranslator info = fetchInfo.GetColumnInfo(cnt);
				DataRow row = schema.NewRow();

				row["ColumnName"] = info.ColumnName;
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = info.PhysicalLength;
				row["NumericPrecision"] = info.Precision;
				row["NumericScale"] = info.Scale;
				row["DataType"] = GetFieldType(cnt);
				row["IsLong"] = info.IsLongKind;
				row["AllowDBNull"] = info.IsNullable;
				row["IsReadOnly"] = !info.IsWritable;
				schema.Rows.Add(row);
			}
#else
			IntPtr meta = SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset);

			for (int cnt = 0; cnt < FieldCount; cnt++)
			{
				DataRow row = schema.NewRow();
				row["ColumnName"] = GetName(cnt);
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = SQLDBC.SQLDBC_ResultSetMetaData_getPhysicalLength(meta, (short)(cnt + 1));
				row["NumericPrecision"] = SQLDBC.SQLDBC_ResultSetMetaData_getPrecision(meta, (short)(cnt + 1));
				row["NumericScale"] = SQLDBC.SQLDBC_ResultSetMetaData_getScale(meta, (short)(cnt + 1));
				row["DataType"] = GetFieldType(cnt);
				row["IsLong"] = GeneralColumnInfo.IsLong(
					SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(cnt + 1)));
				row["AllowDBNull"] = (SQLDBC.SQLDBC_ResultSetMetaData_isNullable(meta, (short)(cnt + 1)) == ColumnNullBehavior.columnNullable);
				row["IsReadOnly"] = (SQLDBC.SQLDBC_ResultSetMetaData_isWritable(meta, (short)(cnt + 1)) == 0);
				schema.Rows.Add(row);
			}
#endif

			return schema;
		}

		/****
		 * METHODS / PROPERTIES FROM IDataRecord.
		 ****/
		public int FieldCount
		{
			// Return the count of the number of columns, which in
			// this case is the size of the column metadata array.
			get
			{
#if NATIVE
				return fetchInfo.NumberOfColumns;
#else
				return SQLDBC.SQLDBC_ResultSetMetaData_getColumnCount(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset)); 
#endif
			}
		}

		public string GetName(int i)
		{
#if NATIVE
			return fetchInfo.GetColumnInfo(i).ColumnName;
#else
			return Encoding.Unicode.GetString(GetNameBytes((short)(i + 1))).TrimEnd('\0');
#endif
		}

		public string GetDataTypeName(int i)
		{
			/*
			 * Usually this would return the name of the type
			 * as used on the back end, for example 'smallint' or 'varchar'.
			 * The sample returns the simple name of the .NET Framework type.
			 */
#if NATIVE
			return fetchInfo.GetColumnInfo(i).ColumnTypeName;
#else
			return GeneralColumnInfo.GetTypeName(
				SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1)));
#endif
		}

		public Type GetFieldType(int i)
		{
#if NATIVE
			return fetchInfo.GetColumnInfo(i).ColumnType;
#else
			return GeneralColumnInfo.GetType(
				SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1)));
#endif
		}

		public object GetValue(int i)
		{
#if NATIVE
			DBTechTranslator info = FindColumnInfo(i);
			return info.IsDBNull(CurrentRecord)? DBNull.Value : FindColumnInfo(i).GetValue(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return (data[0] == 1);
					case DataType.DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return new DateTime(dt_val.year, dt_val.month, dt_val.day);
					case DataType.TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, tm_val.hour, tm_val.minute, tm_val.second);
					case DataType.TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, (int)(ts_val.fraction/1000000)).AddTicks((int)(ts_val.fraction/100000));
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.UNICODE:
						return Encoding.Unicode.GetString(data);
					case DataType.STRB:
					case DataType.VARCHARB:
					case DataType.CHB:
						return data;
					default:
						return DBNull.Value;
				}
			}
			else
				return DBNull.Value; 
#endif
		}

		public int GetValues(object[] values)
		{
			for (int i = 0; i < Math.Min(FieldCount, values.Length); i++)
				values[i] = GetValue(i);
			return Math.Min(FieldCount, values.Length);
		}

		public int GetOrdinal(string name)
		{
			// Throw an exception if the ordinal cannot be found.
			for (short cnt = 0; cnt <= FieldCount - 1; cnt++)
				if (GetName(cnt).Trim().ToUpper() == name.Trim().ToUpper())
					return cnt;
			throw new MaxDBException("Can't find field '" + name + "'");
		}

		public object this[int i]
		{
			get
			{
				return GetValue(i);
			}
		}

		public object this[string name]
		{
			// Look up the ordinal and return the value at that position.
			get { return GetValue(GetOrdinal(name)); }
		}

		public bool GetBoolean(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetBoolean(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return (data[0] == 1);
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to boolean");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to boolean");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to boolean");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (BitConverter.ToDouble(data, 0) == 1);
					case DataType.INTEGER:
						return (BitConverter.ToInt32(data, 0) == 1);
					case DataType.SMALLINT:
						return (BitConverter.ToInt16(data, 0) == 1);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return bool.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the boolean value since column's value is NULL");
#endif
		}

		public byte GetByte(int i)
		{
#if NATIVE
			return FindColumnInfo(i).GetByte(this, CurrentRecord);
#else
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			return (byte)GetValue(i);
#endif
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			//TO DO: need to be optimized
			if (buffer.Length - bufferoffset > length)
				length = buffer.Length - bufferoffset;

#if NATIVE
			byte[] fieldBytes = FindColumnInfo(i).GetBytes(this, CurrentRecord);
#else
			int columnType;
			byte[] fieldBytes = GetValueBytes(i, out columnType);
#endif			
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
			//TO DO: need to be optimized
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
			return new Guid(GetString(i));
		}

		public short GetInt16(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetInt16(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Int16");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Int16");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int16");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (short)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return (short)BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return short.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int16 value since column's value is NULL");
#endif
		}

		public int GetInt32(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetInt32(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Int32");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Int32");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int32");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (int)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return int.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int32 value since column's value is NULL");
#endif
		}

		public long GetInt64(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetInt64(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Int64");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Int64");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Int64");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (long)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return long.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Int64 value since column's value is NULL");
#endif
		}

		public float GetFloat(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetFloat(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Float");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Float");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Float");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (float)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return float.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Float value since column's value is NULL");
#endif
		}

		public double GetDouble(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetDouble(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Double");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Double");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Double");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.CHB:
					case DataType.UNICODE:
						return double.Parse(Encoding.Unicode.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Double value since column's value is NULL");
#endif
		}

		public string GetString(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetString(this, CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return (data[0] == 1).ToString();
					case DataType.DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return (new DateTime(dt_val.year, dt_val.month, dt_val.day)).ToShortDateString();
					case DataType.TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return (new DateTime(0, 0, 0, tm_val.hour, tm_val.minute, tm_val.second)).ToShortTimeString();
					case DataType.TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return (new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, 
							(int)(ts_val.fraction/1000000))).ToString();
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return BitConverter.ToDouble(data, 0).ToString();
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0).ToString();
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0).ToString();
					case DataType.STRA:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.UNICODE:
						return Encoding.Unicode.GetString(data);
					case DataType.STRB:
					case DataType.VARCHARB:
					case DataType.CHB:
						return Encoding.ASCII.GetString(data);
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the String value since column's value is NULL");
#endif
		}

		public decimal GetDecimal(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetDecimal(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.DATE:
						throw new InvalidCastException("Can't convert date value to Decimal");
					case DataType.TIME:
						throw new InvalidCastException("Can't convert time value to Decimal");
					case DataType.TIMESTAMP:
						throw new InvalidCastException("Can't convert timestamp value to Decimal");
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (decimal)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRA:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.UNICODE:
						return decimal.Parse(Encoding.Unicode.GetString(data));
					case DataType.STRB:
					case DataType.VARCHARB:
					case DataType.CHB:
						return decimal.Parse(Encoding.ASCII.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the Decimal value since column's value is NULL");
#endif
		}

		public DateTime GetDateTime(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
#if NATIVE
			return FindColumnInfo(i).GetDateTime(CurrentRecord);
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						throw new InvalidCastException("Can't convert Boolean value to DateTime");
					case DataType.DATE:
						ODBCDATE dt_val = ODBCConverter.GetDate(data);
						return new DateTime(dt_val.year, dt_val.month, dt_val.day);
					case DataType.TIME:
						ODBCTIME tm_val = ODBCConverter.GetTime(data);
						return new DateTime(0, 0, 0, tm_val.hour, tm_val.minute, tm_val.second);
					case DataType.TIMESTAMP:
						ODBCTIMESTAMP ts_val = ODBCConverter.GetTimeStamp(data);
						return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, 
							(int)(ts_val.fraction/1000000));
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						throw new InvalidCastException("Can't convert Double value to DateTime");
					case DataType.INTEGER:
						throw new InvalidCastException("Can't convert Int32 value to DateTime");
					case DataType.SMALLINT:
						throw new InvalidCastException("Can't convert Int16 value to DateTime");
					case DataType.STRA:
					case DataType.STRUNI:
					case DataType.VARCHARA:
					case DataType.VARCHARUNI:
					case DataType.CHA:
					case DataType.UNICODE:
						return DateTime.Parse(Encoding.Unicode.GetString(data));
					case DataType.STRB:
					case DataType.VARCHARB:
					case DataType.CHB:
						return DateTime.Parse(Encoding.ASCII.GetString(data));
					default:
						throw new InvalidCastException("Unknown column type");
				}
			}
			else
				throw new InvalidCastException("Can't get the DateTime value since column's value is NULL");
#endif
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

#if NATIVE
		#region "Methods to support native protocol"

		MaxDBConnection ISQLParamController.Connection
		{
			get
			{
				return m_connection;
			}
		}

		ByteArray ISQLParamController.ReplyData
		{
			get
			{
				return (currentChunk != null) ? currentChunk.ReplyData : null;
			}
		}

		private void AssertNotClosed()
		{
			if(!m_fOpen) 
				throw new ObjectIsClosedException();
		}

		private void CloseOpenStreams()
		{
			foreach (object obj in openStreams)
			{
				try 
				{
					try 
					{
						Stream stream = (Stream)obj;
						stream.Close();
					} 
					catch(InvalidCastException) 
					{
						TextReader r = (TextReader)obj;
						r.Close();
					}
				} 
				catch(IOException) 
				{
					// ignore
				}
			}
			openStreams.Clear();
		}

		/*
			Executes a FETCH FIRST, and stores the result internally.
			@return true if the cursor is positioned correctly.
		*/
		private bool FetchFirst()
		{
			MaxDBReplyPacket reply;

			int usedFetchSize = this.fetchSize;

			try 
			{
				reply=fetchInfo.ExecFetchNext(usedFetchSize);
			} 
			catch(MaxDBSQLException sqlEx) 
			{
				if(sqlEx.VendorCode == 100) 
				{
					this.empty = true;
					this.positionState = PositionType.AFTER_LAST;
					this.currentChunk = null;
				} 
				else 
					throw;
				return false;
			}
			SetCurrentChunk(new FetchChunk(
				FetchType.FIRST,		// fetch first is forward
				1,						// absolute start position
				reply,					// reply packet
				fetchInfo.RecordSize,	// the size for data part navigation
				rowsInResultSet));
			return true;
		}

		/*
			Fetch the next chunk, moving forward over the result set.
		*/
		private bool FetchNextChunk()
		{
			MaxDBReplyPacket reply;

			int usedFetchSize = this.fetchSize;
			int usedOffset=1;


			if(currentChunk.IsForward) 
				if(modifiedKernelPos != 0) 
					usedOffset +=  currentChunk.End - modifiedKernelPos;
				else 
				{
					// if an update destroyed the cursor position, we have to honor this ...
					if(modifiedKernelPos == 0) 
						usedOffset +=  currentChunk.End - currentChunk.KernelPos;
					else 
						usedOffset +=  currentChunk.End - modifiedKernelPos;
				}

			try 
			{
				reply = fetchInfo.ExecFetchNext(usedFetchSize);
			} 
			catch(MaxDBSQLException sqlEx) 
			{
				if(sqlEx.VendorCode == 100) 
				{
					// fine, we are at the end.
					currentChunk.IsLast = true;
					updateRowStatistics();
					// but invalidate it, as it is thrown away by the kernel
					currentChunk = null;
					positionStateOfChunk = PositionType.NOT_AVAILABLE;
					positionState = PositionType.AFTER_LAST;
					return false;
				}
				throw;
			}
			SetCurrentChunk(new FetchChunk(
				FetchType.RELATIVE_UP,
				currentChunk.End + 1,
				reply,
				fetchInfo.RecordSize,
				rowsInResultSet));
			return true;
		}

		private ByteArray CurrentRecord
		{
			get
			{
				if(positionState == PositionType.BEFORE_FIRST) 
					throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_RESULTSET_BEFOREFIRST));
				if(positionState == PositionType.AFTER_LAST) 
					throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_RESULTSET_AFTERLAST));
				return currentChunk.CurrentRecord;
			}
}

		private DBTechTranslator FindColumnInfo(int colIndex)
		{
			AssertNotClosed();
			DBTechTranslator info;

			try 
			{
				info = fetchInfo.GetColumnInfo(colIndex - 1);
			}
			catch (IndexOutOfRangeException) 
			{
				throw new InvalidColumnException(colIndex);
			}
			return info;
		}

		#endregion
#else
		#region "Unsafe methods"
		
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

		private unsafe byte[] GetValueBytes(int i, out int columnType)
		{
			int val_length;
			SQLDBC_Retcode rc;
			columnType = SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1));
			switch(columnType)
			{
				case DataType.BOOLEAN:
					byte byte_val;
					val_length = sizeof(byte);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT1, new IntPtr(&byte_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return new byte[]{byte_val};
				case DataType.DATE:
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
				case DataType.TIME:
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
				case DataType.TIMESTAMP:
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
				case DataType.FIXED:
				case DataType.FLOAT:
				case DataType.VFLOAT:
					double double_val;
					val_length = sizeof(double);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, new IntPtr(&double_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(double_val);
				case DataType.INTEGER:
					int int_val;
					val_length = sizeof(int);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, new IntPtr(&int_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(int_val);
				case DataType.SMALLINT:
					short short_val;
					val_length = sizeof(short);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, new IntPtr(&short_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(short_val);
				case DataType.STRA:
				case DataType.STRUNI:
				case DataType.VARCHARA:
				case DataType.VARCHARUNI:
				case DataType.CHA:
				case DataType.UNICODE:
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
				case DataType.STRB:
				case DataType.VARCHARB:
				case DataType.CHB:
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

		#endregion
#endif
	}
}

