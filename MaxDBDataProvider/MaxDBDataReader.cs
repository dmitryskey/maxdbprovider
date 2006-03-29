using System;
using System.Data;
using System.Text;
using System.IO;
using System.Globalization;
using System.Collections;
using MaxDBDataProvider.MaxDBProtocol;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	public sealed class MaxDBDataReader : IDataReader, IDataRecord 
#if SAFE 
		, ISQLParamController 
#endif
	{
		// The DataReader should always be open when returned to the user.
		private bool m_fOpened = true;
		internal bool m_fCloseConn = false; //close connection after data reader closing
		internal bool m_fSchemaOnly = false; //return column information only
		private MaxDBConnection m_connection; //connection handle
		private string	m_updTableName;	// tablename used for updateable resultsets 

#if SAFE
		
		private FetchInfo       m_fetchInfo;	// The fetch details.
		private FetchChunk      m_currentChunk;		// The data of the last fetch operation.
		private PositionType	m_positionState; //the status of the position
		private PositionType    m_positionStateOfChunk;  // The status of the current chunk.
		private bool	        m_empty;                 // is this result set totally empty
		private ArrayList       m_openStreams;           // a vector of all streams that went outside.
		private int             m_rowsInResultSet;       // the number of rows in this result set, or -1 if not known
		private int             m_largestKnownAbsPos;    // largest known absolute position to be inside.
		private int             m_modifiedKernelPos;     // contains 0 if the kernel pos is not modified or the current kernel position.
		internal int			m_maxRows;	//how many rows fetch

		internal MaxDBDataReader()
		{
			m_empty = true;
			m_connection = null;
		}

		internal MaxDBDataReader(MaxDBConnection connection, FetchInfo fetchInfo, MaxDBCommand cmd, int maxRows, MaxDBReplyPacket reply)
		{
			m_connection = connection;
			m_fetchInfo = fetchInfo;

			m_fOpened = true;

			m_maxRows = maxRows;

			m_updTableName = cmd.m_parseInfo.m_updTableName;
	
			InitializeFields();
			m_openStreams = new ArrayList(5);
			if (reply != null)
			{
				SetCurrentChunk(new FetchChunk(
					FetchType.FIRST,		// fetch first is forward
					1,						// absolute start position
					reply,					// reply packet
					fetchInfo.RecordSize,	// the size for data part navigation condition in that case
					maxRows,				// how many rows to fetch
					m_rowsInResultSet
					));
				m_positionState = PositionType.BEFORE_FIRST;
			}
		}

		internal MaxDBDataReader(MaxDBConnection connection, string cursorName, DBTechTranslator[] infos, string[] columnNames, 
			MaxDBCommand command, int maxRows, MaxDBReplyPacket reply) :
			this(connection, new FetchInfo(connection, cursorName, infos, columnNames), command, maxRows, reply)
		{
		}

		private void InitializeFields()
		{
			m_currentChunk = null;
			m_positionState = PositionType.BEFORE_FIRST;
			m_positionStateOfChunk = PositionType.NOT_AVAILABLE;
			m_empty = false;
			m_largestKnownAbsPos = 1;
			m_rowsInResultSet = -1;
			m_modifiedKernelPos = 0;
		}

		private void SetCurrentChunk(FetchChunk newChunk)
		{
			m_positionState = m_positionStateOfChunk = PositionType.INSIDE;
			m_currentChunk = newChunk;
			m_modifiedKernelPos = 0; // clear this out, until someone will de
			UpdateRowStatistics();
		}

		private void UpdateRowStatistics()
		{
			if(!RowsInResultSetKnown) 
			{
				// If this is the one and only chunk, yes then we
				// have only the records in this chunk.
				if(m_currentChunk.IsLast && m_currentChunk.IsFirst) 
				{
					m_rowsInResultSet = m_currentChunk.Size;
					m_currentChunk.RowsInResultSet = m_rowsInResultSet;
				}
					// otherwise, we may have navigated through it from start ...
				else if(m_currentChunk.IsLast && m_currentChunk.IsForward) 
				{
					m_rowsInResultSet = m_currentChunk.End;
					m_currentChunk.RowsInResultSet = m_rowsInResultSet;
				}
					// ... or from end
				else if(m_currentChunk.IsFirst && !m_currentChunk.IsForward) 
				{
					m_rowsInResultSet = -m_currentChunk.Start;
					m_currentChunk.RowsInResultSet = m_rowsInResultSet;
				} 
				else if (m_currentChunk.IsForward) 
					m_largestKnownAbsPos = Math.Max(m_largestKnownAbsPos, m_currentChunk.End);
			}
		}

		private bool RowsInResultSetKnown
		{
			get
			{
				return m_rowsInResultSet != -1;
			}
		}

		internal bool Empty
		{
			get
			{
				return m_empty;
			}
			set
			{
				m_empty = value;
			}
		}

#else
		private IntPtr m_resultset = IntPtr.Zero;
		internal MaxDBDataReader(IntPtr resultset, MaxDBConnection conn, MaxDBCommand cmd, bool closeConn, bool schemaOnly)
		{
			m_resultset = resultset;
			m_connection = conn;
			m_fCloseConn = closeConn;
			m_fSchemaOnly = schemaOnly;
			m_updTableName = cmd.UpdTableName;			 
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
			get 
			{ 
				return 0;  
			}
		}

		public bool IsClosed
		{
			/*
			 * Keep track of the reader state - some methods should be
			 * disallowed if the reader is closed.
			 */
			get
			{ 
				return !m_fOpened; 
			}
		}

		public int RecordsAffected 
		{
			/*
			 * RecordsAffected is only applicable to batch statements
			 * that include inserts/updates/deletes. The sample always
			 * returns -1.
			 */
			get 
			{ 
				return -1; 
			}
		}

		public void Close()
		{
			m_fOpened = false;
#if SAFE
			m_currentChunk = null;
			m_fetchInfo = null;
#else
			SQLDBC.SQLDBC_ResultSet_close(m_resultset);
#endif
			if (m_fCloseConn && m_connection != null)
				m_connection.Close();
		}

		public bool NextResult()
		{
			return false;
		}

		public bool Read()
		{
#if SAFE
			AssertNotClosed();
			// if we have nothing, there is nothing to do.
			if(m_empty || m_fSchemaOnly) 
			{
				m_positionState = PositionType.AFTER_LAST;
				return false;
			}

			bool result = false;

			// at first we have to close all input streams
			CloseOpenStreams();
        
			// if we are outside, ...
			if(m_positionState == PositionType.BEFORE_FIRST) 
			{
				// ... check whether we still have it
				if(m_positionStateOfChunk == PositionType.INSIDE && m_currentChunk.ContainsRow(1)) 
				{
					m_currentChunk.setRow(1);
					m_positionState = PositionType.INSIDE;
					result = true;
				} 
				else 
					result = FetchFirst();
			} 
			else if(m_positionState == PositionType.INSIDE) 
			{
				if(m_currentChunk.Move(1)) 
					result = true;
				else 
				{
					if(m_currentChunk.IsLast) 
					{
						m_positionState = PositionType.AFTER_LAST;
						return false;
					}
					result = FetchNextChunk();
				}
			} 

			return result;
#else
			if (m_fSchemaOnly)
				return false;

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
			schema.Columns.Add(new DataColumn("BaseSchemaName", typeof(string)));
			schema.Columns.Add(new DataColumn("BaseTableName", typeof(string)));

			DataRow row;

#if SAFE
			for (int cnt = 0; cnt < FieldCount; cnt++)
			{
				DBTechTranslator info = m_fetchInfo.GetColumnInfo(cnt);
				row = schema.NewRow();

				row["ColumnName"] = info.ColumnName;
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = info.PhysicalLength;
				row["NumericPrecision"] = info.Precision;
				row["NumericScale"] = info.Scale;
				row["DataType"] = GetFieldType(cnt);
				row["IsLong"] = info.IsLongKind;
				row["AllowDBNull"] = info.IsNullable;
				row["IsReadOnly"] = !info.IsWritable;
#else
			IntPtr meta = SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset);

			for (int cnt = 0; cnt < FieldCount; cnt++)
			{
				row = schema.NewRow();
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
#endif

				if (m_updTableName != null)
				{
					string[] schemaName = m_updTableName.Split('.');
					if (schemaName.Length > 1)
					{
						row["BaseSchemaName"] = schemaName[0].Replace("\"", "");
						row["BaseTableName"] = schemaName[1].Replace("\"", "");
					}
				}

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
			// this case is the size of the column metadata array.
			get
			{
#if SAFE
				return m_fetchInfo.NumberOfColumns;
#else
				return SQLDBC.SQLDBC_ResultSetMetaData_getColumnCount(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset)); 
#endif
			}
		}

		public string GetName(int i)
		{
#if SAFE
			return m_fetchInfo.GetColumnInfo(i).ColumnName;
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
#if SAFE
			return m_fetchInfo.GetColumnInfo(i).ColumnTypeName;
#else
			return GeneralColumnInfo.GetTypeName(
				SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1)));
#endif
		}

		public Type GetFieldType(int i)
		{
#if SAFE
			return m_fetchInfo.GetColumnInfo(i).ColumnType;
#else
			return GeneralColumnInfo.GetType(
				SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1)));
#endif
		}

		public object GetValue(int i)
		{
#if SAFE
			DBTechTranslator info = FindColumnInfo(i);
			return info.IsDBNull(CurrentRecord)? DBNull.Value : info.GetValue(this, CurrentRecord);
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
#if SAFE
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
#if SAFE
			return FindColumnInfo(i).GetByte(this, CurrentRecord);
#else
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
			return (byte)GetValue(i);
#endif
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
		{
#if SAFE
			return FindColumnInfo(i).GetBytes(this, CurrentRecord, fieldOffset, buffer, bufferOffset, length);
#else
			int columnType;
			return GetValueBytes(i, out columnType, fieldOffset, buffer, bufferOffset, length);
#endif
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
#if SAFE
			return FindColumnInfo(i).GetChars(this, CurrentRecord, fieldoffset, buffer, bufferoffset, length);
#else
			int columnType;
			byte[] byte_buffer = new byte[buffer.Length * Consts.unicodeWidth]; 
			long result_length = GetValueBytes(i, out columnType,
				fieldoffset * Consts.unicodeWidth, byte_buffer, bufferoffset * Consts.unicodeWidth, length * Consts.unicodeWidth);
			for (int k = 0; k < byte_buffer.Length / Consts.unicodeWidth; k++)
				buffer[k] = BitConverter.ToChar(byte_buffer, k * Consts.unicodeWidth);
			return result_length / Consts.unicodeWidth;
#endif
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
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
#if SAFE
			return FindColumnInfo(i).IsDBNull(CurrentRecord);
#else
			return (GetValue(i) == DBNull.Value);
#endif
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

#if SAFE
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
				return (m_currentChunk != null) ? m_currentChunk.ReplyData : null;
			}
		}

		private void AssertNotClosed()
		{
			if(!m_fOpened) 
				throw new ObjectIsClosedException();
		}

		private void CloseOpenStreams()
		{
			foreach (object obj in m_openStreams)
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
			m_openStreams.Clear();
		}

		/*
			Executes a FETCH FIRST, and stores the result internally.
			@return true if the cursor is positioned correctly.
		*/
		private bool FetchFirst()
		{
			MaxDBReplyPacket reply;

			//int usedFetchSize = this.fetchSize;

			try 
			{
				reply = m_fetchInfo.ExecFetchNext();
			} 
			catch(MaxDBSQLException sqlEx) 
			{
				if(sqlEx.VendorCode == 100) 
				{
					m_empty = true;
					m_positionState = PositionType.AFTER_LAST;
					m_currentChunk = null;
				} 
				else 
					throw;
				return false;
			}
			SetCurrentChunk(new FetchChunk(
				FetchType.FIRST,		// fetch first is forward
				1,						// absolute start position
				reply,					// reply packet
				m_fetchInfo.RecordSize,	// the size for data part navigation
				m_maxRows,				// how many rows to fetch
				m_rowsInResultSet));
			return true;
		}

		// Fetch the next chunk, moving forward over the result set.
		private bool FetchNextChunk()
		{
			MaxDBReplyPacket reply;

			//int usedFetchSize = this.fetchSize;
			int usedOffset=1;

			if(m_currentChunk.IsForward) 
				if(m_modifiedKernelPos != 0) 
					usedOffset +=  m_currentChunk.End - m_modifiedKernelPos;
				else 
				{
					// if an update destroyed the cursor position, we have to honor this ...
					if(m_modifiedKernelPos == 0) 
						usedOffset +=  m_currentChunk.End - m_currentChunk.KernelPos;
					else 
						usedOffset +=  m_currentChunk.End - m_modifiedKernelPos;
				}

			try 
			{
				reply = m_fetchInfo.ExecFetchNext();
			} 
			catch(MaxDBSQLException sqlEx) 
			{
				if(sqlEx.VendorCode == 100) 
				{
					// fine, we are at the end.
					m_currentChunk.IsLast = true;
					UpdateRowStatistics();
					// but invalidate it, as it is thrown away by the kernel
					m_currentChunk = null;
					m_positionStateOfChunk = PositionType.NOT_AVAILABLE;
					m_positionState = PositionType.AFTER_LAST;
					return false;
				}
				throw;
			}
			SetCurrentChunk(new FetchChunk(
				FetchType.RELATIVE_UP,
				m_currentChunk.End + 1,
				reply,
				m_fetchInfo.RecordSize,
				m_maxRows,				
				m_rowsInResultSet));
			return true;
		}

		private ByteArray CurrentRecord
		{
			get
			{
				if(m_positionState == PositionType.BEFORE_FIRST) 
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_RESULTSET_BEFOREFIRST));
				if(m_positionState == PositionType.AFTER_LAST) 
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_RESULTSET_AFTERLAST));
				return m_currentChunk.CurrentRecord;
			}
		}

		private DBTechTranslator FindColumnInfo(int colIndex)
		{
			AssertNotClosed();
			DBTechTranslator info;

			try 
			{
				info = m_fetchInfo.GetColumnInfo(colIndex);
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
					new IntPtr(namePtr), SQLDBC_StringEncodingType.UCS2Swapped, len, ref len);
				if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
					throw new MaxDBException("Can't not allocate buffer for the column name");
			}

			len += sizeof(char);
			columnName = new byte[len];

			fixed(byte *namePtr = columnName)
			{
				rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), pos, 
					new IntPtr(namePtr), SQLDBC_StringEncodingType.UCS2Swapped, len, ref len);

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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " + 
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " + 
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(double_val);
				case DataType.INTEGER:
					int int_val;
					val_length = sizeof(int);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, new IntPtr(&int_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return null;
					else
						return BitConverter.GetBytes(int_val);
				case DataType.SMALLINT:
					short short_val;
					val_length = sizeof(short);
					if(SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, new IntPtr(&short_val), 
						ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, 
							Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (val_length == SQLDBC.SQLDBC_NULL_DATA)
							return null;

						if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					columnValue = new byte[val_length];

					fixed(byte *valuePtr = columnValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, 
							Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (rc != SQLDBC_Retcode.SQLDBC_OK)
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
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
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					binValue = new byte[val_length];

					fixed(byte *valuePtr = binValue)
					{
						rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
							new IntPtr(valuePtr), ref val_length, val_length, 0);

						if (rc != SQLDBC_Retcode.SQLDBC_OK)
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
								SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
					}

					return binValue;
				default:
					return null;
			}
		}

		private unsafe long GetValueBytes(int i, out int columnType, long dataIndex, byte[] buffer, int bufferIndex, int length)
		{
			SQLDBC_Retcode rc;
			SQLDBC_HostType hostType;
			columnType = SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(m_resultset), (short)(i + 1));
			switch(columnType)
			{
				case DataType.STRA:
				case DataType.VARCHARA:
				case DataType.CHA:
					hostType = SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII;
					break;
				case DataType.VARCHARUNI:
				case DataType.STRUNI:
				case DataType.UNICODE:
					hostType = Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2;
					break;
				case DataType.STRB:
				case DataType.VARCHARB:
				case DataType.CHB:
					hostType = SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY;
					break;
				default:
					byte[] byte_buffer = GetValueBytes(i, out columnType);
					Array.Copy(byte_buffer, dataIndex, buffer, bufferIndex, length);
					return length;
			}

			byte[] columnValue = new byte[4096];
			int val_length = 0;
			int alreadyRead = 0;

			fixed(byte *valuePtr = columnValue)
			{
				for(;;)
				{
					rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, hostType, new IntPtr(valuePtr), ref val_length, 
						(int)(alreadyRead + columnValue.Length < dataIndex ? columnValue.Length : dataIndex - alreadyRead), 0);

					if (val_length == SQLDBC.SQLDBC_NULL_DATA)
						return 0;

					if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));

					if (rc == SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
					{
						if(alreadyRead + columnValue.Length < dataIndex)
						{
							alreadyRead += columnValue.Length;
							continue;
						}
						else
							break;
					}

					if (rc == SQLDBC_Retcode.SQLDBC_OK)
						break;
				}
			}

			length = Math.Min(length, buffer.Length - bufferIndex);

			fixed(byte *valuePtr = buffer)
			{
				rc = SQLDBC.SQLDBC_ResultSet_getObject(m_resultset, i + 1, hostType, 
					new IntPtr(valuePtr + bufferIndex), ref val_length, length, 0);

				if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC && rc != SQLDBC_Retcode.SQLDBC_OK)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_GETOBJECT_FAILED) + ": " +
						SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(m_resultset)));
			}

			return length;
		}

		#endregion
#endif
	}
}

