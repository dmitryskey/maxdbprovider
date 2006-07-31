//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//	of the License, or (at your option) any later version.
//
//	This program is distributed in the hope that it will be useful,
//	but WITHOUT ANY WARRANTY; without even the implied warranty of
//	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//	GNU General Public License for more details.
//
//	You should have received a copy of the GNU General Public License
//	along with this program; if not, write to the Free Software
//	Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.IO;
using System.Globalization;
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif // NET20
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;

namespace MaxDB.Data
{
    public sealed class MaxDBDataReader : 
#if NET20
        DbDataReader
#else
        IDataReader, IDataRecord, IEnumerable, IDisposable
#endif // NET20
        
#if SAFE
        , ISqlParameterController
#endif // SAFE
    {
        // The DataReader should always be open when returned to the user.
        private bool bOpened = true;
        internal bool bCloseConn;		        //close connection after data reader closing
        internal bool bSchemaOnly;	            //return column information only
        private MaxDBConnection dbConnection;	//connection handle
        private MaxDBCommand cmdCommand;		//command handle
        private string strUpdatedTableName;		// tablename used for updateable resultsets 

#if SAFE

        private FetchInfo mFetchInfo;			        // The fetch details.
        private FetchChunk mCurrentChunk;			    // The data of the last fetch operation.
        private PositionType mPositionState;		    //the status of the position
        private PositionType mPositionStateOfChunk;     // The status of the current chunk.
        private bool bEmpty;                           // is this result set totally empty
        // a vector of all streams that went outside.
#if NET20
        private List<Stream> lstOpenStreams;
#else
        private ArrayList lstOpenStreams;                
#endif // NET20
        private int iRowsInResultSet;                  // the number of rows in this result set, or -1 if not known
        private int iLargestKnownAbsPos;               // largest known absolute position to be inside.
        private int iModifiedKernelPos;                // contains 0 if the kernel pos is not modified or the current kernel position.
        internal int iMaxRows;				           //how many rows fetch

        internal MaxDBDataReader()
        {
            bEmpty = true;
        }

		internal MaxDBDataReader(MaxDBCommand cmd)
		{
			bEmpty = true;
			dbConnection = cmd.Connection;
			cmdCommand = cmd;
		}

        internal MaxDBDataReader(MaxDBConnection connection, FetchInfo fetchInfo, MaxDBCommand cmd, int maxRows, MaxDBReplyPacket reply)
        {
            dbConnection = connection;
            mFetchInfo = fetchInfo;

            bOpened = true;

            iMaxRows = maxRows;

            cmdCommand = cmd;
            strUpdatedTableName = cmd.mParseInfo.UpdatedTableName;

            InitializeFields();
            lstOpenStreams = new 
#if NET20
                List<Stream>(5);
#else
                ArrayList(5);
#endif // NET20
            if (reply != null)
            {
                SetCurrentChunk(new FetchChunk(dbConnection,
                    FetchType.FIRST,		// fetch first is forward
                    1,						// absolute start position
                    reply,					// reply packet
                    fetchInfo.RecordSize,	// the size for data part navigation condition in that case
                    maxRows,				// how many rows to fetch
                    iRowsInResultSet
                    ));
                mPositionState = PositionType.BEFORE_FIRST;
            }
        }

        private void InitializeFields()
        {
            mCurrentChunk = null;
            mPositionState = PositionType.BEFORE_FIRST;
            mPositionStateOfChunk = PositionType.NOT_AVAILABLE;
            bEmpty = false;
            iLargestKnownAbsPos = 1;
            iRowsInResultSet = -1;
            iModifiedKernelPos = 0;
        }

        private void SetCurrentChunk(FetchChunk newChunk)
        {
            mPositionState = mPositionStateOfChunk = PositionType.INSIDE;
            mCurrentChunk = newChunk;
            iModifiedKernelPos = 0; // clear this out, until someone will de
            UpdateRowStatistics();
        }

        private void UpdateRowStatistics()
        {
            if (!RowsInResultSetKnown)
            {
                // If this is the one and only chunk, yes then we
                // have only the records in this chunk.
                if (mCurrentChunk.IsLast && mCurrentChunk.IsFirst)
                {
                    iRowsInResultSet = mCurrentChunk.Size;
                    mCurrentChunk.RowsInResultSet = iRowsInResultSet;
                }
                // otherwise, we may have navigated through it from start ...
                else if (mCurrentChunk.IsLast && mCurrentChunk.IsForward)
                {
                    iRowsInResultSet = mCurrentChunk.End;
                    mCurrentChunk.RowsInResultSet = iRowsInResultSet;
                }
                // ... or from end
                else if (mCurrentChunk.IsFirst && !mCurrentChunk.IsForward)
                {
                    iRowsInResultSet = -mCurrentChunk.Start;
                    mCurrentChunk.RowsInResultSet = iRowsInResultSet;
                }
                else if (mCurrentChunk.IsForward)
                    iLargestKnownAbsPos = Math.Max(iLargestKnownAbsPos, mCurrentChunk.End);
            }
        }

        private bool RowsInResultSetKnown
        {
            get
            {
                return iRowsInResultSet != -1;
            }
        }

        internal bool Empty
        {
            set
            {
                bEmpty = value;
            }
        }

#else
		private IntPtr mResultset = IntPtr.Zero;
		private Hashtable m_ValArrays = new Hashtable();

		internal MaxDBDataReader(MaxDBCommand cmd)
		{
			dbConnection = cmd.Connection;
			cmdCommand = cmd;
		}
		
		internal MaxDBDataReader(IntPtr resultset, MaxDBConnection conn, MaxDBCommand cmd, bool closeConn, bool schemaOnly)
		{
			mResultset = resultset;
			dbConnection = conn;
			cmdCommand = cmd;
			
			bCloseConn = closeConn;
			bSchemaOnly = schemaOnly;
			strUpdatedTableName = cmd.UpdTableName;
		}
#endif // SAFE

        /// <summary>
        /// Always return a value of zero since nesting is not supported.
        /// </summary>
#if NET20
        public override int Depth
#else
        public int Depth
#endif // NET20
        {
            get
            {
                return 0;
            }
        }

#if NET20
        public override bool IsClosed
#else
        public bool IsClosed
#endif // NET20
        {
            //Keep track of the reader state - some methods should be disallowed if the reader is closed.
            get
            {
                return !bOpened;
            }
        }

#if NET20
        public override int RecordsAffected
#else
        public int RecordsAffected
#endif // NET20
        {
            get
            {
                return cmdCommand.iRowsAffected;
            }
        }

#if NET20
        public override void Close()
#else
        public void Close()
#endif // NET20
        {
            if (bOpened)
            {
                bOpened = false;
#if SAFE
                mCurrentChunk = null;
                mFetchInfo = null;
#else
			if (mResultset != IntPtr.Zero)
			{
				UnsafeNativeMethods.SQLDBC_ResultSet_close(mResultset);
				mResultset = IntPtr.Zero;
			}
#endif // SAFE
                if (bCloseConn && dbConnection != null)
                {
                    dbConnection.Close();
                    dbConnection.mLogger.Flush();
                    dbConnection = null;
                }
            }
        }

#if NET20
        public override bool NextResult()
#else
        public bool NextResult()
#endif // NET20
        {
            return false;
        }

#if NET20
        public override bool Read()
#else
        public bool Read()
#endif // NET20
        {
#if SAFE
            AssertNotClosed();
            // if we have nothing, there is nothing to do.
            if (bEmpty || bSchemaOnly)
            {
                mPositionState = PositionType.AFTER_LAST;
                return false;
            }

            bool result = false;

            // at first we have to close all input streams
            CloseOpenStreams();

            // if we are outside, ...
            if (mPositionState == PositionType.BEFORE_FIRST)
            {
                // ... check whether we still have it
                if (mPositionStateOfChunk == PositionType.INSIDE && mCurrentChunk.ContainsRow(1))
                {
                    mCurrentChunk.setRow(1);
                    mPositionState = PositionType.INSIDE;
                    result = true;
                }
                else
                    result = FetchFirst();
            }
            else if (mPositionState == PositionType.INSIDE)
            {
                if (mCurrentChunk.Move(1))
                    result = true;
                else
                {
                    if (mCurrentChunk.IsLast)
                    {
                        mPositionState = PositionType.AFTER_LAST;
                        return false;
                    }
                    result = FetchNextChunk();
                }
            }

            return result;
#else
			m_ValArrays.Clear();
			if (bSchemaOnly)
				return false;

			SQLDBC_Retcode rc = UnsafeNativeMethods.SQLDBC_ResultSet_next(mResultset);

			switch (rc)
			{
				case SQLDBC_Retcode.SQLDBC_OK:
					return true;
				case SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND:
					return false;
				default:
					MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.FETCH_DATA_FAILED),
						UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					return false;
			}
#endif // SAFE
        }

#if NET20
        public override DataTable GetSchemaTable()
#else
        public DataTable GetSchemaTable()
#endif // NET20
        {
            DataTable schema = new DataTable("SchemaTable");
            schema.Locale = CultureInfo.InvariantCulture;
            DataTable dtMetaData = new DataTable();
            dtMetaData.Locale = CultureInfo.InvariantCulture;
            string user = null, table = null;

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
            schema.Columns.Add(new DataColumn("ProviderType", typeof(MaxDBType)));
            schema.Columns.Add(new DataColumn("IsLong", typeof(bool)));
            schema.Columns.Add(new DataColumn("AllowDBNull", typeof(bool)));
            schema.Columns.Add(new DataColumn("IsReadOnly", typeof(bool)));
            schema.Columns.Add(new DataColumn("IsUnique", typeof(bool)));
            schema.Columns.Add(new DataColumn("IsKeyColumn", typeof(bool)));
            schema.Columns.Add(new DataColumn("IsAutoIncrement", typeof(bool)));
            schema.Columns.Add(new DataColumn("BaseSchemaName", typeof(string)));
            schema.Columns.Add(new DataColumn("BaseTableName", typeof(string)));

            DataRow row;
            if (strUpdatedTableName != null)
            {
                string[] schemaName = strUpdatedTableName.Split('.');
                if (schemaName.Length > 1)
                {
                    user = schemaName[0].Replace("\"", string.Empty);
                    table = schemaName[1].Replace("\"", string.Empty);

                    MaxDBCommand cmdColumns = new MaxDBCommand(
                                    "SELECT A.COLUMNNAME, A.MODE, A.DEFAULT, B.TYPE FROM DOMAIN.COLUMNS A " +
                                    "LEFT OUTER JOIN DOMAIN.INDEXCOLUMNS B " +
                                    "ON A.OWNER = B.OWNER AND A.TABLENAME = B.TABLENAME AND A.COLUMNNAME = B.COLUMNNAME " +
                                    "WHERE A.OWNER = ? AND A.TABLENAME = ?", dbConnection);
                    cmdColumns.Parameters.Add("OWNER", MaxDBType.VarCharA).Value = user;
                    cmdColumns.Parameters.Add("TABLENAME", MaxDBType.VarCharA).Value = table;
                    MaxDBDataAdapter da = new MaxDBDataAdapter();
                    da.SelectCommand = cmdColumns;
                    da.Fill(dtMetaData);
                }
            }

#if SAFE
            for (int cnt = 0; cnt < FieldCount; cnt++)
            {
                DBTechTranslator info = mFetchInfo.GetColumnInfo(cnt);
                row = schema.NewRow();

                row["ColumnName"] = info.ColumnName;
                row["ColumnOrdinal"] = cnt + 1;
                row["ColumnSize"] = info.PhysicalLength;
                row["NumericPrecision"] = info.Precision;
                row["NumericScale"] = info.Scale;
                row["DataType"] = info.ColumnDataType;
                row["ProviderType"] = info.ColumnProviderType;
                row["IsLong"] = info.IsLongKind;
                row["AllowDBNull"] = info.IsNullable;
                row["IsReadOnly"] = true;
#else
			IntPtr meta = UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset);

			for (int cnt = 0; cnt < FieldCount; cnt++)
			{
				row = schema.NewRow();
				row["ColumnName"] = GetName(cnt);
				row["ColumnOrdinal"] = cnt + 1;
				row["ColumnSize"] = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getPhysicalLength(meta, (short)(cnt + 1));
				row["NumericPrecision"] = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getPrecision(meta, (short)(cnt + 1));
				row["NumericScale"] = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getScale(meta, (short)(cnt + 1));
				row["DataType"] = GetFieldType(cnt);
				row["ProviderType"] =  GeneralColumnInfo.GetMaxDBType(
					UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(cnt + 1)));
				row["IsLong"] = GeneralColumnInfo.IsLong(
					UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(cnt + 1)));
				row["AllowDBNull"] = (UnsafeNativeMethods.SQLDBC_ResultSetMetaData_isNullable(meta, (short)(cnt + 1)) == ColumnNullBehavior.columnNullable);
				row["IsReadOnly"] = (UnsafeNativeMethods.SQLDBC_ResultSetMetaData_isWritable(meta, (short)(cnt + 1)) == 0);
#endif // SAFE

                if (user != null && table != null)
                {
                    row["BaseSchemaName"] = user;
                    row["BaseTableName"] = table;

					foreach (DataRow columnRow in dtMetaData.Rows)
						if (!columnRow.IsNull(0) && string.Compare(columnRow[0].ToString().Trim(), row["ColumnName"].ToString().Trim(), true, CultureInfo.InvariantCulture) == 0)
						{
							row["IsKeyColumn"] = (!columnRow.IsNull(1) && columnRow[1].ToString() == "KEY");
							row["IsAutoIncrement"] = (!columnRow.IsNull(2) && columnRow[2].ToString().StartsWith("DEFAULT SERIAL"));
							row["IsUnique"] = (!columnRow.IsNull(3) && columnRow[3].ToString() == "UNIQUE");
						}
                }

                schema.Rows.Add(row);
            }

            return schema;
        }

#if NET20
        public override int FieldCount
#else
        public int FieldCount
#endif // NET20
        {
            // Return the count of the number of columns, which in this case is the size of the column metadata array.
            get
            {
#if SAFE
                return mFetchInfo.NumberOfColumns;
#else
				return UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnCount(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset)); 
#endif // SAFE
            }
        }

#if NET20
        public override int VisibleFieldCount
#else
        public int VisibleFieldCount
#endif // NET20
        {
            // Return the count of the number of columns, which in this case is the size of the column metadata array.
            get
            {
                return FieldCount;
            }
        }


#if NET20
        public override string GetName(int i)
#else
        public string GetName(int i)
#endif // NET20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            return mFetchInfo.GetColumnInfo(i).ColumnName;
#else
			return Encoding.Unicode.GetString(GetNameBytes((short)(i + 1))).TrimEnd('\0');
#endif
        }

#if NET20
        public override string GetDataTypeName(int i)
#else
        public string GetDataTypeName(int i)
#endif // NET20
        {
            /*
             * Usually this would return the name of the type
             * as used on the back end, for example 'smallint' or 'varchar'.
             * The sample returns the simple name of the .NET Framework type.
             */
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            return mFetchInfo.GetColumnInfo(i).ColumnTypeName;
#else
			return GeneralColumnInfo.GetTypeName(
				UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1)));
#endif // SAFE
        }

#if NET20
        public override Type GetFieldType(int i)
#else
        public Type GetFieldType(int i)
#endif // NET 20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            return mFetchInfo.GetColumnInfo(i).ColumnDataType;
#else
			return GeneralColumnInfo.GetType(
				UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1)));
#endif
        }

#if NET20
        public override object GetValue(int i)
#else
        public object GetValue(int i)
#endif // NET 20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            object obj_value = transl.IsDBNull(CurrentRecord) ? DBNull.Value : transl.GetValue(this, CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                if (obj_value != DBNull.Value)
                {
                    string str_value = obj_value.ToString();
                    LogValue(i + 1, transl, "OBJECT", 0, 1,
                        (str_value.Length <= MaxDBLogger.DataSize ? str_value : str_value.Substring(0, MaxDBLogger.DataSize) + "..."));
                }
                else
                    LogValue(i + 1, transl, "OBJECT", 0, 1, "NULL");
            //<<< SQL TRACE

            return obj_value;
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
						return ODBCConverter.GetDateTime(ODBCConverter.GetDate(data));
					case DataType.TIME:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTime(data));
					case DataType.TIMESTAMP:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTimeStamp(data));
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					case DataType.STRUNI:
					case DataType.VARCHARUNI:
					case DataType.UNICODE:
						if (Consts.IsLittleEndian)
							return Encoding.Unicode.GetString(data);
						else
							return Encoding.BigEndianUnicode.GetString(data);
					case DataType.STRA:
					case DataType.VARCHARA:
					case DataType.CHA:
						return Encoding.ASCII.GetString(data);
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
#endif // SAFE
        }

#if NET20
        public override int GetValues(object[] values)
#else
        public int GetValues(object[] values)
#endif // NET20
        {
            if (values == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));

            for (int i = 0; i < Math.Min(FieldCount, values.Length); i++)
                values[i] = GetValue(i);
            return Math.Min(FieldCount, values.Length);
        }

#if NET20
        public override int GetOrdinal(string name)
#else
        public int GetOrdinal(string name)
#endif // NET20
        {
            if (name == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAME_NOTFOUND, name));

            // Throw an exception if the ordinal cannot be found.
            for (short cnt = 0; cnt <= FieldCount - 1; cnt++)
                if (string.Compare(GetName(cnt).Trim(), name.Trim(), true, CultureInfo.InvariantCulture) == 0)
                    return cnt;
            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAME_NOTFOUND, name));
        }

#if NET20
        public override object this[int i]
#else
        public object this[int i]
#endif // NET20
        {
            get
            {
                return GetValue(i);
            }
        }

#if NET20
        public override object this[string name]
#else
        public object this[string name]
#endif // NET20
        {
            // Look up the ordinal and return the value at that position.
            get
            {
                return GetValue(GetOrdinal(name));
            }
        }

#if SAFE
        private void LogValue(int i, DBTechTranslator transl, string type, int size, int minusLen, string value)
        {
            DateTime dt = DateTime.Now;
            dbConnection.mLogger.SqlTrace(dt, "GET " + type + " VALUE:");
            dbConnection.mLogger.SqlTraceDataHeader(dt);
            StringBuilder sb = new StringBuilder();
            sb.Append(i.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.NumSize));
            sb.Append(transl.ColumnTypeName.PadRight(MaxDBLogger.TypeSize));
            sb.Append((transl.PhysicalLength - minusLen).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
            sb.Append(size.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
            sb.Append(value);
            dbConnection.mLogger.SqlTrace(dt, sb.ToString());
        }
#endif  // SAFE

#if NET20
        public override bool GetBoolean(int i)
#else
        public bool GetBoolean(int i)
#endif // NET20
        {
            /*
             * Force the cast to return the type. InvalidCastException
             * should be thrown if the data is not already of the correct type.
             */
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);

#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            bool bool_value = transl.GetBoolean(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "BOOLEAN", 1, 0, bool_value.ToString());
            //<<< SQL TRACE

            return bool_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
					case DataType.STRB:	
					case DataType.VARCHARB:
					case DataType.CHB:
						return (data[0] == 1);
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (BitConverter.ToDouble(data, 0) == 1);
					case DataType.INTEGER:
						return (BitConverter.ToInt32(data, 0) == 1);
					case DataType.SMALLINT:
						return (BitConverter.ToInt16(data, 0) == 1);
					case DataType.STRUNI:
					case DataType.VARCHARUNI:
					case DataType.UNICODE:
						if (Consts.IsLittleEndian)
							return bool.Parse(Encoding.Unicode.GetString(data));
						else
							return bool.Parse(Encoding.BigEndianUnicode.GetString(data));
					case DataType.STRA:
					case DataType.VARCHARA:
					case DataType.CHA:
						return bool.Parse(Encoding.ASCII.GetString(data));
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Boolean"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override byte GetByte(int i)
#else
        public byte GetByte(int i)
#endif // NET20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            byte byte_value = transl.GetByte(this, CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "BYTE", 1, 0, byte_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return byte_value;
#else
			byte[] buffer = new byte[1];
			int columnType = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1));
			GetValueBytes(i, columnType, 0, buffer, 0, 1);
			return buffer[0];
#endif // SAFE
        }

#if NET20
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
#else
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
#endif // NET20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            long result = transl.GetBytes(this, CurrentRecord, fieldOffset, buffer, bufferoffset, length);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                byte[] logs = new byte[Math.Min(MaxDBLogger.DataSize / 2, length)];
                Array.Copy(buffer, bufferoffset, logs, 0, logs.Length);

                LogValue(i + 1, transl, "BYTES", logs.Length, 0, Consts.ToHexString(logs) + (logs.Length < length ? "..." : ""));
            }
            //<<< SQL TRACE

            return result;
#else
			int columnType = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1));
			return GetValueBytes(i, columnType, fieldOffset, buffer, bufferoffset, length);
#endif
        }

#if NET20
        public override char GetChar(int i)
#else
        public char GetChar(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
			return GetString(i)[0];
        }

#if NET20
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
#else
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
#endif // NET20
        {
            if (buffer == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            long result = transl.GetChars(this, CurrentRecord, fieldoffset, buffer, bufferoffset, length);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                char[] logs = new char[Math.Min(MaxDBLogger.DataSize, length)];
                Array.Copy(buffer, bufferoffset, logs, 0, logs.Length);

                LogValue(i + 1, transl, "CHARS", logs.Length, 0, new string(logs) + (logs.Length < length ? "..." : ""));
            }
            //<<< SQL TRACE

            return result;
#else
			int columnType = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1));
			int elemSize;
			switch(columnType)
			{
				case DataType.LONGUNI:
				case DataType.STRUNI:
				case DataType.VARCHARUNI:
				case DataType.UNICODE:
					elemSize = Consts.UnicodeWidth;
					break;
				default:
					elemSize = 1;
					break;
			}

			byte[] byte_buffer = new byte[buffer.Length * elemSize]; 
			long result_length = GetValueBytes(i, columnType, fieldoffset * elemSize, byte_buffer, bufferoffset * elemSize, length * elemSize);
			if (elemSize == Consts.UnicodeWidth)
				Encoding.Unicode.GetChars(byte_buffer, 0, byte_buffer.Length, buffer, 0);
			else
				Encoding.ASCII.GetChars(byte_buffer, 0, byte_buffer.Length, buffer, 0);
					
			return result_length / elemSize;
#endif // SAFE
        }

#if NET20
        public override Guid GetGuid(int i)
#else
        public Guid GetGuid(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
            return new Guid(GetString(i));
        }

#if NET20
        public override short GetInt16(int i)
#else
        public short GetInt16(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            short short_value = transl.GetInt16(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "INT16", 2, 0, short_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return short_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (short)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return (short)BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Int16"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override int GetInt32(int i)
#else
        public int GetInt32(int i)
#endif
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            int int_value = transl.GetInt32(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "INT32", 4, 0, int_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return int_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (int)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Int32"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override long GetInt64(int i)
#else
        public long GetInt64(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            long long_value = transl.GetInt64(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "INT64", 8, 0, long_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return long_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (long)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Int64"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override float GetFloat(int i)
#else
        public float GetFloat(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            float float_value = transl.GetFloat(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "FLOAT", 4, 0, float_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return float_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (float)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Float"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override double GetDouble(int i)
#else
        public double GetDouble(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            double double_value = transl.GetDouble(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "DOUBLE", 8, 0, double_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return double_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Double"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override string GetString(int i)
#else
        public string GetString(int i)
#endif // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            string str_value = transl.GetString(this, CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                if (str_value != null)
                    LogValue(i + 1, transl, "STRING", str_value.Length, 1,
                    (str_value.Length <= MaxDBLogger.DataSize ? str_value : str_value.Substring(0, MaxDBLogger.DataSize) + "..."));
                else
                    LogValue(i + 1, transl, "STRING", 0, 1, "NULL");
            //<<< SQL TRACE

            return str_value;
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
						return ODBCConverter.GetDateTime(ODBCConverter.GetDate(data)).ToShortDateString();
					case DataType.TIME:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTime(data)).ToShortTimeString();
					case DataType.TIMESTAMP:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTimeStamp(data)).ToString();
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
                        return BitConverter.ToDouble(data, 0).ToString(CultureInfo.InvariantCulture);
					case DataType.INTEGER:
                        return BitConverter.ToInt32(data, 0).ToString(CultureInfo.InvariantCulture);
					case DataType.SMALLINT:
                        return BitConverter.ToInt16(data, 0).ToString(CultureInfo.InvariantCulture);
					case DataType.STRUNI:
					case DataType.VARCHARUNI:
					case DataType.UNICODE:
						if (Consts.IsLittleEndian)
							return Encoding.Unicode.GetString(data);
						else
							return Encoding.BigEndianUnicode.GetString(data);
					case DataType.STRA:
					case DataType.STRB:
					case DataType.VARCHARA:
					case DataType.VARCHARB:
					case DataType.CHA:
					case DataType.CHB:
						return Encoding.ASCII.GetString(data);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.UNKNOWN_DATATYPE));
				}
			}
			else
				return null;
#endif // SAFE
        }

#if NET20
        public override decimal GetDecimal(int i)
#else
        public decimal GetDecimal(int i)
#endif
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            decimal dec_value = transl.GetDecimal(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "DECIMAL", 8, 0, dec_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return dec_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.BOOLEAN:
						return data[0];
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return (decimal)BitConverter.ToDouble(data, 0);
					case DataType.INTEGER:
						return BitConverter.ToInt32(data, 0);
					case DataType.SMALLINT:
						return BitConverter.ToInt16(data, 0);
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "Decimal"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if NET20
        public override DateTime GetDateTime(int i)
#else
        public DateTime GetDateTime(int i)
#endif  // NET20
        {
            // Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            DBTechTranslator transl = FindColumnInfo(i);
            DateTime dt_value = transl.GetDateTime(CurrentRecord);

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
                LogValue(i + 1, transl, "DATETIME", 0, 0, dt_value.ToString(CultureInfo.InvariantCulture));
            //<<< SQL TRACE

            return dt_value;
#else
			int columnType;
			byte[] data = GetValueBytes(i, out columnType);
			if (data != null)
			{
				switch(columnType)
				{
					case DataType.DATE:
						return ODBCConverter.GetDateTime(ODBCConverter.GetDate(data));
					case DataType.TIME:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTime(data));
					case DataType.TIMESTAMP:
						return ODBCConverter.GetDateTime(ODBCConverter.GetTimeStamp(data));
					default:
						throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, 
							DataType.StrValues[columnType], "DateTime"));
				}
			}
			else
				throw new InvalidCastException(MaxDBMessages.Extract(MaxDBError.COLUMNVALUE_NULL));
#endif // SAFE
        }

#if !NET20
        public IDataReader GetData(int i)
        {
            /*
             * The sample code does not support this method. Normally,
             * this would be used to expose nested tables and
             * other hierarchical data.
             */
            throw new NotSupportedException();
        }
#endif // !NET20

#if NET20
        public override bool IsDBNull(int i)
#else
        public bool IsDBNull(int i)
#endif // NET20
        {
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
#if SAFE
            return FindColumnInfo(i).IsDBNull(CurrentRecord);
#else
			return (GetValue(i) == DBNull.Value);
#endif // SAFE
        }

#if NET20
        public override bool HasRows
#else
        public bool HasRows
#endif // NET20
        {
            get
            {
#if SAFE
                return iRowsInResultSet > 0;
#else
				if (mResultset != IntPtr.Zero)
					return UnsafeNativeMethods.SQLDBC_ResultSet_getResultCount(mResultset) > 0;
				else
					return false;
#endif // SAFE
            }
        }
#if SAFE
        #region "Methods to support native protocol"

        MaxDBConnection ISqlParameterController.Connection
        {
            get
            {
                return dbConnection;
            }
        }

        ByteArray ISqlParameterController.ReplyData
        {
            get
            {
                return (mCurrentChunk != null) ? mCurrentChunk.ReplyData : null;
            }
        }

        private void AssertNotClosed()
        {
            if (!bOpened)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
        }

        private void CloseOpenStreams()
        {
            foreach(Stream stream in lstOpenStreams)
            {
                try
                {
                    stream.Close();
                }
                catch (IOException)
                {
                    // ignore
                }
            }
            lstOpenStreams.Clear();
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
                reply = mFetchInfo.ExecFetchNext();
            }
            catch(MaxDBException ex)
            {
                if (ex.ErrorCode == 100)
                {
                    bEmpty = true;
                    mPositionState = PositionType.AFTER_LAST;
                    mCurrentChunk = null;
                }
                else
                    throw;
                return false;
            }
            SetCurrentChunk(new FetchChunk(dbConnection,
                FetchType.FIRST,		// fetch first is forward
                1,						// absolute start position
                reply,					// reply packet
                mFetchInfo.RecordSize,	// the size for data part navigation
                iMaxRows,				// how many rows to fetch
                iRowsInResultSet));
            return true;
        }

        // Fetch the next chunk, moving forward over the result set.
        private bool FetchNextChunk()
        {
            MaxDBReplyPacket reply;

            //int usedFetchSize = this.fetchSize;
            int usedOffset = 1;

            if (mCurrentChunk.IsForward)
                if (iModifiedKernelPos != 0)
                    usedOffset += mCurrentChunk.End - iModifiedKernelPos;
                else
                {
                    // if an update destroyed the cursor position, we have to honor this ...
                    if (iModifiedKernelPos == 0)
                        usedOffset += mCurrentChunk.End - mCurrentChunk.KernelPos;
                    else
                        usedOffset += mCurrentChunk.End - iModifiedKernelPos;
                }

            try
            {
                reply = mFetchInfo.ExecFetchNext();
            }
            catch (MaxDBException ex)
            {
                if (ex.ErrorCode == 100)
                {
                    // fine, we are at the end.
                    mCurrentChunk.IsLast = true;
                    UpdateRowStatistics();
                    // but invalidate it, as it is thrown away by the kernel
                    mCurrentChunk = null;
                    mPositionStateOfChunk = PositionType.NOT_AVAILABLE;
                    mPositionState = PositionType.AFTER_LAST;
                    return false;
                }
                throw;
            }
            SetCurrentChunk(new FetchChunk(dbConnection,
                FetchType.RELATIVE_UP,
                mCurrentChunk.End + 1,
                reply,
                mFetchInfo.RecordSize,
                iMaxRows,
                iRowsInResultSet));
            return true;
        }

        private ByteArray CurrentRecord
        {
            get
            {
                if (mPositionState == PositionType.BEFORE_FIRST)
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.RESULTSET_BEFOREFIRST));
                if (mPositionState == PositionType.AFTER_LAST)
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.RESULTSET_AFTERLAST));
                return mCurrentChunk.CurrentRecord;
            }
        }

        private DBTechTranslator FindColumnInfo(int colIndex)
        {
            AssertNotClosed();
            DBTechTranslator info;

            try
            {
                info = mFetchInfo.GetColumnInfo(colIndex);
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
				rc = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnName(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), pos, 
					namePtr, SQLDBC_StringEncodingType.UCS2Swapped, len, &len);
			if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLUMNNAME_BUFFER));

			len += sizeof(char);
			columnName = new byte[len];

			fixed(byte *namePtr = columnName)
				rc = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnName(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), pos, 
					namePtr, SQLDBC_StringEncodingType.UCS2Swapped, len, &len);

				if (rc != SQLDBC_Retcode.SQLDBC_OK)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALID_COLUMNNAME));


			return columnName;
		}

		private unsafe byte[] GetValueBytes(int i, out int columnType)
		{
			int valLength;
			SQLDBC_Retcode rc;
			columnType = UnsafeNativeMethods.SQLDBC_ResultSetMetaData_getColumnType(UnsafeNativeMethods.SQLDBC_ResultSet_getResultSetMetaData(mResultset), (short)(i + 1));

			if (m_ValArrays[i] != null)
				return (byte[])m_ValArrays[i];

			switch(columnType)
			{
				case DataType.BOOLEAN:
					byte byte_val;
					valLength = sizeof(byte);

                    if (UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT1, &byte_val,
                         &valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK)
                        MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
                            UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
					{
						byte[] result = new byte[]{byte_val};
						m_ValArrays[i] = result;
					}
					return (byte[])m_ValArrays[i];
				case DataType.DATE:
					byte[] dt_val = new byte[sizeof(OdbcDate)];
					valLength = dt_val.Length;
					fixed(byte *dt_ptr = dt_val)
						if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, dt_ptr, 
						    &valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						    MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED), 
							    UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = dt_val;
					return (byte[])m_ValArrays[i];
				case DataType.TIME:
					byte[] tm_val = new byte[sizeof(OdbcTime)];
					valLength = tm_val.Length;
					fixed(byte* tm_ptr = tm_val)
						if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, tm_ptr, 
							&valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
							MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
								UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = tm_val;
					return (byte[])m_ValArrays[i];
				case DataType.TIMESTAMP:
					byte[] ts_val = new byte[sizeof(OdbcTimeStamp)];
					valLength = ts_val.Length;
					fixed(byte *ts_ptr = ts_val)
						if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, ts_ptr, 
							&valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
							MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
								UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = ts_val;
					return (byte[])m_ValArrays[i];
				case DataType.FIXED:
				case DataType.FLOAT:
				case DataType.VFLOAT:
					double double_val;
					valLength = sizeof(double);
					if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, &double_val, 
						&valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
							UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = BitConverter.GetBytes(double_val);
					return (byte[])m_ValArrays[i];
				case DataType.INTEGER:
					int int_val;
					valLength = sizeof(int);
					if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, &int_val, 
						&valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
							UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = BitConverter.GetBytes(int_val);
					return (byte[])m_ValArrays[i];
				case DataType.SMALLINT:
					short short_val;
					valLength = sizeof(short);
					if(UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, &short_val, 
						&valLength, valLength, 0) != SQLDBC_Retcode.SQLDBC_OK) 
						MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
							UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						m_ValArrays[i] = null;
					else
						m_ValArrays[i] = BitConverter.GetBytes(short_val);
					return (byte[])m_ValArrays[i];
				case DataType.STRUNI:
				case DataType.VARCHARUNI:
				case DataType.UNICODE:
					byte[] strValue = new byte[sizeof(char)];
					valLength = 0;

					fixed(byte *valuePtr = strValue)
						rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, 
							Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
							valuePtr, &valLength, valLength, 0);

						if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
						{
							m_ValArrays[i] = null;
							return null;
						}

						if (rc == SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
							throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NODATA_FOUND));

						if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
							MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
								UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					strValue = new byte[valLength];

					if (valLength > 0)
						fixed(byte *valuePtr = strValue)
							rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, 
								Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
								valuePtr, &valLength, valLength, SQLDBC_BOOL.SQLDBC_FALSE);

							if (rc != SQLDBC_Retcode.SQLDBC_OK)
								MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
									UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					m_ValArrays[i] = strValue;
					return strValue;
				case DataType.STRA:
				case DataType.VARCHARA:
				case DataType.CHA:
					byte[] asciiValue = new byte[sizeof(byte)];
					valLength = 0;

					fixed(byte *valuePtr = asciiValue)
						rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, 
							SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, valuePtr, &valLength, valLength, 0);

					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
					{
						m_ValArrays[i] = null;
						return null;
					}

					if (rc == SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NODATA_FOUND));

				    if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
						MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
							UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					asciiValue = new byte[valLength];

					if (valLength > 0)
						fixed(byte *valuePtr = asciiValue)
						{
							rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, 
								SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, valuePtr, &valLength, valLength, SQLDBC_BOOL.SQLDBC_FALSE);

							if (rc != SQLDBC_Retcode.SQLDBC_OK)
								MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
									UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));
						}

					m_ValArrays[i] = asciiValue;
					return asciiValue;
				case DataType.STRB:
				case DataType.VARCHARB:
				case DataType.CHB:
					byte[] binValue = new byte[1];
					valLength = 0;

					fixed(byte *valuePtr = binValue)
						rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
							valuePtr, &valLength, valLength, 0);

					if (valLength == UnsafeNativeMethods.SQLDBC_NULL_DATA)
					{
						m_ValArrays[i] = null;
						return null;
					}

					if (rc == SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NODATA_FOUND));

					if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
						MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
							UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					binValue = new byte[valLength];

					if (valLength > 0)
						fixed(byte *valuePtr = binValue)
							rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
								valuePtr, &valLength, valLength, 0);

						if (rc != SQLDBC_Retcode.SQLDBC_OK)
							MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
								UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					m_ValArrays[i] = binValue;
					return binValue;
				default:
					return null;
			}
		}

		private unsafe long GetValueBytes(int i, int columnType, long dataIndex, byte[] buffer, int bufferIndex, int length)
		{
			SQLDBC_Retcode rc;
			SQLDBC_HostType hostType;
			switch(columnType)
			{
				case DataType.STRA:
				case DataType.LONGA:
					hostType = SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII;
					break;
				case DataType.STRUNI:
				case DataType.LONGUNI:
					hostType = Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2;
					break;
				case DataType.STRB:
				case DataType.LONGB:
					hostType = SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY;
					break;
				default:
					byte[] byte_buffer = GetValueBytes(i, out columnType);
					Array.Copy(byte_buffer, dataIndex, buffer, bufferIndex, Math.Min(length, byte_buffer.Length - dataIndex));
					return length;
			}

			length = buffer.Length - bufferIndex;

			bool addNullTerminator = !(columnType == DataType.STRB || columnType == DataType.LONGB);

			byte[] readBuffer = new byte[length + (addNullTerminator ? 1 : 0)];

			fixed(byte *valuePtr = readBuffer)
			{
				int ref_length = 0;
				rc = UnsafeNativeMethods.SQLDBC_ResultSet_getObject(mResultset, i + 1, hostType, 
					valuePtr, &ref_length, length + (addNullTerminator ? 1 : 0), 
					addNullTerminator ? SQLDBC_BOOL.SQLDBC_TRUE : SQLDBC_BOOL.SQLDBC_FALSE);

				if (ref_length == UnsafeNativeMethods.SQLDBC_NULL_DATA)
					return 0;

				if (rc == SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NODATA_FOUND));

				if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC && rc != SQLDBC_Retcode.SQLDBC_OK)
					MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.GETOBJECT_FAILED),
						UnsafeNativeMethods.SQLDBC_ResultSet_getError(mResultset));

					switch(columnType)
					{
						case DataType.STRA:
						case DataType.LONGA:
						case DataType.STRUNI:
						case DataType.LONGUNI:
							length = Array.IndexOf(readBuffer, (byte)0); // we ignore ref_length parameter and looking for zero terminator
							break; //since ref_length contains wrong value. Thus CLOB can't contain nulls
						case DataType.STRB:
						case DataType.LONGB:
							if (ref_length < length)
								length = ref_length;
							break;
					}
					
					Array.Copy(readBuffer, 0, buffer, bufferIndex, length);
				}

			return length;
		}

        #endregion
#endif // SAFE

#if NET20
        public override IEnumerator GetEnumerator()
#else
		public IEnumerator GetEnumerator()
#endif // NET20
        {
            return new DbEnumerator(this);
        }

        #region IDisposable Members

#if !NET20
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
		}
#endif // NET20

#if NET20 
        protected override void Dispose(bool disposing)
#else
        private void Dispose(bool disposing)
#endif // NET20 
        {
#if NET20 
            base.Dispose(disposing);
#endif // NET20
            if (disposing)
                Close();
        }

        #endregion
    }
}

