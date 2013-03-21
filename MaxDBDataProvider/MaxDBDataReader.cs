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
using System.Collections.Generic;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Security.Permissions;
using System.Runtime.InteropServices;

namespace MaxDB.Data
{
	/// <summary>
	/// Provides a means of reading a forward-only stream of rows from a MaxDB database. This class cannot be inherited.
	/// </summary>
	/// <remarks>
	/// <para>
	/// To create a <B>MaxDBDataReader</B>, you must call the <see cref="MaxDBCommand.ExecuteReader()"/>
	/// method of the <see cref="MaxDBCommand"/> object, rather than directly using a constructor.
	/// </para>
	/// <para>
	/// While the <B>MaxDBDataReader</B> is in use, the associated <see cref="MaxDBConnection"/>
	/// is busy serving the <B>MaxDBDataReader</B>, and no other operations can be performed 
	/// on the <B>MaxDBConnection</B> other than closing it. This is the case until the 
	/// <see cref="MaxDBDataReader.Close"/> method of the <B>MaxDBDataReader</B> is called.
	/// </para>
	/// </remarks>
	public sealed class MaxDBDataReader : DbDataReader, ISqlParameterController
	{
		// The DataReader should always be open when returned to the user.
		private bool bOpened = true;
		internal bool bCloseConn;		        //close connection after data reader closing
		internal bool bSchemaOnly;	            //return column information only
		private MaxDBConnection dbConnection;	//connection handle
		private MaxDBCommand cmdCommand;		//command handle
		private string strUpdatedTableName;		// tablename used for updateable resultsets 

		private FetchInfo mFetchInfo;			        // The fetch details.
		private FetchChunk mCurrentChunk;			    // The data of the last fetch operation.
		private PositionType mPositionState;		    //the status of the position
		private PositionType mPositionStateOfChunk;     // The status of the current chunk.
		private bool bEmpty;                           // is this result set totally empty
		// a vector of all streams that went outside.
		private List<Stream> lstOpenStreams;
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
			lstOpenStreams = new List<Stream>(5);
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

		/// <summary>
		/// Always return a value of zero since nesting is not supported.
		/// </summary>
		public override int Depth
		{
			get
			{
				return 0;
			}
		}

		/// <summary>
		/// Gets a value indicating whether the data reader is closed.
		/// </summary>
		public override bool IsClosed
		{
			//Keep track of the reader state - some methods should be disallowed if the reader is closed.
			get
			{
				return !bOpened;
			}
		}

		/// <summary>
		/// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
		/// </summary>
		public override int RecordsAffected
		{
			get
			{
				return cmdCommand.iRowsAffected;
			}
		}

		/// <summary>
		/// Closes the MaxDBDataReader object.
		/// </summary>
		public override void Close()
		{
			if (bOpened)
			{
				bOpened = false;
				mCurrentChunk = null;
				mFetchInfo = null;
				if (bCloseConn && dbConnection != null)
				{
					dbConnection.Close();
					dbConnection.mLogger.Flush();
					dbConnection = null;
				}
			}
		}

		/// <summary>
		/// Advances the data reader to the next result, when reading the results of batch SQL statements.
		/// </summary>
		/// <returns>Always <b>false</b>.</returns>
		public override bool NextResult()
		{
			return false;
		}

		/// <summary>
		/// Advances the MaxDBDataReader to the next record.
		/// </summary>
		/// <returns><b>true</b> if there are more rows; otherwise, <b>false</b>.</returns>
		public override bool Read()
		{
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
		}

		/// <summary>
		/// Returns a DataTable that describes the column metadata of the MaxDBDataReader.
		/// </summary>
		/// <returns>The DataTable object with column metadata.</returns>
		public override DataTable GetSchemaTable()
		{
			DataTable schema = new DataTable("SchemaTable");
			schema.Locale = CultureInfo.InvariantCulture;
			DataTable dtMetaData = new DataTable();
			dtMetaData.Locale = CultureInfo.InvariantCulture;
			dtMetaData.Columns.Add(new DataColumn("COLUMNNAME", typeof(string)));
			dtMetaData.Columns.Add(new DataColumn("MODE", typeof(string)));
			dtMetaData.Columns.Add(new DataColumn("DEFAULT", typeof(string)));
			dtMetaData.Columns.Add(new DataColumn("TYPE", typeof(string)));
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

					SqlMode oldMode = dbConnection.SqlMode;
					dbConnection.SqlMode = SqlMode.Internal;

					try
					{
						using (MaxDBCommand cmdColumns = new MaxDBCommand(
								   "SELECT A.COLUMNNAME, A.MODE, A.DEFAULT, B.TYPE FROM DOMAIN.COLUMNS A " +
								   "LEFT OUTER JOIN DOMAIN.INDEXCOLUMNS B " +
								   "ON A.OWNER = B.OWNER AND A.TABLENAME = B.TABLENAME AND A.COLUMNNAME = B.COLUMNNAME " +
								   "WHERE A.OWNER = ? AND A.TABLENAME = ?", dbConnection))
						{
							cmdColumns.Parameters.Add("OWNER", MaxDBType.VarCharA).Value = user;
							cmdColumns.Parameters.Add("TABLENAME", MaxDBType.VarCharA).Value = table;
							using (MaxDBDataReader reader = cmdColumns.ExecuteReader())
								while (reader.Read())
									dtMetaData.Rows.Add(new object[]{
										reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)});
						}
					}
					finally
					{
						dbConnection.SqlMode = oldMode;
					}
				}
			}

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

		/// <summary>
		/// Gets the count of the number of columns, which in this case is the size of the column metadata array.
		/// </summary>
		public override int FieldCount
		{
			get
			{
				return mFetchInfo.NumberOfColumns;
			}
		}

		/// <summary>
		/// Return the count of the number of columns, which in this case is the size of the column metadata array.
		/// </summary>
		public override int VisibleFieldCount
		{
			get
			{
				return FieldCount;
			}
		}

		/// <summary>
		/// Gets the name of the specified column.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>Name of the column.</returns>
		public override string GetName(int i)
		{
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			return mFetchInfo.GetColumnInfo(i).ColumnName;
		}

		/// <summary>
		/// Gets the name of the source data type.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>Data type name of the column.</returns>
		public override string GetDataTypeName(int i)
		{
			/*
			 * Usually this would return the name of the type
			 * as used on the back end, for example 'smallint' or 'varchar'.
			 * The sample returns the simple name of the .NET Framework type.
			 */
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			return mFetchInfo.GetColumnInfo(i).ColumnTypeName;
		}

		/// <summary>
		/// Gets the Type that is the data type of the column.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>Type of the column.</returns>
		public override Type GetFieldType(int i)
		{
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			return mFetchInfo.GetColumnInfo(i).ColumnDataType;
		}

		/// <summary>
		/// Gets the value of the specified column in its native format.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>Object that represents the value of the column.</returns>
		public override object GetValue(int i)
		{
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			object obj_value = transl.IsDBNull(CurrentRecord) ? DBNull.Value : transl.GetValue(this, CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                if (obj_value != DBNull.Value)
                {
                    string str_value = obj_value.ToString();
                    LogValue(i + 1, transl, "OBJECT", 0, 1,
                        (str_value.Length <= MaxDBLogger.DataSize ? str_value : str_value.Substring(0, MaxDBLogger.DataSize) + "..."));
                }
                else
                {
                    LogValue(i + 1, transl, "OBJECT", 0, 1, "NULL");
                }
            }

			//<<< SQL TRACE

			return obj_value;
		}

		/// <summary>
		/// Gets all attribute columns in the collection for the current row.
		/// </summary>
		/// <param name="values">Array to store values.</param>
		/// <returns>The number of the stored values.</returns>
		public override int GetValues(object[] values)
		{
			if (values == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));

			for (int i = 0; i < Math.Min(FieldCount, values.Length); i++)
				values[i] = GetValue(i);
			return Math.Min(FieldCount, values.Length);
		}

		/// <summary>
		/// Gets the column ordinal, given the name of the column.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <returns>The column ordinal.</returns>
		public override int GetOrdinal(string name)
		{
            if (name == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAME_NOTFOUND, name));
            }

			// Throw an exception if the ordinal cannot be found.
            for (short cnt = 0; cnt <= FieldCount - 1; cnt++)
            {
                if (string.Compare(GetName(cnt).Trim(), name.Trim(), true, CultureInfo.InvariantCulture) == 0)
                {
                    return cnt;
                }
            }

			throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAME_NOTFOUND, name));
		}

		/// <summary>
		/// Overloaded. Gets the value of a column in its native format.
		/// In C#, this property is the indexer for the MaxDBDataReader class.
		/// </summary>
		public override object this[int i]
		{
			get
			{
				return GetValue(i);
			}
		}

		/// <summary>
		/// Gets the value of a column in its native format.
		///	In C#, this property is the indexer for the MaxDBDataReader class.
		/// </summary>
		public override object this[string name]
		{
			// Look up the ordinal and return the value at that position.
			get
			{
				return GetValue(GetOrdinal(name));
			}
		}

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

		/// <summary>
		/// Gets the value of the specified column as a Boolean.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>Boolean value of the column.</returns>
		public override bool GetBoolean(int i)
		{
			/*
			 * Force the cast to return the type. InvalidCastException
			 * should be thrown if the data is not already of the correct type.
			 */
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			bool bool_value = transl.GetBoolean(CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "BOOLEAN", 1, 0, bool_value.ToString());
            }
			//<<< SQL TRACE

			return bool_value;
		}

		/// <summary>
		/// Gets the value of the specified column as a Byte.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The byte value of the column.</returns>
		public override byte GetByte(int i)
		{
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			byte byte_value = transl.GetByte(this, CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "BYTE", 1, 0, byte_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return byte_value;
		}

		/// <summary>
		/// Reads a stream of bytes from the specified column offset into the buffer an array starting at the given buffer offset.
		/// </summary>
		/// <param name="i">The zero-based column ordinal. </param>
		/// <param name="fieldOffset">The index within the field from which to begin the read operation. </param>
		/// <param name="buffer">The buffer into which to read the stream of bytes. </param>
		/// <param name="bufferoffset">The index for buffer to begin the read operation. </param>
		/// <param name="length">The maximum length to copy into the buffer. </param>
		/// <returns>The actual number of bytes read.</returns>
		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			long result = transl.GetBytes(this, CurrentRecord, fieldOffset, buffer, bufferoffset, length);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
			{
				byte[] logs = new byte[Math.Min(MaxDBLogger.DataSize / 2, length)];
                Buffer.BlockCopy(buffer, bufferoffset, logs, 0, logs.Length);

				LogValue(i + 1, transl, "BYTES", logs.Length, 0, Consts.ToHexString(logs) + (logs.Length < length ? "..." : ""));
			}
			//<<< SQL TRACE

			return result;
		}

		/// <summary>
		/// Gets the value of the specified column as a Char.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The char value of the column.</returns>
		public override char GetChar(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
				throw new InvalidColumnException(i);
			return GetString(i)[0];
		}

		/// <summary>
		/// Reads a stream of chars from the specified column offset into the buffer an array starting at the given buffer offset.
		/// </summary>
		/// <param name="i">The zero-based column ordinal. </param>
		/// <param name="fieldoffset">The index within the field from which to begin the read operation. </param>
		/// <param name="buffer">The buffer into which to read the stream of chars. </param>
		/// <param name="bufferoffset">The index for buffer to begin the read operation. </param>
		/// <param name="length">The maximum length to copy into the buffer. </param>
		/// <returns>The actual number of chars read.</returns>
		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
            if (buffer == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));
            }

            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			long result = transl.GetChars(this, CurrentRecord, fieldoffset, buffer, bufferoffset, length);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
			{
				char[] logs = new char[Math.Min(MaxDBLogger.DataSize, length)];
                Buffer.BlockCopy(buffer, bufferoffset, logs, 0, logs.Length);

				LogValue(i + 1, transl, "CHARS", logs.Length, 0, new string(logs) + (logs.Length < length ? "..." : ""));
			}
			//<<< SQL TRACE

			return result;
		}

		/// <summary>Gets the value of the specified column as a globally-unique identifier (GUID).</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override Guid GetGuid(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			return new Guid(GetString(i));
		}

		/// <summary>Gets the value of the specified column as a 16-bit signed integer.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override short GetInt16(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			short short_value = transl.GetInt16(CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "INT16", 2, 0, short_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return short_value;
		}

		/// <summary>Gets the value of the specified column as a 32-bit signed integer.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override int GetInt32(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			int int_value = transl.GetInt32(CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "INT32", 4, 0, int_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return int_value;
		}

		/// <summary>Gets the value of the specified column as a 64-bit signed integer.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override long GetInt64(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
            if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			long long_value = transl.GetInt64(CurrentRecord);

			//>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "INT64", 8, 0, long_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return long_value;
		}

		/// <summary>Gets the value of the specified column as a single-precision floating point number.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override float GetFloat(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			float float_value = transl.GetFloat(CurrentRecord);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "FLOAT", 4, 0, float_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return float_value;
		}

		///	<summary>Gets the value of the specified column as a double-precision floating point number.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override double GetDouble(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			double double_value = transl.GetDouble(CurrentRecord);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "DOUBLE", 8, 0, double_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return double_value;
		}

		/// <summary>Gets the value of the specified column as a <see cref="String"/> object.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override string GetString(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			string str_value = transl.GetString(this, CurrentRecord);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
            {
				if (str_value != null)
                {
					LogValue(i + 1, transl, "STRING", str_value.Length, 1,
						(str_value.Length <= MaxDBLogger.DataSize ? str_value : str_value.Substring(0, MaxDBLogger.DataSize) + "..."));
                }
				else
                {
					LogValue(i + 1, transl, "STRING", 0, 1, "NULL");
                }
            }
			//<<< SQL TRACE

			return str_value;
		}

		/// <summary>Gets the value of the specified column as a <see cref="Decimal"/> object.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override decimal GetDecimal(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			decimal dec_value = transl.GetDecimal(CurrentRecord);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "DECIMAL", 8, 0, dec_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return dec_value;
		}

		/// <summary>Gets the value of the specified column as a <see cref="DateTime"/> object.</summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns>The value of the specified column.</returns>
		public override DateTime GetDateTime(int i)
		{
			// Force the cast to return the type. InvalidCastException should be thrown if the data is not already of the correct type.
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			DBTechTranslator transl = FindColumnInfo(i);
			DateTime dt_value = transl.GetDateTime(CurrentRecord);

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
            {
                LogValue(i + 1, transl, "DATETIME", 0, 0, dt_value.ToString(CultureInfo.InvariantCulture));
            }
			//<<< SQL TRACE

			return dt_value;
		}

		/// <summary>
		/// Gets a value indicating whether the column contains non-existent or missing values.
		/// </summary>
		/// <param name="i">The zero-based column ordinal.</param>
		/// <returns><b>true</b> if the specified column value is equivalent to DBNull; otherwise, <b>false</b>.</returns>
		public override bool IsDBNull(int i)
		{
			if (i < 0 || i >= FieldCount)
            {
                throw new InvalidColumnException(i);
            }

			return FindColumnInfo(i).IsDBNull(CurrentRecord);
		}

		/// <summary>
		/// Gets a value indicating whether the MaxDBDataReader contains one or more rows.
		/// </summary>
		public override bool HasRows
		{
			get
			{
				return iRowsInResultSet > 0;
			}
		}

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
			foreach (Stream stream in lstOpenStreams)
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
			catch (MaxDBException ex)
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

		/// <summary>
		/// gets an enumerator that can iterate through this collection.
		/// </summary>
		/// <returns>The object that represents an enumerator.</returns>
		public override IEnumerator GetEnumerator()
		{
			return new DbEnumerator(this);
		}

		#region IDisposable Members

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="disposing">Disposing flag</param>
		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
            if (disposing)
            {
                Close();
            }
		}

		#endregion
	}
}

