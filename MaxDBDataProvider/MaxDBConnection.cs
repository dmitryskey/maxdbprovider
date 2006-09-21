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
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif
using System.Runtime.CompilerServices;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data
{
	internal struct ConnectArgs
	{
		public string username;
		public string password;
		public string dbname;
		public string host;
		public int port;
	};

	/// <summary>
	/// SQL Mode
	/// </summary>
	/// <remarks>
	/// copy of vsp001::tsp1_sqlmode
	/// </remarks>
	public enum SqlMode
	{
		Nil = 0,
		SessionSqlMode = 1,
		Internal = 2,
		Ansi = 3,
		Db2 = 4,
		Oracle = 5,
		SapR3 = 6,
	}

	public class MaxDBConnection :
#if NET20
		DbConnection
#else
        IDbConnection, IDisposable
#endif // NET20
	{
		internal MaxDBConnectionStringBuilder mConnStrBuilder;
		private string strConnection;

		internal ConnectArgs mConnArgs;
		internal MaxDBComm mComm;

#if !SAFE
		#region "SQLDBC Wrapper parameters"

		//we cache table names extracted from SELECT ... FOR UPDATE statement
		//SQLDBC library does not store it in its command cache!!!
		//hash algorithm is equal to the SQLDBC counterpart

#if NET20
        private class TableNameHashCodeProvider : IEqualityComparer
        {
            bool IEqualityComparer.Equals(object x, object y)
            {
                return (string)x == (string)y;
            }

            int IEqualityComparer.GetHashCode(object obj)
#else
		private class TableNameHashCodeProvider : IHashCodeProvider
		{
			int IHashCodeProvider.GetHashCode(object obj)
#endif // NET20
            {
				// the X31 hash formula is hash = (hash<<5) - hash + char(i) for i in 1 ... string length
				// as it degenerates when the input are 0's, a little bit decoration is added
				// to hash UTF8 data and UCS2 data equally. 
				// also chars >= 128 are completely skipped.

				string str = (string)obj;
				if (str.Length > 0)
				{
					int result = 0;
					
					foreach(char c in str)
					{
						byte[] b = BitConverter.GetBytes(c);
						if (b[0] < 128 && b[1] == 0)
							result = (result << 5) - result + b[0];
					}
					return result;
				}
				else
					return 0;
			}
		}

		internal Hashtable mTableNames = new Hashtable(new TableNameHashCodeProvider()
#if !NET20
			, new Comparer(System.Globalization.CultureInfo.InvariantCulture)
#endif // !NET20
            );

		#endregion
#endif // !SAFE

		internal MaxDBLogger mLogger;

		// Always have a default constructor.
		public MaxDBConnection()
		{
			mLogger = new MaxDBLogger();
		}

		// Have a constructor that takes a connection string.
		public MaxDBConnection(string connectionString)
			: this()
		{
			strConnection = connectionString;
			mConnStrBuilder = new MaxDBConnectionStringBuilder(connectionString);
			SetConnectionParameters();
		}

		private void SetConnectionParameters()
		{
			if (mConnStrBuilder.DataSource != null)
			{
				string[] hostPort = mConnStrBuilder.DataSource.Split(':');
				mConnArgs.host = hostPort[0];
				mConnArgs.port = 0;
				try
				{
					mConnArgs.port = int.Parse(hostPort[1], CultureInfo.InvariantCulture);
				}
				catch (IndexOutOfRangeException)
				{
				}
				catch (ArgumentNullException)
				{
				}
				catch (FormatException)
				{
				}
				catch (OverflowException)
				{
				}
			}

			mConnArgs.dbname = mConnStrBuilder.InitialCatalog;
			mConnArgs.username = mConnStrBuilder.UserId;
			mConnArgs.password = mConnStrBuilder.Password;
		}

		public Encoding DatabaseEncoding
		{
			get
			{
#if SAFE
				return mComm.mEncoding;
#else
				return UnsafeNativeMethods.SQLDBC_Connection_isUnicodeDatabase(mComm.mConnectionHandler) == 1 ? Encoding.Unicode : Encoding.ASCII;
#endif // SAFE
			}
		}

		public SqlMode SqlMode
		{
			get
			{
				return mConnStrBuilder.Mode;
			}
			set
			{
				mConnStrBuilder.Mode = value;
				if (mComm != null)
					mComm.mConnStrBuilder.Mode = value;
			}
		}

		public bool AutoCommit
		{
			get
			{
				if (mComm == null || State != ConnectionState.Open)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));

#if SAFE
				return mComm.bAutoCommit;
#else
				return UnsafeNativeMethods.SQLDBC_Connection_getAutoCommit(mComm.mConnectionHandler) == SQLDBC_BOOL.SQLDBC_TRUE;
#endif // SAFE
			}
			set
			{
				if (mComm == null || State != ConnectionState.Open)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));

				//>>> SQL TRACE
				if (mLogger.TraceSQL)
					mLogger.SqlTrace(DateTime.Now, "::SET AUTOCOMMIT " + (value ? "ON" : "OFF"));
				//<<< SQL TRACE				

#if SAFE
				mComm.bAutoCommit = value;
#else
				UnsafeNativeMethods.SQLDBC_Connection_setAutoCommit(mComm.mConnectionHandler, value ? SQLDBC_BOOL.SQLDBC_TRUE : SQLDBC_BOOL.SQLDBC_FALSE);
#endif // SAFE
			}
		}

#if NET20
		public override string ServerVersion
#else
        public string ServerVersion
#endif // NET20
		{
			get
			{
				int version =
#if SAFE
					mComm.iKernelVersion;
#else
					UnsafeNativeMethods.SQLDBC_Connection_getKernelVersion(mComm.mConnectionHandler);
#endif // SAFE
				int correction_level = version % 100;
				int minor_release = ((version - correction_level) % 10000) / 100;
				int mayor_release = (version - minor_release * 100 - correction_level) / 10000;
				return mayor_release.ToString(CultureInfo.InvariantCulture) + "." +
					minor_release.ToString(CultureInfo.InvariantCulture) + "." +
					correction_level.ToString("d2", CultureInfo.InvariantCulture);
			}
		}

		public static int MapIsolationLevel(IsolationLevel level)
		{
			switch (level)
			{
				case IsolationLevel.ReadUncommitted:
					return 0;
				case IsolationLevel.ReadCommitted:
					return 1;
				case IsolationLevel.RepeatableRead:
					return 2;
				case IsolationLevel.Serializable:
					return 3;
				default:
					return 1;
			}
		}

		private void SetIsolationLevel(IsolationLevel level)
		{
#if SAFE
			if (mComm.mIsolationLevel != level)
			{
				AssertOpen();
				string cmd = "SET ISOLATION LEVEL " + MapIsolationLevel(level).ToString(CultureInfo.InvariantCulture);
				MaxDBRequestPacket requestPacket = mComm.GetRequestPacket();
				byte oldMode = requestPacket.SwitchSqlMode((byte)SqlMode.Internal);
				requestPacket.InitDbsCommand(mComm.bAutoCommit, cmd);
				try
				{
					mComm.Execute(mConnArgs, requestPacket, this, GCMode.GC_ALLOWED);
				}
				catch (MaxDBTimeoutException)
				{
					requestPacket.SwitchSqlMode(oldMode);
				}

				mComm.mIsolationLevel = level;
			}
#else
			mComm.mIsolationLevel = level;

			if (UnsafeNativeMethods.SQLDBC_Connection_setTransactionIsolation(mComm.mConnectionHandler, MapIsolationLevel(level)) 
				!= SQLDBC_Retcode.SQLDBC_OK) 
				MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.CONNECTION_ISOLATIONLEVEL),
					UnsafeNativeMethods.SQLDBC_Connection_getError(mComm.mConnectionHandler));
#endif // SAFE
		}

		internal void AssertOpen()
		{
			if (State == ConnectionState.Closed)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
		}

#if SAFE
		#region "Methods to support native protocol"


		#endregion
#endif // SAFE

		#region IDbConnection Members

#if NET20
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
#else
		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel isolationLevel)
#endif // NET20
		{
			return BeginTransaction(isolationLevel);
		}

#if NET20
		public new MaxDBTransaction BeginTransaction(IsolationLevel isolationLevel)
#else
        public MaxDBTransaction BeginTransaction(IsolationLevel isolationLevel)
#endif // NET20
		{
			SetIsolationLevel(isolationLevel);
			return new MaxDBTransaction(this);
		}

#if !NET20
		IDbTransaction IDbConnection.BeginTransaction()
		{
			return BeginTransaction();
		}
#endif

#if NET20
		public new MaxDBTransaction BeginTransaction()
#else
        public MaxDBTransaction BeginTransaction()
#endif // NET20
		{
			return new MaxDBTransaction(this);
		}

#if NET20
		public override void ChangeDatabase(string databaseName)
#else
        public void ChangeDatabase(string databaseName)
#endif // NET20
		{
			// Change the database setting on the back-end. Note that it is a method
			// and not a property because the operation requires an expensive round trip.
			mConnArgs.dbname = databaseName;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
#if NET20
		public override void Close()
#else
        public void Close()
#endif // NET20
		{
			/*
			 * Close the database connection and set the ConnectionState
			 * property. If the underlying connection to the server is
			 * being pooled, Close() will release it back to the pool.
			 */
			if (State == ConnectionState.Open)
			{
				//>>> SQL TRACE
				if (mLogger.TraceSQL)
					mLogger.SqlTrace(DateTime.Now, "::CLOSE CONNECTION");
				//<<< SQL TRACE

				mLogger.Flush();

				if (mComm != null)
				{
					if (mConnStrBuilder.Pooling)
						MaxDBConnectionPool.ReleaseEntry(this);
					else
					{
#if SAFE
						mComm.Close(true, true);
#else
						mComm.Close();
#endif // SAFE
						mComm.Dispose();
					}

					mComm = null;
				}
			}
		}

#if NET20
		public override string ConnectionString
#else
        public string ConnectionString
#endif // NET20
		{
			get
			{
				// Always return exactly what the user set. Security-sensitive information may be removed.
				return strConnection;
			}
			set
			{
				strConnection = value;
				mConnStrBuilder = new MaxDBConnectionStringBuilder(value);
				SetConnectionParameters();
			}
		}

#if NET20
		public override int ConnectionTimeout
#else
        public int ConnectionTimeout
#endif // NET20
		{
			get
			{
				// Returns the connection time-out value set in the connection
				// string. Zero indicates an indefinite time-out period.
				return mConnStrBuilder.Timeout;
			}
		}

#if NET20
		protected override DbCommand CreateDbCommand()
		{
			return CreateCommand();
		}
#else
		IDbCommand IDbConnection.CreateCommand()
		{
			return CreateCommand();
		}
#endif // NET20

#if NET20
		public new MaxDBCommand CreateCommand()
#else
		public MaxDBCommand CreateCommand()
#endif // NET20
		{
			// Return a new instance of a command object.
			return new MaxDBCommand(string.Empty, this);
		}

		internal bool Ping(MaxDBComm communication)
		{
			SqlMode oldMode = communication.mConnStrBuilder.Mode;
			MaxDBComm oldComm = mComm;
			mComm = communication;
			if (mComm.mConnStrBuilder.Mode != SqlMode.Internal)
				mComm.mConnStrBuilder.Mode = SqlMode.Internal;
			try
			{
				using (MaxDBCommand cmd = new MaxDBCommand("PING", this))
					cmd.ExecuteNonQuery();
				
				return true;
			}
			catch
			{
				return false;
			}
			finally
			{
				mComm = oldComm;
				if (oldMode != SqlMode.Internal)
					communication.mConnStrBuilder.Mode = oldMode;
			}
		}

#if NET20

		private DataTable ExecuteInternalQuery(string sql, string table, MaxDBParameterCollection parameters)
		{
			DataTable dt = new DataTable(table);
			SqlMode oldMode = mComm.mConnStrBuilder.Mode;
			if (oldMode != SqlMode.Internal)
				mComm.mConnStrBuilder.Mode = SqlMode.Internal;

			try
			{
				using (MaxDBCommand cmd = new MaxDBCommand(sql, this))
				{
					if (parameters != null)
						cmd.Parameters.AddRange(parameters.ToArray());
					MaxDBDataAdapter da = new MaxDBDataAdapter();
					da.SelectCommand = cmd;
					da.Fill(dt);
				}
			}
			finally
			{
				if (mComm.mConnStrBuilder.Mode != oldMode)
					mComm.mConnStrBuilder.Mode = oldMode;
			}

			return dt;
		}

		private DataTable ExecuteInternalQuery(string sql, string table)
		{
			return ExecuteInternalQuery(sql, table, null);
		}

		public override DataTable GetSchema()
		{
			return GetSchema("MetaDataCollections");
		}

		public override DataTable GetSchema(string collectionName)
		{
			return GetSchema(collectionName, null);
		}


		public override DataTable GetSchema(string collectionName, string[] restrictionValues)
		{
			DataTable dt = new DataTable("SchemaTable");
			dt.Locale = CultureInfo.InvariantCulture;

			if (string.Compare(collectionName, "MetaDataCollections", true, CultureInfo.InvariantCulture) == 0)
			{
				dt.Columns.Add(new DataColumn("CollectionName", typeof(string)));
				dt.Columns.Add(new DataColumn("NumberOfRestriction", typeof(int)));
				dt.Columns.Add(new DataColumn("NumberOfIdentifierParts", typeof(int)));

				dt.Rows.Add("MetaDataCollections", 0, 0);
				dt.Rows.Add("DataSourceInformation", 0, 0);
				dt.Rows.Add("DataTypes", 0, 0);
				dt.Rows.Add("ReservedWords", 0, 0);
				dt.Rows.Add("Restrictions", 0, 0);
				dt.Rows.Add("Catalogs", 0, 0);
				dt.Rows.Add("Schemas", 0, 0);
				dt.Rows.Add("TableTypes", 0, 0);
				dt.Rows.Add("ForeignKeys", 2, 0);
				dt.Rows.Add("PrimaryKeys", 2, 0);
				dt.Rows.Add("Procedures", 2, 0);
				dt.Rows.Add("SuperTables", 0, 0);
				dt.Rows.Add("SuperTypes", 0, 0);
				dt.Rows.Add("TablePrivileges", 2, 0);
				dt.Rows.Add("VersionColumns", 0, 0);
				dt.Rows.Add("BestRowIdentifier", 0, 0);
				dt.Rows.Add("Indexes", 4, 0);
				dt.Rows.Add("UserDefinedTypes", 0, 0);
				dt.Rows.Add("Attributes", 2, 0);
				dt.Rows.Add("ColumnPrivileges", 3, 0);
				dt.Rows.Add("Columns", 3, 0);
				dt.Rows.Add("ProcedureColumns", 3, 0);
				dt.Rows.Add("Tables", 2, 0);
				dt.Rows.Add("Constraints", 2, 0);
				dt.Rows.Add("SystemInfo", 0, 0);
			}

			if (string.Compare(collectionName, "DataSourceInformation", true, CultureInfo.InvariantCulture) == 0)
			{
				dt.Columns.Add(new DataColumn("CompositeIdentifierSeparatorPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("DataSourceProductName", typeof(string)));
				dt.Columns.Add(new DataColumn("DataSourceProductVersion", typeof(string)));
				dt.Columns.Add(new DataColumn("DataSourceProductVersionNormalized", typeof(string)));
				dt.Columns.Add(new DataColumn("GroupByBehavior", typeof(GroupByBehavior)));
				dt.Columns.Add(new DataColumn("IdentifierPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("IdentifierCase", typeof(IdentifierCase)));
				dt.Columns.Add(new DataColumn("OrderByColumnsInSelect", typeof(bool)));
				dt.Columns.Add(new DataColumn("ParameterMarkerFormat", typeof(string)));
				dt.Columns.Add(new DataColumn("ParameterMarkerPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("ParameterNameMaxLength", typeof(int)));
				dt.Columns.Add(new DataColumn("ParameterNamePattern", typeof(string)));
				dt.Columns.Add(new DataColumn("QuotedIdentifierPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("QuotedIdentifierCase", typeof(IdentifierCase)));
				dt.Columns.Add(new DataColumn("StatementSeparatorPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("StringLiteralPattern", typeof(string)));
				dt.Columns.Add(new DataColumn("SupportedJoinOperators ", typeof(SupportedJoinOperators)));

				string langSpecific = "\\u0080-\\uFFFF";
				if (DatabaseEncoding != Encoding.Unicode)
				{
					Encoding enc = Encoding.GetEncoding(1252);
					byte[] buf = new byte[1];
					StringBuilder sb = new StringBuilder();
					for (byte b = 0x80; b < 0xFF; b++)
					{
						buf[0] = b;
						if (char.IsLetter(enc.GetString(buf), 0))
							sb.Append("\\x").Append(b.ToString("x2", CultureInfo.InvariantCulture));
					}
					langSpecific = sb.ToString();
				}

				string identifierPattern = "(([A-Za-z" + langSpecific + "#@\\$][\\w" + langSpecific + "#@\\$_]*)|(\".+\"))";

				dt.Rows.Add("\\.", "MaxDB", ServerVersion, ServerVersion, GroupByBehavior.Unrelated,
					identifierPattern, IdentifierCase.Insensitive, false, ":{0}", ":" + identifierPattern, 128,
					identifierPattern, "(([^\\\"]|\\\"\\\")*)", IdentifierCase.Sensitive, "\r\n", "(\'([^\']|\'\')*\')",
					SupportedJoinOperators.FullOuter);
			}

			if (string.Compare(collectionName, "DataTypes", true, CultureInfo.InvariantCulture) == 0)
			{
				dt.Columns.Add(new DataColumn("TypeName", typeof(string)));
				dt.Columns.Add(new DataColumn("ProviderDbType", typeof(int)));
				dt.Columns.Add(new DataColumn("ColumnSize", typeof(long)));
				dt.Columns.Add(new DataColumn("CreateFormat", typeof(string)));
				dt.Columns.Add(new DataColumn("CreateParameters", typeof(string)));
				dt.Columns.Add(new DataColumn("DataType", typeof(string)));
				dt.Columns.Add(new DataColumn("IsAutoincrementable", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsBestMatch", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsCaseSensitive", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsFixedLength", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsFixedPrecisionScale", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsLong", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsNullable", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsSearchable", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsSearchableWithLike", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsUnsigned", typeof(bool)));
				dt.Columns.Add(new DataColumn("MaximumScale", typeof(short)));
				dt.Columns.Add(new DataColumn("MinimumScale", typeof(short)));
				dt.Columns.Add(new DataColumn("IsConcurrencyType", typeof(bool)));
				dt.Columns.Add(new DataColumn("IsLiteralsSupported ", typeof(bool)));
				dt.Columns.Add(new DataColumn("LiteralPrefix", typeof(string)));
				dt.Columns.Add(new DataColumn("LitteralSuffix", typeof(string)));

				bool isUnicode = (DatabaseEncoding == Encoding.Unicode);

				dt.Rows.Add(new object[]{"CHAR", MaxDBType.CharA, 8000, "CHAR({0})", "length", typeof(string).ToString(),
					false, !isUnicode, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				dt.Rows.Add(new object[]{"CHAR ASCII", MaxDBType.CharA, 8000, "CHAR({0}) ASCII", "length", typeof(string).ToString(),
					false, false, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				if (isUnicode)
					dt.Rows.Add(new object[]{"CHAR UNICODE", MaxDBType.Unicode, 4000, "CHAR({0}) UNICODE", "length", typeof(string).ToString(),
						false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
						true, "'", "'"});
				dt.Rows.Add(new object[]{"CHAR BYTE", MaxDBType.CharB, 8000, "CHAR({0}) BYTE", "length", typeof(byte[]).ToString(),
					false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "X'", "X'"});
				dt.Rows.Add(new object[]{"VARCHAR", MaxDBType.VarCharA, 8000, "VARCHAR({0})", "length", typeof(string).ToString(),
					false, !isUnicode, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				dt.Rows.Add(new object[]{"VARCHAR ASCII", MaxDBType.VarCharA, 8000, "VARCHAR({0}) ASCII", "length", typeof(string).ToString(),
					false, false, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				if (isUnicode)
					dt.Rows.Add(new object[]{"VARCHAR UNICODE", MaxDBType.VarCharUni, 4000, "VARCHAR({0}) UNICODE", "length", typeof(string).ToString(),
						false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
						true, string.Empty, string.Empty});
				dt.Rows.Add(new object[]{"VARCHAR BYTE", MaxDBType.VarCharB, 8000, "VARCHAR({0}) BYTE", "length", typeof(byte[]).ToString(),
					false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "X'", "X'"});
				dt.Rows.Add(new object[]{"LONG", MaxDBType.LongA, 2147483648, "LONG", DBNull.Value, typeof(string).ToString(),
					false, !isUnicode, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				dt.Rows.Add(new object[]{"LONG VARCHAR", MaxDBType.LongA, 2147483648, "LONG VARCHAR", DBNull.Value, typeof(string).ToString(),
					false, false, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				dt.Rows.Add(new object[]{"LONG ASCII", MaxDBType.LongA, 2147483648, "LONG ASCII", DBNull.Value, typeof(string).ToString(),
					false, false, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "'", "'"});
				if (isUnicode)
					dt.Rows.Add(new object[]{"LONG UNICODE", MaxDBType.LongUni, 1073741824, "LONG UNICODE", DBNull.Value, typeof(string).ToString(),
						false, true, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
						true, "'", "'"});
				dt.Rows.Add(new object[]{"LONG BYTE", MaxDBType.LongB, 2147483648, "LONG BYTE", DBNull.Value, typeof(byte[]).ToString(),
					false, true, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, "X'", "X'"});
				dt.Rows.Add(new object[]{"BOOLEAN", MaxDBType.Boolean, 1, "BOOLEAN", DBNull.Value, typeof(bool).ToString(),
					false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					true, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"FIXED", MaxDBType.Fixed, 38, "FIXED({0},{1})", "precision,scale", typeof(decimal).ToString(),
					true, true, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"NUMERIC", MaxDBType.Number, 38, "NUMERIC({0},{1})", "precision,scale", typeof(decimal).ToString(),
					true, false, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"DECIMAL", MaxDBType.VFloat, 38, "DECIMAL({0},{1})", "precision,scale", typeof(decimal).ToString(),
					true, false, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"FLOAT", MaxDBType.Float, 38, "FLOAT({0})", "precision", typeof(decimal).ToString(),
					false, true, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"REAL", MaxDBType.Float, 38, "REAL({0})", "precision", typeof(decimal).ToString(),
					false, true, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"DOUBLE PRECISION", MaxDBType.VFloat, 38, "DOUBLE PRECISION", "precision", typeof(decimal).ToString(),
					false, true, false, true, false, false, true, true, false, false, 38, 0, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"INTEGER", MaxDBType.Integer, 10, "INTEGER", DBNull.Value, typeof(int).ToString(),
					true, true, false, true, true, false, true, true, false, false, DBNull.Value, DBNull.Value, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"SMALLINT", MaxDBType.SmallInt, 5, "SMALLINT", DBNull.Value, typeof(short).ToString(),
					true, true, false, true, true, false, true, true, false, false, DBNull.Value, DBNull.Value, false,
					DBNull.Value, DBNull.Value, DBNull.Value});

				string DatePattern;
				string TimePattern;
				string TimestampPattern;
				string DateTimeFormat = "INTERNAL";

				DataTable formatTable = ExecuteInternalQuery("SELECT VALUE FROM DBA.DBPARAMETERS WHERE DESCRIPTION = 'DATE_TIME_FORMAT'", "DateTimeFormat");
				if (formatTable.Rows.Count > 0)
					DateTimeFormat = formatTable.Rows[0].ToString();

				switch (DateTimeFormat)
				{
					case "EUR":
						DatePattern = "DD.MM.YYYY";
						TimePattern = "HH.MM.SS";
						TimestampPattern = "YYYY-MM-DD-HH.MM.SS.MMMMMM";
						break;
					case "ISO":
						DatePattern = "YYYY-MM-DD";
						TimePattern = "HH:MM:SS";
						TimestampPattern = "YYYY-MM-DD HH:MM:SS.MMMMMM";
						break;
					case "JIS":
						DatePattern = "YYYY-MM-DD";
						TimePattern = "HH:MM:SS";
						TimestampPattern = "YYYY-MM-DD-HH:MM:SS.MMMMMM";
						break;
					case "USA":
						DatePattern = "MM/DD/YYYY";
						TimePattern = "HH:MM AM";
						TimestampPattern = "YYYY-MM-DD-HH:MM:SS.MMMMMM";
						break;
					default:
						DatePattern = "YYYYMMDD";
						TimePattern = "HHHHMMSS";
						TimestampPattern = "YYYYMMDDHHMMSSMMMMMM";
						break;
				};

				dt.Rows.Add(new object[]{"DATE", MaxDBType.Date, DatePattern.Length, "DATE", DBNull.Value, typeof(DateTime).ToString(),
					false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"TIME", MaxDBType.Time, TimePattern.Length, "TIME", DBNull.Value, typeof(DateTime).ToString(),
					false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
				dt.Rows.Add(new object[]{"TIMESTAMP", MaxDBType.Timestamp, TimestampPattern.Length, "TIMESTAMP", DBNull.Value, typeof(DateTime).ToString(),
					false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
					DBNull.Value, DBNull.Value, DBNull.Value});
			}

			if (string.Compare(collectionName, "Restrictions", true, CultureInfo.InvariantCulture) == 0)
			{
				dt.Columns.Add(new DataColumn("CollectionName", typeof(string)));
				dt.Columns.Add(new DataColumn("RestrictionName", typeof(string)));
				dt.Columns.Add(new DataColumn("ParameterName", typeof(string)));
				dt.Columns.Add(new DataColumn("RestrictionDefault", typeof(string)));
				dt.Columns.Add(new DataColumn("RestrictionNumber", typeof(int)));

				dt.Rows.Add(new object[] { "ForeignKeys", "PKTABLE_SCHEM", ":PKTABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "ForeignKeys", "PKTABLE_NAME", ":PKTABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "PrimaryKeys", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "PrimaryKeys", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "Procedures", "PROCEDURE_SCHEM", ":PROCEDURE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "Procedures", "PROCEDURE_NAME", ":PROCEDURE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "TablePrivileges", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "TablePrivileges", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "BestRowIdentifier", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "BestRowIdentifier", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "Indexes", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "Indexes", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "Indexes", "unique", null, null, 3 });
				dt.Rows.Add(new object[] { "Indexes", "approximate", null, null, 4 });
				dt.Rows.Add(new object[] { "Attributes", "SCOPE_SCHEMA", ":SCOPE_SCHEMA", null, 1 });
				dt.Rows.Add(new object[] { "Attributes", "SCOPE_NAME", ":SCOPE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "ColumnPrivileges", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "ColumnPrivileges", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "ColumnPrivileges", "COLUMN_NAME", ":COLUMN_NAME", null, 3 });
				dt.Rows.Add(new object[] { "Columns", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "Columns", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "Columns", "COLUMN_NAME", ":COLUMN_NAME", null, 3 });
				dt.Rows.Add(new object[] { "ProcedureColumns", "PROCEDURE_SCHEM", ":PROCEDURE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "ProcedureColumns", "PROCEDURE_NAME", ":PROCEDURE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "ProcedureColumns", "COLUMN_NAME", ":COLUMN_NAME", null, 3 });
				dt.Rows.Add(new object[] { "Tables", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "Tables", "TABLE_NAME", ":TABLE_NAME", null, 2 });
				dt.Rows.Add(new object[] { "Constraints", "TABLE_SCHEM", ":TABLE_SCHEM", null, 1 });
				dt.Rows.Add(new object[] { "Constraints", "TABLE_NAME", ":TABLE_NAME", null, 2 });
			}

			if (string.Compare(collectionName, "ReservedWords", true, CultureInfo.InvariantCulture) == 0)
			{
				dt.Columns.Add(new DataColumn("ReservedWord", typeof(string)));

				string[] keywords = new string[] { 
					"ABS", "ABSOLUTE", "ACOS", "ADDDATE", "ADDTIME", "ALL", "ALPHA", "ALTER", "ANY", "ASCII", "ASIN", 
					"ATAN", "ATAN2", "AVG", "BINARY", "BIT", "BOOLEAN", "BYTE", "CASE", "CEIL", "CEILING", "CHAR", 
					"CHARACTER", "CHECK", "CHR", "COLUMN", "CONCAT", "CONSTRAINT", "COS", "COSH", "COT", "COUNT", 
					"CROSS", "CURDATE", "CURRENT", "CURTIME", "DATABASE", "DATE", "DATEDIFF", "DAY", "DAYNAME", 
					"DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "DEC", "DECIMAL", "DECODE", "DEFAULT", "DEGREES", 
					"DELETE", "DIGITS", "DISTINCT", "DOUBLE", "EXCEPT", "EXISTS", "EXP", "EXPAND", "FIRST", "FIXED", 
					"FLOAT", "FLOOR", "FOR", "FROM", "FULL", "GET_OBJECTNAME", "GET_SCHEMA", "GRAPHIC", "GREATEST", 
					"GROUP", "HAVING", "HEX", "HEXTORAW", "HOUR", "IFNULL", "IGNORE", "INDEX", "INITCAP", "INNER", 
					"INSERT", "INT", "INTEGER", "INTERNAL", "INTERSECT", "INTO", "JOIN", "KEY", "LAST", "LCASE", 
					"LEAST", "LEFT", "LENGTH", "LFILL", "LIST", "LN", "LOCATE", "LOG", "LOG10", "LONG", "LONGFILE", 
					"LOWER", "LPAD", "LTRIM", "MAKEDATE", "MAKETIME", "MAPCHAR", "MAX", "MBCS", "MICROSECOND", "MIN", 
					"MINUTE", "MOD", "MONTH", "MONTHNAME", "NATURAL", "NCHAR", "NEXT", "NO", "NOROUND", "NOT", "NOW", 
					"NULL", "NUM", "NUMERIC", "OBJECT", "OF", "ON", "ORDER", "PACKED", "PI", "POWER", "PREV", "PRIMARY", 
					"RADIANS", "REAL", "REJECT", "RELATIVE", "REPLACE", "RFILL", "RIGHT", "ROUND", "ROWID", "ROWNO", 
					"RPAD", "RTRIM", "SECOND", "SELECT", "SELUPD", "SERIAL", "SET", "SHOW", "SIGN", "SIN", "SINH", "SMALLINT", 
					"SOME", "SOUNDEX", "SPACE", "SQRT", "STAMP", "STATISTICS", "STDDEV", "SUBDATE", "SUBSTR", "SUBSTRING", 
					"SUBTIME", "SUM", "SYSDBA", "TABLE", "TAN", "TANH", "TIME", "TIMEDIFF", "TIMESTAMP", "TIMEZONE", "TO", 
					"TOIDENTIFIER", "TRANSACTION", "TRANSLATE", "TRIM", "TRUNC", "TRUNCATE", "UCASE", "UID", "UNICODE", "UNION", 
					"UPDATE", "UPPER", "USER", "USERGROUP", "USING", "UTCDATE", "UTCDIFF", "VALUE", "VALUES", "VARCHAR", 
					"VARGRAPHIC", "VARIANCE", "WEEK", "WEEKOFYEAR", "WHEN", "WHERE", "WITH", "YEAR", "ZONED" };
				foreach (string keyword in keywords)
					dt.Rows.Add(keyword);
			}

			if (string.Compare(collectionName, "Catalogs", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.CATALOGS", "Catalogs");

			if (string.Compare(collectionName, "Schemas", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.SCHEMAS ORDER BY TABLE_SCHEM", "Schemas");

			if (string.Compare(collectionName, "TableTypes", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.TABLETYPES ORDER BY TABLE_TYPE", "TableTypes");

			if (string.Compare(collectionName, "ForeignKeys", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.CROSSREFERENCES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND PKTABLE_SCHEM = :PKTABLE_SCHEM ";
					parameters.Add(":PKTABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND PKTABLE_NAME = :PKTABLE_NAME ";
					parameters.Add(":PKTABLE_NAME", restrictionValues[1]);
				}

				sql += "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
				dt = ExecuteInternalQuery(sql, "ForeignKeys", parameters);
			}

			if (string.Compare(collectionName, "PrimaryKeys", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.PRIMARYKEYS WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM = :TABLE_SCHEM ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME = :TABLE_NAME ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				dt = ExecuteInternalQuery(sql, "PrimaryKeys", parameters);
			}

			if (string.Compare(collectionName, "Procedures", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.PROCEDURES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND PROCEDURE_SCHEM LIKE :PROCEDURE_SCHEM ESCAPE '\\' ";
					parameters.Add(":PROCEDURE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND PROCEDURE_NAME LIKE :PROCEDURE_NAME ESCAPE '\\' ";
					parameters.Add(":PROCEDURE_NAME", restrictionValues[1]);
				}

				sql += "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME";

				dt = ExecuteInternalQuery(sql, "Procedures", parameters);
			}

			if (string.Compare(collectionName, "SuperTables", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.SUPERTABLES", "SuperTables");

			if (string.Compare(collectionName, "SuperTypes", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.SUPERTYPES", "SuperTypes");

			if (string.Compare(collectionName, "TablePrivileges", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.TABLEPRIVILEGES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM LIKE :TABLE_SCHEM ESCAPE '\\' ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME LIKE :TABLE_NAME ESCAPE '\\' ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				sql += "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE";

				dt = ExecuteInternalQuery(sql, "TablePrivileges", parameters);
			}

			if (string.Compare(collectionName, "VersionColumns", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.VERSIONCOLUMNS", "VersionColumns");

			if (string.Compare(collectionName, "BestRowIdentifier", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.BESTROWIDENTIFIER WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM = :TABLE_SCHEM ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME = :TABLE_NAME ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				dt = ExecuteInternalQuery(sql, "BestRowIdentifier", parameters);
			}

			if (string.Compare(collectionName, "Indexes", true, CultureInfo.InvariantCulture) == 0)
			{
				bool unique = (restrictionValues != null && restrictionValues.Length > 2 && restrictionValues[2] != null) &&
						(string.Compare(restrictionValues[2].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
						 string.Compare(restrictionValues[2].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
						 restrictionValues[2].Trim() == "1");

				bool approximate = (restrictionValues != null && restrictionValues.Length > 3 && restrictionValues[3] != null) &&
						(string.Compare(restrictionValues[3].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
						 string.Compare(restrictionValues[3].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
						 restrictionValues[3].Trim() == "1");

				string sql = "SELECT * FROM SYSJDBC.";
				if (approximate)
					sql += "APPROXINDEXINFO";
				else
					sql += "INDEXINFO";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();

				if (restrictionValues != null && restrictionValues.Length > 2)
				{
					sql += " WHERE NON_UNIQUE = :NON_UNIQUE ";
					parameters.Add(":NON_UNIQUE", !unique);
				}
				else
					sql += " WHERE 1=1 ";
				
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM = :TABLE_SCHEM ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME = :TABLE_NAME ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				sql += "ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";

				dt = ExecuteInternalQuery(sql, "Indexes", parameters);
			}

			if (string.Compare(collectionName, "UserDefinedTypes", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.UDTS", "UserDefinedTypes");

			if (string.Compare(collectionName, "Attributes", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.ATTRIBUTES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND SCOPE_SCHEMA LIKE :SCOPE_SCHEMA ESCAPE '\\' ";
					parameters.Add(":SCOPE_SCHEMA", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND SCOPE_NAME LIKE :SCOPE_NAME ESCAPE '\\' ";
					parameters.Add(":SCOPE_NAME", restrictionValues[1]);
				}

				dt = ExecuteInternalQuery(sql, "Attributes", parameters);
			}

			if (string.Compare(collectionName, "ColumnPrivileges", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.COLUMNPRIVILEGES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM = :TABLE_SCHEM ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME = :TABLE_NAME ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				if (restrictionValues != null && restrictionValues.Length > 2 && restrictionValues[2] != null)
				{
					sql += " AND COLUMN_NAME LIKE :COLUMN_NAME ESCAPE '\\' ";
					parameters.Add(":COLUMN_NAME", restrictionValues[2]);
				}

				dt = ExecuteInternalQuery(sql, "ColumnPrivileges", parameters);
			}

			if (string.Compare(collectionName, "Columns", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.COLUMNS WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM LIKE :TABLE_SCHEM ESCAPE '\\' ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME LIKE :TABLE_NAME ESCAPE '\\' ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				if (restrictionValues != null && restrictionValues.Length > 2 && restrictionValues[2] != null)
				{
					sql += " AND COLUMN_NAME LIKE :COLUMN_NAME ESCAPE '\\' ";
					parameters.Add(":COLUMN_NAME", restrictionValues[2]);
				}

				sql += "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

				dt = ExecuteInternalQuery(sql, "Columns", parameters);
			}

			if (string.Compare(collectionName, "ProcedureColumns", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.PROCEDURECOLUMNS WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND PROCEDURE_SCHEM LIKE :PROCEDURE_SCHEM ESCAPE '\\' ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND PROCEDURE_NAME LIKE :PROCEDURE_NAME ESCAPE '\\' ";
					parameters.Add(":PROCEDURE_NAME", restrictionValues[1]);
				}

				if (restrictionValues != null && restrictionValues.Length > 2 && restrictionValues[2] != null)
				{
					sql += " AND COLUMN_NAME LIKE :COLUMN_NAME ESCAPE '\\' ";
					parameters.Add(":COLUMN_NAME", restrictionValues[2]);
				}

				sql += "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, ORDINAL_POSITION";

				dt = ExecuteInternalQuery(sql, "ProcedureColumns", parameters);
			}

			if (string.Compare(collectionName, "Tables", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.TABLES WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM LIKE :TABLE_SCHEM ESCAPE '\\' ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME LIKE :TABLE_NAME ESCAPE '\\' ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				dt = ExecuteInternalQuery(sql, "Tables", parameters);
			}

			if (string.Compare(collectionName, "Constraints", true, CultureInfo.InvariantCulture) == 0)
			{
				string sql = "SELECT * FROM SYSJDBC.CONSTRAINTS WHERE 1=1 ";

				MaxDBParameterCollection parameters = new MaxDBParameterCollection();
				if (restrictionValues != null && restrictionValues.Length > 0 && restrictionValues[0] != null)
				{
					sql += " AND TABLE_SCHEM LIKE :TABLE_SCHEM ESCAPE '\\' ";
					parameters.Add(":TABLE_SCHEM", restrictionValues[0]);
				}

				if (restrictionValues != null && restrictionValues.Length > 1 && restrictionValues[1] != null)
				{
					sql += " AND TABLE_NAME LIKE :TABLE_NAME ESCAPE '\\' ";
					parameters.Add(":TABLE_NAME", restrictionValues[1]);
				}

				dt = ExecuteInternalQuery(sql, "Constraints", parameters);
			}

			if (string.Compare(collectionName, "SystemInfo", true, CultureInfo.InvariantCulture) == 0)
				dt = ExecuteInternalQuery("SELECT * FROM SYSJDBC.SYSTEMINFO", "SystemInfo");

			return dt;
		}
#endif // NET20

#if NET20
		public override string Database
#else
        public string Database
#endif // NET20
		{
			get
			{
				return mConnArgs.dbname;
			}
		}

#if NET20
		public override string DataSource
#else
        public string DataSource
#endif // NET20
		{
			get
			{
				return mConnArgs.host;
			}
		}

#if NET20
		public override void Open()
#else
        public void Open()
#endif // NET20
		{
			if (mConnStrBuilder.Pooling)
			{
#if SAFE
				mComm = MaxDBConnectionPool.GetPoolEntry(this, mLogger);
#else
				mComm = MaxDBConnectionPool.GetPoolEntry(this);
#endif // SAFE
			}
			else
			{
#if SAFE
				mComm = new MaxDBComm(mLogger);
#else
				mComm = new MaxDBComm();
#endif //SAFE
				mComm.mConnStrBuilder = mConnStrBuilder;

				mComm.Open(mConnArgs);
			}
		}

#if NET20
		public override ConnectionState State
#else
        public ConnectionState State
#endif // NET20
		{
			get
			{
#if SAFE
				return mComm != null && mComm.iSessionID >= 0 ? ConnectionState.Open : ConnectionState.Closed;
#else
				if (mComm != null && mComm.mConnectionHandler != IntPtr.Zero &&
					UnsafeNativeMethods.SQLDBC_Connection_isConnected(mComm.mConnectionHandler) == SQLDBC_BOOL.SQLDBC_TRUE)
					return ConnectionState.Open;
				else
					return ConnectionState.Closed;
#endif // SAFE
			}
		}

		#endregion

		public static void ClearPool(MaxDBConnection connection)
		{
			MaxDBConnectionPool.ClearEntry(connection);
		}

		public static void ClearAllPools()
		{
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
			{
				if (State == ConnectionState.Open)
					Close();

				if (mLogger != null)
					((IDisposable)mLogger).Dispose();
			}
		}

		#endregion
	}
}

