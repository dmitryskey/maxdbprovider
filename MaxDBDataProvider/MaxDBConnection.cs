//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnection.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Runtime.CompilerServices;
    using System.Text;
    using MaxDB.Data.MaxDBProtocol;
    using MaxDB.Data.Utilities;

    internal struct ConnectArgs
    {
        public string username;
        public string password;
        public string dbname;
        public string host;
        public int port;
    }

    /// <summary>
    /// MaxDB SQL Mode.
    /// </summary>
    /// <remarks>
    /// Copy of vsp001::tsp1_sqlmode.
    /// </remarks>
    public enum SqlMode
    {
        /// <summary>Unknown mode</summary>
        Nil = 0,

        /// <summary>Session mode</summary>
        SessionSqlMode = 1,

        /// <summary>Internal mode</summary>
        Internal = 2,

        /// <summary>ANSI mode</summary>
        Ansi = 3,

        /// <summary>DB2 mode</summary>
        Db2 = 4,

        /// <summary>Oracle mode</summary>
        Oracle = 5,

        /// <summary>SAP R/3 mode</summary>
        SapR3 = 6,
    }

    /// <summary>
    /// Represents an open connection to a MaxDB database. This class cannot be inherited.
    /// </summary>
    public sealed class MaxDBConnection : DbConnection
    {
        internal MaxDBConnectionStringBuilder mConnStrBuilder;
        private string strConnection;

        internal ConnectArgs mConnArgs;
        internal MaxDBComm mComm;

        internal MaxDBLogger mLogger;

        /// <summary>
        /// Default constructor
        /// </summary>
        public MaxDBConnection() => this.mLogger = new MaxDBLogger();

        /// <summary>
        /// A constructor that takes a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        public MaxDBConnection(string connectionString)
            : this()
        {
            this.strConnection = connectionString;
            this.mConnStrBuilder = new MaxDBConnectionStringBuilder(connectionString);
            this.SetConnectionParameters();
        }

        private void SetConnectionParameters()
        {
            if (this.mConnStrBuilder.DataSource != null)
            {
                string[] hostPort = this.mConnStrBuilder.DataSource.Split(':');
                this.mConnArgs.host = hostPort[0];
                this.mConnArgs.port = 0;
                try
                {
                    if (hostPort.Length > 1)
                    {
                        this.mConnArgs.port = int.Parse(hostPort[1], CultureInfo.InvariantCulture);
                    }
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

            this.mConnArgs.dbname = this.mConnStrBuilder.InitialCatalog;
            this.mConnArgs.username = this.mConnStrBuilder.UserId;
            this.mConnArgs.password = this.mConnStrBuilder.Password;
            if (this.mConnStrBuilder.CodePage > 0)
            {
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                this.UserAsciiEncoding = Encoding.GetEncoding(this.mConnStrBuilder.CodePage);
            }
        }

        /// <summary>
        /// MaxDB database encoding (<see cref="Encoding.ASCII"/> or <see cref="Encoding.Unicode"/>).
        /// </summary>
        public Encoding DatabaseEncoding => this.mComm.Encoding;

        /// <summary>
        /// MaxDB database SQL mode (<see cref="SqlMode"/>).
        /// </summary>
        public SqlMode SqlMode
        {
            get => this.mConnStrBuilder.Mode;

            set
            {
                this.mConnStrBuilder.Mode = value;
                if (this.mComm != null)
                {
                    this.mComm.ConnStrBuilder.Mode = value;
                }
            }
        }

        /// <summary>
        /// MaxDB database AutoCommit mode.
        /// </summary>
        public bool AutoCommit
        {
            get
            {
                if (this.mComm == null || this.State != ConnectionState.Open)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));
                }

                return this.mComm.AutoCommit;
            }

            set
            {
                if (this.mComm == null || this.State != ConnectionState.Open)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));
                }

                // >>> SQL TRACE
                if (this.mLogger.TraceSQL)
                {
                    this.mLogger.SqlTrace(DateTime.Now, "::SET AUTOCOMMIT " + (value ? "ON" : "OFF"));
                }

                // <<< SQL TRACE

                this.mComm.AutoCommit = value;
            }
        }

        /// <summary>
        /// MaxDB server version (e.g. 7.6.34)
        /// </summary>
        public override string ServerVersion
        {
            get
            {
                int version = this.mComm.KernelVersion;
                int correction_level = version % 100;
                int minor_release = ((version - correction_level) % 10000) / 100;
                int mayor_release = (version - (minor_release * 100) - correction_level) / 10000;
                return mayor_release.ToString(CultureInfo.InvariantCulture) + "." +
                    minor_release.ToString(CultureInfo.InvariantCulture) + "." +
                    correction_level.ToString("d2", CultureInfo.InvariantCulture);
            }
        }

        internal int KernelVersion => this.mComm.KernelVersion;

        private Encoding userAsciiEncoding;

        /// <summary>
        /// Gets or sets the user encoding.
        /// </summary>
        public Encoding UserAsciiEncoding
        {
            get => this.userAsciiEncoding ?? Encoding.ASCII;

            set => this.userAsciiEncoding = value;
        }

        internal static int MapIsolationLevel(IsolationLevel level)
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
            if (this.mComm.IsolationLevel != level)
            {
                this.AssertOpen();
                string cmd = "SET ISOLATION LEVEL " + MapIsolationLevel(level).ToString(CultureInfo.InvariantCulture);
                var requestPacket = this.mComm.GetRequestPacket();
                byte oldMode = requestPacket.SwitchSqlMode((byte)SqlMode.Internal);
                requestPacket.InitDbsCommand(this.mComm.AutoCommit, cmd);
                try
                {
                    this.mComm.Execute(this.mConnArgs, requestPacket, this, GCMode.ALLOWED);
                }
                catch (MaxDBTimeoutException)
                {
                    requestPacket.SwitchSqlMode(oldMode);
                }

                this.mComm.IsolationLevel = level;
            }
        }

        internal void AssertOpen()
        {
            if (this.State == ConnectionState.Closed)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
            }
        }

        #region "Methods to support native protocol"


        #endregion

        #region IDbConnection Members

        /// <summary>
        /// Initiate a local transaction with the specified isolation level.
        /// </summary>
        /// <param name="isolationLevel">Isolation level <see cref="IsolationLevel"/> under which the transaction should run.</param>
        /// <returns>A <see cref="DbTransaction"/> object.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => this.BeginTransaction(isolationLevel);

        /// <summary>
        /// Initiate a local transaction with the specified isolation level.
        /// </summary>
        /// <param name="isolationLevel">Isolation level <see cref="IsolationLevel"/> under which the transaction should run.</param>
        /// <returns>A <see cref="MaxDBTransaction"/> object.</returns>
        public new MaxDBTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            this.SetIsolationLevel(isolationLevel);
            return new MaxDBTransaction(this);
        }

        /// <summary>
        /// Initiate a local transaction
        /// </summary>
        /// <returns>A <see cref="MaxDBTransaction"/> object.</returns>
        public new MaxDBTransaction BeginTransaction() => new MaxDBTransaction(this);

        /// <summary>
        /// Change the database setting on the back-end. Note that it is a method
        /// and not a property because the operation requires an expensive round trip.
        /// </summary>
        /// <param name="databaseName">Database name</param>
        public override void ChangeDatabase(string databaseName) => this.mConnArgs.dbname = databaseName;

        /// <summary>
        /// Close the database connection and set the ConnectionState
        /// property. If the underlying connection to the server is
        /// being pooled, Close() will release it back to the pool.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void Close()
        {
            if (this.State == ConnectionState.Open)
            {
                // >>> SQL TRACE
                if (this.mLogger.TraceSQL)
                {
                    this.mLogger.SqlTrace(DateTime.Now, "::CLOSE CONNECTION");
                }

                // <<< SQL TRACE

                this.mLogger.Flush();

                if (this.mComm != null)
                {
                    if (this.mConnStrBuilder.Pooling)
                    {
                        MaxDBConnectionPool.ReleaseEntry(this);
                    }
                    else
                    {
                        this.mComm.Close(true, true);
                        this.mComm.Dispose();
                    }

                    this.mComm = null;
                }
            }
        }

        /// <summary>
        /// Gets or sets the string used to connect to a MaxDB Server database.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The <b>ConnectionString</b> always returns exactly what the user set.
        /// Security-sensitive information may be removed.
        /// </para>
        /// <para>
        /// You can use this property to connect to a database.
        /// The following example illustrates a typical connection string.
        /// <c>"Server=MyServer;Database=MyDB;User ID=MyLogin;Password=MyPassword;"</c>
        /// </para>
        /// <para>To connect to a local machine, specify "localhost" or "127.0.0.1" for the server.
        /// If you do not specify a server, localhost is assumed.
        /// </para>
        /// </remarks>
        public override string ConnectionString
        {
            get => this.strConnection;

            set
            {
                this.strConnection = value;
                this.mConnStrBuilder = new MaxDBConnectionStringBuilder(value);
                this.SetConnectionParameters();
            }
        }

        /// <summary>
        /// Returns the connection time-out value set in the connection
        /// string. Zero indicates an indefinite time-out period.
        /// </summary>
        public override int ConnectionTimeout => this.mConnStrBuilder.Timeout;

        /// <summary>
        /// Creates and returns a <see cref="DbCommand"/> object associated with the <see cref="MaxDBConnection"/>.
        /// </summary>
        /// <returns>A <see cref="DbCommand"/> object.</returns>
        protected override DbCommand CreateDbCommand() => this.CreateCommand();

        /// <summary>
        /// Creates and returns a <see cref="MaxDBCommand"/> object associated with the <see cref="MaxDBConnection"/>.
        /// </summary>
        /// <returns>A <see cref="MaxDBCommand"/> object.</returns>
        public new MaxDBCommand CreateCommand() => new MaxDBCommand(string.Empty, this);

        internal bool Ping(MaxDBComm communication)
        {
            var oldMode = communication.ConnStrBuilder.Mode;
            var oldComm = this.mComm;
            this.mComm = communication;
            if (this.mComm.ConnStrBuilder.Mode != SqlMode.Internal)
            {
                this.mComm.ConnStrBuilder.Mode = SqlMode.Internal;
            }

            try
            {
                using (var cmd = new MaxDBCommand("PING", this))
                {
                    cmd.ExecuteNonQuery();
                }

                return true;
            }
            catch
            {
                return false;
            }
            finally
            {
                this.mComm = oldComm;
                if (oldMode != SqlMode.Internal)
                {
                    communication.ConnStrBuilder.Mode = oldMode;
                }
            }
        }

        private DataTable ExecuteInternalQuery(string sql, string table, MaxDBParameterCollection parameters)
        {
            var dt = new DataTable(table);
            var oldMode = this.mComm.ConnStrBuilder.Mode;
            if (oldMode != SqlMode.Internal)
            {
                this.mComm.ConnStrBuilder.Mode = SqlMode.Internal;
            }

            try
            {
                using (var cmd = new MaxDBCommand(sql, this))
                {
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                    }

                    using (var da = new MaxDBDataAdapter { SelectCommand = cmd, })
                    {
                        da.Fill(dt);
                    }
                }
            }
            finally
            {
                if (this.mComm.ConnStrBuilder.Mode != oldMode)
                {
                    this.mComm.ConnStrBuilder.Mode = oldMode;
                }
            }

            return dt;
        }

        private DataTable ExecuteInternalQuery(string sql, string table)
        {
            return this.ExecuteInternalQuery(sql, table, null);
        }

        /// <summary>
        /// Returns schema information for the data source of this <see cref="MaxDBConnection"/>.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that contains schema information. </returns>
        public override DataTable GetSchema()
        {
            return this.GetSchema("MetaDataCollections");
        }

        /// <summary>
        /// Returns schema information for the data source of this <see cref="MaxDBConnection"/>
        /// using the specified string for the schema name.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that contains schema information.</returns>
        public override DataTable GetSchema(string collectionName)
        {
            return this.GetSchema(collectionName, null);
        }

        /// <summary>
        /// Returns schema information for the data source of this <see cref="MaxDBConnection"/>
        /// using the specified string for the schema name and
        /// the specified string array for the restriction values.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that contains schema information.</returns>
        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            var dt = new DataTable("SchemaTable")
            {
                Locale = CultureInfo.InvariantCulture,
            };

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
                if (this.DatabaseEncoding != Encoding.Unicode)
                {
                    var enc = this.UserAsciiEncoding;
                    byte[] buf = new byte[1];
                    var sb = new StringBuilder();
                    for (byte b = 0x80; b < 0xFF; b++)
                    {
                        buf[0] = b;
                        if (char.IsLetter(enc.GetString(buf), 0))
                        {
                            sb.Append("\\x").Append(b.ToString("x2", CultureInfo.InvariantCulture));
                        }
                    }

                    langSpecific = sb.ToString();
                }

                string identifierPattern = "(([A-Za-z" + langSpecific + "#@\\$][\\w" + langSpecific + "#@\\$_]*)|(\".+\"))";

                dt.Rows.Add("\\.", "MaxDB", this.ServerVersion, this.ServerVersion, GroupByBehavior.Unrelated,
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

                bool isUnicode = this.DatabaseEncoding == Encoding.Unicode;

                dt.Rows.Add(new object[]
                {
                    "CHAR", MaxDBType.CharA, 8000, "CHAR({0})", "length", typeof(string).ToString(),
                    false, !isUnicode, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                dt.Rows.Add(new object[]
                {
                    "CHAR ASCII", MaxDBType.CharA, 8000, "CHAR({0}) ASCII", "length", typeof(string).ToString(),
                    false, false, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                if (isUnicode)
                    dt.Rows.Add(new object[]
                    {
                        "CHAR UNICODE", MaxDBType.Unicode, 4000, "CHAR({0}) UNICODE", "length", typeof(string).ToString(),
                        false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                        true, "'", "'",
                    });
                dt.Rows.Add(new object[]
                {
                    "CHAR BYTE", MaxDBType.CharB, 8000, "CHAR({0}) BYTE", "length", typeof(byte[]).ToString(),
                    false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "X'", "X'",
                });
                dt.Rows.Add(new object[]
                {
                    "VARCHAR", MaxDBType.VarCharA, 8000, "VARCHAR({0})", "length", typeof(string).ToString(),
                    false, !isUnicode, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                dt.Rows.Add(new object[]
                {
                    "VARCHAR ASCII", MaxDBType.VarCharA, 8000, "VARCHAR({0}) ASCII", "length", typeof(string).ToString(),
                    false, false, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                if (isUnicode)
                    dt.Rows.Add(new object[]
                    {
                        "VARCHAR UNICODE", MaxDBType.VarCharUni, 4000, "VARCHAR({0}) UNICODE", "length", typeof(string).ToString(),
                        false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                        true, string.Empty, string.Empty,
                    });
                dt.Rows.Add(new object[]
                {
                    "VARCHAR BYTE", MaxDBType.VarCharB, 8000, "VARCHAR({0}) BYTE", "length", typeof(byte[]).ToString(),
                    false, true, true, false, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "X'", "X'",
                });
                dt.Rows.Add(new object[]
                {
                    "LONG", MaxDBType.LongA, 2147483648, "LONG", DBNull.Value, typeof(string).ToString(),
                    false, !isUnicode, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                dt.Rows.Add(new object[]
                {
                    "LONG VARCHAR", MaxDBType.LongA, 2147483648, "LONG VARCHAR", DBNull.Value, typeof(string).ToString(),
                    false, false, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                dt.Rows.Add(new object[]
                {
                    "LONG ASCII", MaxDBType.LongA, 2147483648, "LONG ASCII", DBNull.Value, typeof(string).ToString(),
                    false, false, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "'", "'",
                });
                if (isUnicode)
                    dt.Rows.Add(new object[]
                    {
                        "LONG UNICODE", MaxDBType.LongUni, 1073741824, "LONG UNICODE", DBNull.Value, typeof(string).ToString(),
                        false, true, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                        true, "'", "'",
                    });
                dt.Rows.Add(new object[]
                {
                    "LONG BYTE", MaxDBType.LongB, 2147483648, "LONG BYTE", DBNull.Value, typeof(byte[]).ToString(),
                    false, true, true, false, false, true, false, false, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, "X'", "X'",
                });
                dt.Rows.Add(new object[]
                {
                    "BOOLEAN", MaxDBType.Boolean, 1, "BOOLEAN", DBNull.Value, typeof(bool).ToString(),
                    false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    true, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "FIXED", MaxDBType.Fixed, 38, "FIXED({0},{1})", "precision,scale", typeof(decimal).ToString(),
                    true, true, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "NUMERIC", MaxDBType.Number, 38, "NUMERIC({0},{1})", "precision,scale", typeof(decimal).ToString(),
                    true, false, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "DECIMAL", MaxDBType.VFloat, 38, "DECIMAL({0},{1})", "precision,scale", typeof(decimal).ToString(),
                    true, false, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "FLOAT", MaxDBType.Float, 38, "FLOAT({0})", "precision", typeof(decimal).ToString(),
                    false, true, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "REAL", MaxDBType.Float, 38, "REAL({0})", "precision", typeof(decimal).ToString(),
                    false, true, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "DOUBLE PRECISION", MaxDBType.VFloat, 38, "DOUBLE PRECISION", "precision", typeof(decimal).ToString(),
                    false, true, false, true, false, false, true, true, false, false, 38, 0, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "INTEGER", MaxDBType.Integer, 10, "INTEGER", DBNull.Value, typeof(int).ToString(),
                    true, true, false, true, true, false, true, true, false, false, DBNull.Value, DBNull.Value, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "SMALLINT", MaxDBType.SmallInt, 5, "SMALLINT", DBNull.Value, typeof(short).ToString(),
                    true, true, false, true, true, false, true, true, false, false, DBNull.Value, DBNull.Value, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });

                string DatePattern;
                string TimePattern;
                string TimestampPattern;
                string DateTimeFormat = "INTERNAL";

                DataTable formatTable = this.ExecuteInternalQuery("SELECT VALUE FROM DBA.DBPARAMETERS WHERE DESCRIPTION = 'DATE_TIME_FORMAT'", "DateTimeFormat");
                if (formatTable.Rows.Count > 0)
                {
                    DateTimeFormat = formatTable.Rows[0].ToString();
                }

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
                }

                dt.Rows.Add(new object[]
                {
                    "DATE", MaxDBType.Date, DatePattern.Length, "DATE", DBNull.Value, typeof(DateTime).ToString(),
                    false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "TIME", MaxDBType.Time, TimePattern.Length, "TIME", DBNull.Value, typeof(DateTime).ToString(),
                    false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
                dt.Rows.Add(new object[]
                {
                    "TIMESTAMP", MaxDBType.Timestamp, TimestampPattern.Length, "TIMESTAMP", DBNull.Value, typeof(DateTime).ToString(),
                    false, true, false, true, false, false, true, true, true, DBNull.Value, DBNull.Value, DBNull.Value, false,
                    DBNull.Value, DBNull.Value, DBNull.Value,
                });
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

                var keywords = new List<string>
                {
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
                    "VARGRAPHIC", "VARIANCE", "WEEK", "WEEKOFYEAR", "WHEN", "WHERE", "WITH", "YEAR", "ZONED",
                };

                keywords.ForEach(keyword => dt.Rows.Add(keyword));
            }

            if (string.Compare(collectionName, "Catalogs", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.CATALOGS", "Catalogs");
            }

            if (string.Compare(collectionName, "Schemas", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.SCHEMAS ORDER BY TABLE_SCHEM", "Schemas");
            }

            if (string.Compare(collectionName, "TableTypes", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.TABLETYPES ORDER BY TABLE_TYPE", "TableTypes");
            }

            if (string.Compare(collectionName, "ForeignKeys", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.CROSSREFERENCES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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
                dt = this.ExecuteInternalQuery(sql, "ForeignKeys", parameters);
            }

            if (string.Compare(collectionName, "PrimaryKeys", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.PRIMARYKEYS WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "PrimaryKeys", parameters);
            }

            if (string.Compare(collectionName, "Procedures", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.PROCEDURES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "Procedures", parameters);
            }

            if (string.Compare(collectionName, "SuperTables", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.SUPERTABLES", "SuperTables");
            }

            if (string.Compare(collectionName, "SuperTypes", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.SUPERTYPES", "SuperTypes");
            }

            if (string.Compare(collectionName, "TablePrivileges", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.TABLEPRIVILEGES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "TablePrivileges", parameters);
            }

            if (string.Compare(collectionName, "VersionColumns", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.VERSIONCOLUMNS", "VersionColumns");
            }

            if (string.Compare(collectionName, "BestRowIdentifier", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.BESTROWIDENTIFIER WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "BestRowIdentifier", parameters);
            }

            if (string.Compare(collectionName, "Indexes", true, CultureInfo.InvariantCulture) == 0)
            {
                bool unique = restrictionValues != null && restrictionValues.Length > 2 && restrictionValues[2] != null &&
                        (string.Compare(restrictionValues[2].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
                         string.Compare(restrictionValues[2].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
                         restrictionValues[2].Trim() == "1");

                bool approximate = restrictionValues != null && restrictionValues.Length > 3 && restrictionValues[3] != null &&
                        (string.Compare(restrictionValues[3].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
                         string.Compare(restrictionValues[3].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
                         restrictionValues[3].Trim() == "1");

                string sql = "SELECT * FROM SYSJDBC." + (approximate ? "APPROXINDEXINFO" : "INDEXINFO");

                var parameters = new MaxDBParameterCollection();

                if (restrictionValues != null && restrictionValues.Length > 2)
                {
                    sql += " WHERE NON_UNIQUE = :NON_UNIQUE ";
                    parameters.Add(":NON_UNIQUE", !unique);
                }
                else
                {
                    sql += " WHERE 1=1 ";
                }

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

                dt = this.ExecuteInternalQuery(sql, "Indexes", parameters);
            }

            if (string.Compare(collectionName, "UserDefinedTypes", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.UDTS", "UserDefinedTypes");
            }

            if (string.Compare(collectionName, "Attributes", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.ATTRIBUTES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "Attributes", parameters);
            }

            if (string.Compare(collectionName, "ColumnPrivileges", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.COLUMNPRIVILEGES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "ColumnPrivileges", parameters);
            }

            if (string.Compare(collectionName, "Columns", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.COLUMNS WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "Columns", parameters);
            }

            if (string.Compare(collectionName, "ProcedureColumns", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.PROCEDURECOLUMNS WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "ProcedureColumns", parameters);
            }

            if (string.Compare(collectionName, "Tables", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.TABLES WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "Tables", parameters);
            }

            if (string.Compare(collectionName, "Constraints", true, CultureInfo.InvariantCulture) == 0)
            {
                string sql = "SELECT * FROM SYSJDBC.CONSTRAINTS WHERE 1=1 ";

                var parameters = new MaxDBParameterCollection();
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

                dt = this.ExecuteInternalQuery(sql, "Constraints", parameters);
            }

            if (string.Compare(collectionName, "SystemInfo", true, CultureInfo.InvariantCulture) == 0)
            {
                dt = this.ExecuteInternalQuery("SELECT * FROM SYSJDBC.SYSTEMINFO", "SystemInfo");
            }

            return dt;
        }

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        public override string Database => this.mConnArgs.dbname;

        /// <summary>
        /// Data source address or IP.
        /// </summary>
        public override string DataSource => this.mConnArgs.host;

        /// <summary>
        /// Opens a database connection with the property settings specified by the ConnectionString.
        /// </summary>
        /// <remarks>
        /// <para>The <see cref="MaxDBConnection"/> draws an open connection from the connection pool if one is available.
        /// Otherwise, it establishes a new connection to an instance of MaxDB.</para>
        /// </remarks>
        public override void Open()
        {
            if (this.mConnStrBuilder.Pooling)
            {
                this.mComm = MaxDBConnectionPool.GetPoolEntry(this, this.mLogger);
            }
            else
            {
                this.mComm = new MaxDBComm(this.mLogger)
                {
                    ConnStrBuilder = this.mConnStrBuilder,
                };

                this.mComm.Open(this.mConnArgs);
            }
        }

        /// <summary>
        /// Gets the current <see cref="ConnectionState"/> of the connection.
        /// </summary>
        public override ConnectionState State => this.mComm != null && this.mComm.SessionID >= 0 ? ConnectionState.Open : ConnectionState.Closed;

        #endregion

        /// <summary>
        /// Empties the connection pool associated with the specified connection.
        /// </summary>
        /// <param name="connection">The <see cref="MaxDBConnection"/> to be cleared from the pool.</param>
        public static void ClearPool(MaxDBConnection connection) => MaxDBConnectionPool.ClearEntry(connection);

        /// <summary>
        /// Empties the connection pool.
        /// </summary>
        public static void ClearAllPools()
        {
        }

        #region IDisposable Members

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="disposing">The disposing flag.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                if (this.State == ConnectionState.Open)
                {
                    this.Close();
                }
            }
        }

        #endregion
    }
}