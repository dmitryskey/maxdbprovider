//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnectionStringBuilder.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
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
    using System.Collections;
    using System.Data.Common;
    using System.Globalization;
    using System.Text;
    using MaxDB.Data.MaxDBProtocol;

    /// <summary>
    /// Provides a simple way to create and manage the contents of connection strings used by the <see cref="MaxDBConnection"/> class.
    /// This class cannot be inherited.
    /// </summary>
    public sealed class MaxDBConnectionStringBuilder : DbConnectionStringBuilder, IEnumerable
    {
        private readonly Hashtable mKeyValuePairs = new Hashtable();

        /// <summary>
        /// Default constructor.
        /// </summary>
        public MaxDBConnectionStringBuilder()
        {
        }

        /// <summary>
        /// A constructor that takes a connection string.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        public MaxDBConnectionStringBuilder(string connectionString) => this.ConnectionString = connectionString;

        /// <summary>
        /// Gets or sets a value that indicates whether the <see cref="ConnectionString"/> property is visible in Visual Studio designers. 
        /// </summary>
        public new bool BrowsableConnectionString { get; set; } = true;

        /// <summary>
        /// Append a key and value to an existing <see cref="StringBuilder"/> object. 
        /// </summary>
        /// <param name="builder">The <see cref="StringBuilder"/> to which to add the key/value pair.</param>
        /// <param name="keyword">Key value.</param>
        /// <param name="value">The value for the supplied key.</param>
        public static new void AppendKeyValuePair(StringBuilder builder, string keyword, string value)
        {
            if (builder == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "builder"));
            }

            builder.Append(keyword).Append("=").Append(value).Append(";");
        }

        /// <summary>
        /// Indicates whether the specified key exists in this <see cref="MaxDBConnectionStringBuilder"/> instance. 
        /// </summary>
        /// <param name="keyword">Key value.</param>
        /// <returns>true if an entry with the specified key was found and false otherwise.</returns>
        public override bool ShouldSerialize(string keyword) => this.mKeyValuePairs.ContainsKey(keyword);

        /// <summary>
        /// Return connection string.
        /// </summary>
        /// <returns>Connection string.</returns>
        public override string ToString() => this.ConnectionString;

        /// <summary>
        /// Try to retrieve value for the specified key.
        /// </summary>
        /// <param name="keyword">The key of the item.</param>
        /// <param name="value">The corresponding value.</param>
        /// <returns>true if an entry with the specified key was found and false otherwise.</returns>
        public override bool TryGetValue(string keyword, out object value)
        {
            if (this.mKeyValuePairs.ContainsKey(keyword))
            {
                value = this.mKeyValuePairs[keyword];
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        private void ParseIntParameter(string key, string parameter)
        {
            this.mKeyValuePairs[key] = 0;
            try
            {
                this.mKeyValuePairs[key] = int.Parse(parameter.Split('=')[1], CultureInfo.InvariantCulture);
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

        private void ParseBoolParameter(string key, string parameter)
        {
            this.mKeyValuePairs[key] = true;
            try
            {
                this.mKeyValuePairs[key] = bool.Parse(parameter.Split('=')[1]);
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

        /// <summary>
        /// Gets or sets the string used to connect to a MaxDB Server database.
        /// </summary>
        /// <remarks>
        /// <para>
        /// You can use this property to connect to a database.
        /// The following example illustrates a typical connection string.
        /// <c>"Server=MyServer;Database=MyDB;User ID=MyLogin;Password=MyPassword;"</c>
        /// </para>
        /// </remarks>
        public new string ConnectionString
        {
            get
            {
                var builder = new StringBuilder();
                var keys = new SortedList(this.mKeyValuePairs);

                foreach (string key in keys.Keys)
                {
                    builder.Append(key).Append("=").Append(this.mKeyValuePairs[key]).Append(";");
                }

                return builder.ToString();
            }
            set
            {
                bool isModeSet = false;
                if (value == null)
                {
                    return;
                }

                string[] paramArr = value.Split(';');
                foreach (string param in paramArr)
                {
                    if (param.Split('=').Length > 1)
                    {
                        switch (param.Split('=')[0].Trim().ToUpper(CultureInfo.InvariantCulture))
                        {
                            case ConnectionStringParams.DATA_SOURCE:
                            case "SERVER":
                            case "ADDRESS":
                            case "ADDR":
                            case "NETWORK ADDRESS":
                                this.mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.INITIAL_CATALOG:
                            case "DATABASE":
                                this.mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.USER_ID:
                            case "LOGIN":
                                this.mKeyValuePairs[ConnectionStringParams.USER_ID] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.PASSWORD:
                            case "PWD":
                                this.mKeyValuePairs[ConnectionStringParams.PASSWORD] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.TIMEOUT:
                                this.ParseIntParameter(ConnectionStringParams.TIMEOUT, param);
                                break;
                            case ConnectionStringParams.SPACE_OPTION:
                                if (string.Compare(param.Split('=')[1].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
                                    string.Compare(param.Split('=')[1].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
                                    param.Split('=')[1].Trim() == "1")
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] = true;
                                }

                                break;
                            case ConnectionStringParams.CACHE:
                                this.mKeyValuePairs[ConnectionStringParams.CACHE] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.CACHE_LIMIT:
                                this.ParseIntParameter(ConnectionStringParams.CACHE_LIMIT, param);
                                break;
                            case ConnectionStringParams.CACHE_SIZE:
                                this.ParseIntParameter(ConnectionStringParams.CACHE_SIZE, param);
                                break;
                            case ConnectionStringParams.ENCRYPT:
                                if (string.Compare(param.Split('=')[1].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
                                    string.Compare(param.Split('=')[1].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
                                    param.Split('=')[1].Trim() == "1")
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.ENCRYPT] = true;
                                }
                                break;
                            case ConnectionStringParams.MODE:
                                isModeSet = true;
                                string mode = param.Split('=')[1].Trim();
                                if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Ansi], true, CultureInfo.InvariantCulture) == 0)
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Ansi;
                                    break;
                                }

                                if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Db2], true, CultureInfo.InvariantCulture) == 0)
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Db2;
                                    break;
                                }

                                if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Oracle], true, CultureInfo.InvariantCulture) == 0)
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Oracle;
                                    break;
                                }

                                if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.SapR3], true, CultureInfo.InvariantCulture) == 0)
                                {
                                    this.mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.SapR3;
                                    break;
                                }

                                this.mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Internal;
                                break;
                            case ConnectionStringParams.SSL_CERTIFICATE:
                                this.mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE] = param.Split('=')[1].Trim();
                                break;
                            case ConnectionStringParams.POOLING:
                                this.ParseBoolParameter(ConnectionStringParams.POOLING, param);
                                break;
                            case ConnectionStringParams.CONNECTION_LIFETIME:
                            case "LOAD BALANCE TIMEOUT":
                                this.ParseIntParameter(ConnectionStringParams.CONNECTION_LIFETIME, param);
                                break;
                            case ConnectionStringParams.MIN_POOL_SIZE:
                                this.ParseIntParameter(ConnectionStringParams.MIN_POOL_SIZE, param);
                                break;
                            case ConnectionStringParams.MAX_POOL_SIZE:
                                this.ParseIntParameter(ConnectionStringParams.MAX_POOL_SIZE, param);
                                break;
                            case ConnectionStringParams.CODE_PAGE:
                                this.ParseIntParameter(ConnectionStringParams.CODE_PAGE, param);
                                break;
                        }
                    }
                }

                if (!isModeSet)
                {
                    this.Mode = SqlMode.Internal;
                }
            }
        }

        /// <summary>
        /// Gets or sets database server address or IP.
        /// </summary>
        /// <remarks>
        /// <para>
        /// To connect to a local machine, specify "localhost" or "127.0.0.1" for the server. 
        /// If you do not specify a server, localhost is returned.
        /// </para>
        ///</remarks>
        public string DataSource
        {
            get => this.mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] as string ?? "localhost";
            set => this.mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] = value;
        }

        /// <summary>
        /// Gets or sets database name.
        /// </summary>
        public string InitialCatalog
        {
            get => (string)this.mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG];
            set => this.mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = value;
        }

        /// <summary>
        /// Get or sets database user login.
        /// </summary>
        public string UserId
        {
            get => (string)this.mKeyValuePairs[ConnectionStringParams.USER_ID];
            set => this.mKeyValuePairs[ConnectionStringParams.USER_ID] = value;
        }

        /// <summary>
        /// Get or sets database user password.
        /// </summary>
        public string Password
        {
            get => (string)this.mKeyValuePairs[ConnectionStringParams.PASSWORD];
            set => this.mKeyValuePairs[ConnectionStringParams.PASSWORD] = value;
        }

        /// <summary>
        /// Get or sets connection timeout. If you do not specify a timeout, 0 is returned.
        /// </summary>
        public int Timeout
        {
            get => this.mKeyValuePairs[ConnectionStringParams.TIMEOUT] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.TIMEOUT] : 0;
            set => this.mKeyValuePairs[ConnectionStringParams.TIMEOUT] = value;
        }

        /// <summary>
        /// Gets or sets connection <see cref="SqlMode"/> mode. If you do not specify a mode, internal one is returned.
        /// </summary>
        public SqlMode Mode
        {
            get => this.mKeyValuePairs[ConnectionStringParams.MODE] != null ? (SqlMode)this.mKeyValuePairs[ConnectionStringParams.MODE] : SqlMode.Internal;
            set => this.mKeyValuePairs[ConnectionStringParams.MODE] = value;
        }

        /// <summary>
        /// Gets or sets a value indicating whether character values contain at least 1 blank, or are NULL. 
        /// </summary>
        public bool SpaceOption
        {
            get => this.mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] != null ? (bool)this.mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] : false;
            set => this.mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] = value;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the connection use SSL connection. If you do not specify this flag, false is returned.
        /// </summary>
        public bool Encrypt
        {
            get => this.mKeyValuePairs[ConnectionStringParams.ENCRYPT] != null ? (bool)this.mKeyValuePairs[ConnectionStringParams.ENCRYPT] : false;
            set => this.mKeyValuePairs[ConnectionStringParams.ENCRYPT] = value;
        }

        /// <summary>
        /// Gets or sets SSL certificate name.
        /// </summary>
        public string SslCertificateName
        {
            get => (string)this.mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE];
            set => this.mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE] = value;
        }

        /// <summary>
        /// Gets or sets the property that indicates what kind of SQL statements has to cached. This property is the string of the
        /// form [s][i][u][d][all] where 
        /// <list type="bullet">
        /// <item>s - cache SELECT statements</item>
        /// <item>i - cache INSERT statements</item>
        /// <item>u - cache UPDATE statements</item>
        /// <item>d - cache DELETE statements</item>
        /// <item>all - cache all statements.</item>
        /// </list>
        /// </summary>
        public string Cache
        {
            get => this.mKeyValuePairs[ConnectionStringParams.CACHE] as string ?? "all";
            set => this.mKeyValuePairs[ConnectionStringParams.CACHE] = value;
        }

        /// <summary>
        /// Gets or sets statement cache size
        /// </summary>
        public int CacheSize
        {
            get => this.mKeyValuePairs[ConnectionStringParams.CACHE_SIZE] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.CACHE_SIZE] : 0;
            set => this.mKeyValuePairs[ConnectionStringParams.CACHE_SIZE] = value;
        }

        /// <summary>
        /// Gets or sets statement cache limit
        /// </summary>
        public int CacheLimit
        {
            get => this.mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT] : 0;
            set => this.mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT] = value;
        }

        /// <summary>
        /// Gets or sets a value whether the connection supports pooling.
        /// </summary>
        public bool Pooling
        {
            get => this.mKeyValuePairs[ConnectionStringParams.POOLING] != null ? (bool)this.mKeyValuePairs[ConnectionStringParams.POOLING] : true;
            set => this.mKeyValuePairs[ConnectionStringParams.POOLING] = value;
        }

        /// <summary>
        /// Gets the maximum number of seconds a connection should live. This is checked when a connection is returned to the pool.
        /// Default value is 0.
        /// </summary>
        public int ConnectionLifetime
        {
            get => this.mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME] : 0;
            set => this.mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME] = value;
        }

        /// <summary>
        /// Gets or sets the minimum number of connections to have in the pool. Default value is 0.
        /// </summary>
        public int MinPoolSize
        {
            get => this.mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE] : 0;
            set => this.mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE] = value;
        }

        /// <summary>
        /// Gets or sets the minimum number of connections to have in the pool. Default value is 100.
        /// </summary>
        public int MaxPoolSize
        {
            get => this.mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE] : 100;
            set => this.mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE] = value;
        }

        /// <summary>
        /// Gets or sets the current user code page (1252 by default).
        /// </summary>
        public int CodePage
        {
            get => this.mKeyValuePairs[ConnectionStringParams.CODE_PAGE] != null ? (int)this.mKeyValuePairs[ConnectionStringParams.CODE_PAGE] : 1252;
            set => this.mKeyValuePairs[ConnectionStringParams.CODE_PAGE] = value;
        }

        #region IDictionary Members

        /// <summary>
        /// Add key/value pair.
        /// </summary>
        /// <param name="key">The key of the item.</param>
        /// <param name="value">The value for the specified key.</param>
        public new void Add(string key, object value) => this.mKeyValuePairs[key] = value;

        /// <summary>
        /// Remove all key/value pairs. 
        /// </summary>
        public override void Clear() => this.mKeyValuePairs.Clear();

        /// <summary>
        /// Check whether the key can be found.
        /// </summary>
        /// <param name="key">Key value.</param>
        /// <returns>true if an entry with the specified key was found and false otherwise.</returns>
        public bool Contains(object key) => this.mKeyValuePairs.ContainsKey(key);

        /// <summary>
        /// Returns an enumerator to support iterating through the collection. 
        /// </summary>
        /// <returns>An enumerator object.</returns>
        public IDictionaryEnumerator GetEnumerator() => this.mKeyValuePairs.GetEnumerator();

        /// <summary>
        /// Gets a value indicating whether the collection is fixed-sized.
        /// </summary>
        public override bool IsFixedSize => this.mKeyValuePairs.IsFixedSize;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public new bool IsReadOnly => this.mKeyValuePairs.IsReadOnly;

        /// <summary>
        /// The collection of keys. 
        /// </summary>
        public override ICollection Keys => this.mKeyValuePairs.Keys;

        /// <summary>
        /// Remove the item with specified key.
        /// </summary>
        /// <param name="keyword">The key of the item</param>
        /// <returns>true if an entry with the specified key was found and false otherwise.</returns>
        public override bool Remove(string keyword)
        {
            if (this.Contains(keyword))
            {
                this.mKeyValuePairs.Remove(keyword);
                return true;
            }
            else
                return false;
        }

        /// <summary>
        /// The collection of values.
        /// </summary>
        public override ICollection Values => this.mKeyValuePairs.Values;

        /// <summary>
        /// Gets or sets the value associated with the specified key. 
        /// </summary>
        /// <param name="keyword">The key of the item.</param>
        /// <returns>The value associated with the specified key.</returns>
        public override object this[string keyword]
        {
            get => this.mKeyValuePairs[keyword];
            set => this.mKeyValuePairs[keyword] = value;
        }

        #endregion

        #region ICollection Members

        /// <summary>
        /// Copy values to the one-dimensional array starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">A target array.</param>
        /// <param name="index">The index in the array at which to begin copying.</param>
        public void CopyTo(Array array, int index) => this.mKeyValuePairs.CopyTo(array, index);

        /// <summary>
        /// Copy values to the one-dimensional string array starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">A target array.</param>
        /// <param name="index">The index in the array at which to begin copying.</param>
        public void CopyTo(string[] array, int index) => this.mKeyValuePairs.CopyTo(array, index);

        /// <summary>
        /// Gets the number of items in the collection.
        /// </summary>
        public override int Count => this.mKeyValuePairs.Count;

        /// <summary>
        /// Gets a value indicating whether collection is synchronized.
        /// </summary>
        public bool IsSynchronized => this.mKeyValuePairs.IsSynchronized;

        /// <summary>
        /// Gets an object that can be used to synchronize access to the collection. 
        /// </summary>
        public object SyncRoot => this.mKeyValuePairs.SyncRoot;

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator() => this.mKeyValuePairs.GetEnumerator();

        #endregion
    }
}
