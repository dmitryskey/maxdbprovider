//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using System.Text;
using System.ComponentModel;
using System.Reflection;
using System.Collections;
using MaxDB.Data.MaxDBProtocol;
using System.Data.Common;
using System.Globalization;

namespace MaxDB.Data
{
	public sealed class MaxDBConnectionStringBuilder :
#if NET20
		DbConnectionStringBuilder
#else
        IDictionary, ICollection
#endif
, IEnumerable
	{
		private Hashtable mKeyValuePairs = new Hashtable();
		private bool bBrowsable = true;

		public MaxDBConnectionStringBuilder()
		{
		}

		public MaxDBConnectionStringBuilder(string connectionString)
		{
			ConnectionString = connectionString;
		}

#if NET20
		public new bool BrowsableConnectionString
#else
        public bool BrowsableConnectionString
#endif // NET20
		{
			get
			{
				return bBrowsable;
			}
			set
			{
				bBrowsable = value;
			}
		}

#if NET20
		public static new void AppendKeyValuePair(StringBuilder builder, string keyword, string value)
#else
        public static void AppendKeyValuePair(StringBuilder builder, string keyword, string value)
#endif // NET20
		{
			if (builder == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "builder"));

			builder.Append(keyword).Append("=").Append(value).Append(";");
		}

#if NET20
		public override bool ShouldSerialize(string keyword)
#else
        public bool ShouldSerialize(string keyword)
#endif
		{
			return mKeyValuePairs.ContainsKey(keyword);
		}

		public override string ToString()
		{
			return ConnectionString;
		}

#if NET20
		public override bool TryGetValue(string keyword, out object value)
#else
        public bool TryGetValue(string keyword, out object value)
#endif
		{
			if (mKeyValuePairs.ContainsKey(keyword))
			{
				value = mKeyValuePairs[keyword];
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
			mKeyValuePairs[key] = 0;
			try
			{
				mKeyValuePairs[key] = int.Parse(parameter.Split('=')[1], CultureInfo.InvariantCulture);
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
			mKeyValuePairs[key] = true;
			try
			{
				mKeyValuePairs[key] = bool.Parse(parameter.Split('=')[1]);
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

#if NET20
		public new string ConnectionString
#else
        public string ConnectionString
#endif
		{
			get
			{
				StringBuilder builder = new StringBuilder();
				SortedList keys = new SortedList(mKeyValuePairs);

				foreach (string key in keys.Keys)
					builder.Append(key).Append("=").Append(mKeyValuePairs[key]).Append(";");

				return builder.ToString();
			}
			set
			{
				bool isModeSet = false;
				if (value == null)
					return;
				string[] paramArr = value.Split(';');
				foreach (string param in paramArr)
				{
					if (param.Split('=').Length > 1)
						switch (param.Split('=')[0].Trim().ToUpper(CultureInfo.InvariantCulture))
						{
							case ConnectionStringParams.DATA_SOURCE:
							case "SERVER":
							case "ADDRESS":
							case "ADDR":
							case "NETWORK ADDRESS":
								mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.INITIAL_CATALOG:
							case "DATABASE":
								mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.USER_ID:
							case "LOGIN":
								mKeyValuePairs[ConnectionStringParams.USER_ID] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.PASSWORD:
							case "PWD":
								mKeyValuePairs[ConnectionStringParams.PASSWORD] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.TIMEOUT:
								ParseIntParameter(ConnectionStringParams.TIMEOUT, param);
								break;
							case ConnectionStringParams.SPACE_OPTION:
								if (string.Compare(param.Split('=')[1].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
									string.Compare(param.Split('=')[1].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
									param.Split('=')[1].Trim() == "1")
									mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] = true;
								break;
							case ConnectionStringParams.CACHE:
								mKeyValuePairs[ConnectionStringParams.CACHE] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.CACHE_LIMIT:
								ParseIntParameter(ConnectionStringParams.CACHE_LIMIT, param);
								break;
							case ConnectionStringParams.CACHE_SIZE:
								ParseIntParameter(ConnectionStringParams.CACHE_SIZE, param);
								break;
							case ConnectionStringParams.ENCRYPT:
								if (string.Compare(param.Split('=')[1].Trim(), "TRUE", true, CultureInfo.InvariantCulture) == 0 ||
									string.Compare(param.Split('=')[1].Trim(), "YES", true, CultureInfo.InvariantCulture) == 0 ||
									param.Split('=')[1].Trim() == "1")
									mKeyValuePairs[ConnectionStringParams.ENCRYPT] = true;
								break;
							case ConnectionStringParams.MODE:
								isModeSet = true;
								string mode = param.Split('=')[1].Trim();
								if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Ansi], true, CultureInfo.InvariantCulture) == 0)
								{
									mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Ansi;
									break;
								}
								if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Db2], true, CultureInfo.InvariantCulture) == 0)
								{
									mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Db2;
									break;
								}
								if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.Oracle], true, CultureInfo.InvariantCulture) == 0)
								{
									mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Oracle;
									break;
								}
								if (string.Compare(mode, SqlModeName.Value[(byte)SqlMode.SapR3], true, CultureInfo.InvariantCulture) == 0)
								{
									mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.SapR3;
									break;
								}
								mKeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Internal;
								break;
							case ConnectionStringParams.SSL_CERTIFICATE:
								mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE] = param.Split('=')[1].Trim();
								break;
							case ConnectionStringParams.POOLING:
								ParseBoolParameter(ConnectionStringParams.POOLING, param);
								break;
							case ConnectionStringParams.CONNECTION_LIFETIME:
							case "LOAD BALANCE TIMEOUT":
								ParseIntParameter(ConnectionStringParams.CONNECTION_LIFETIME, param);
								break;
							case ConnectionStringParams.MIN_POOL_SIZE:
								ParseIntParameter(ConnectionStringParams.MIN_POOL_SIZE, param);
								break;
							case ConnectionStringParams.MAX_POOL_SIZE:
								ParseIntParameter(ConnectionStringParams.MAX_POOL_SIZE, param);
								break;
						}
				}

				if (!isModeSet)
					Mode = SqlMode.Internal;
			}
		}

		public string DataSource
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] != null)
					return (string)mKeyValuePairs[ConnectionStringParams.DATA_SOURCE];
				else
					return "localhost";
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.DATA_SOURCE] = value;
			}
		}

		public string InitialCatalog
		{
			get
			{
				return (string)mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG];
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = value;
			}
		}

		public string UserId
		{
			get
			{
				return (string)mKeyValuePairs[ConnectionStringParams.USER_ID];
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.USER_ID] = value;
			}
		}

		public string Password
		{
			get
			{
				return (string)mKeyValuePairs[ConnectionStringParams.PASSWORD];
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.PASSWORD] = value;
			}
		}

		public int Timeout
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.TIMEOUT] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.TIMEOUT];
				else
					return 0;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.TIMEOUT] = value;
			}
		}

		public SqlMode Mode
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.MODE] != null)
					return (SqlMode)mKeyValuePairs[ConnectionStringParams.MODE];
				else
					return SqlMode.Internal;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.MODE] = value;
			}
		}

		public bool SpaceOption
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] != null)
					return (bool)mKeyValuePairs[ConnectionStringParams.SPACE_OPTION];
				else
					return false;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.SPACE_OPTION] = value;
			}
		}

		public bool Encrypt
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.ENCRYPT] != null)
					return (bool)mKeyValuePairs[ConnectionStringParams.ENCRYPT];
				else
					return false;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.ENCRYPT] = value;
			}
		}

		public string SslCertificateName
		{
			get
			{
				return (string)mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE];
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.SSL_CERTIFICATE] = value;
			}
		}

		public string Cache
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.CACHE] != null)
					return (string)mKeyValuePairs[ConnectionStringParams.CACHE];
				else
					return "all";
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.CACHE] = value;
			}
		}

		public int CacheSize
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.CACHE_SIZE] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.CACHE_SIZE];
				else
					return 0;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.CACHE_SIZE] = value;
			}
		}

		public int CacheLimit
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT];
				else
					return 0;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.CACHE_LIMIT] = value;
			}
		}

		public bool Pooling
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.POOLING] != null)
					return (bool)mKeyValuePairs[ConnectionStringParams.POOLING];
				else
					return true;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.POOLING] = value;
			}
		}

		public int ConnectionLifetime
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME];
				else
					return 0;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.CONNECTION_LIFETIME] = value;
			}
		}

		public int MinPoolSize
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE];
				else
					return 0;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.MIN_POOL_SIZE] = value;
			}
		}

		public int MaxPoolSize
		{
			get
			{
				if (mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE] != null)
					return (int)mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE];
				else
					return 100;
			}
			set
			{
				mKeyValuePairs[ConnectionStringParams.MAX_POOL_SIZE] = value;
			}
		}

		#region IDictionary Members

#if NET20
		public new void Add(string key, object value)
#else
        public void Add(object key, object value)
#endif
		{
			mKeyValuePairs[key] = value;
		}

#if NET20
		public override void Clear()
#else
        public void Clear()
#endif // NET20
		{
			mKeyValuePairs.Clear();
		}

		public bool Contains(object key)
		{
			return mKeyValuePairs.ContainsKey(key);
		}

		public IDictionaryEnumerator GetEnumerator()
		{
			return mKeyValuePairs.GetEnumerator();
		}

#if NET20
		public override bool IsFixedSize
#else
        public bool IsFixedSize
#endif // NET20
		{
			get
			{
				return mKeyValuePairs.IsFixedSize;
			}
		}

#if NET20
		public new bool IsReadOnly
#else
        public bool IsReadOnly
#endif // NET20
		{
			get
			{
				return mKeyValuePairs.IsReadOnly;
			}
		}

#if NET20
		public override ICollection Keys
#else
        public ICollection Keys
#endif // NET20
		{
			get
			{
				return mKeyValuePairs.Keys;
			}
		}

#if NET20
		public override bool Remove(string keyword)
		{
			if (Contains(keyword))
			{
				mKeyValuePairs.Remove(keyword);
				return true;
			}
			else
				return false;
		}
#else
		public void Remove(object key)
		{
			if (Contains(key))
				mKeyValuePairs.Remove(key);
		}
#endif

#if NET20
		public override ICollection Values
#else
        public ICollection Values
#endif // NET20
		{
			get
			{
				return mKeyValuePairs.Values;
			}
		}

#if NET20
		public override object this[string keyword]
#else
        public object this[object keyword]
#endif // NET20
		{
			get
			{
				return mKeyValuePairs[keyword];
			}
			set
			{
				mKeyValuePairs[keyword] = value;
			}
		}

		#endregion

		#region ICollection Members

		public void CopyTo(Array array, int index)
		{
			mKeyValuePairs.CopyTo(array, index);
		}

		public void CopyTo(string[] array, int index)
		{
			mKeyValuePairs.CopyTo(array, index);
		}

#if NET20
		public override int Count
#else
        public int Count
#endif
		{
			get
			{
				return mKeyValuePairs.Count;
			}
		}

		public bool IsSynchronized
		{
			get
			{
				return mKeyValuePairs.IsSynchronized;
			}
		}

		public object SyncRoot
		{
			get
			{
				return mKeyValuePairs.SyncRoot;
			}
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return mKeyValuePairs.GetEnumerator();
		}

		#endregion
	}
}
