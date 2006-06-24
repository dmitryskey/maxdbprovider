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

namespace MaxDB.Data
{
    public class MaxDBConnectionStringBuilder : 
#if NET20
        DbConnectionStringBuilder
#else
        IDictionary, ICollection, ICustomTypeDescriptor
#endif
        , IEnumerable
    {
        private Hashtable m_KeyValuePairs = new Hashtable();
        private bool m_browsable = true;

        public MaxDBConnectionStringBuilder()
        {
        }

        public MaxDBConnectionStringBuilder(string connStr)
        {
            ConnectionString = connStr;
        }

#if NET20
        public new bool BrowsableConnectionString
#else
        public bool BrowsableConnectionString
#endif // NET20
        { 
            get
            {
                return m_browsable;
            } 
            set
            {
                m_browsable = value;
            }
        }

#if NET20
        public static new void AppendKeyValuePair(StringBuilder builder, string keyword, string value)
#else
        public static void AppendKeyValuePair(StringBuilder builder, string keyword, string value)
#endif // NET20
        {
            builder.Append(keyword).Append("=").Append(value).Append(";");
        }

#if NET20
        public override bool ShouldSerialize(string keyword)
#else
        public bool ShouldSerialize(string keyword)
#endif
		{
			return m_KeyValuePairs.ContainsKey(keyword);
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
            if (m_KeyValuePairs.ContainsKey(keyword))
            {
                value = m_KeyValuePairs[keyword];
                return true;
            }
            else
            {
                value = null;
                return false;
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
                foreach (string key in m_KeyValuePairs)
                    builder.Append(key).Append("=").Append(m_KeyValuePairs[key]).Append(";");

                return builder.ToString();
            }
            set
            {
                string[] paramArr = value.Split(';');
                foreach (string param in paramArr)
                {
                    if (param.Split('=').Length > 1)
                        switch (param.Split('=')[0].Trim().ToUpper())
                        {
                            case "DATA SOURCE":
                            case "SERVER":
                            case "ADDRESS":
                            case "ADDR":
                            case "NETWORK ADDRESS":
                                m_KeyValuePairs[ConnectionStringParams.DATA_SOURCE] = param.Split('=')[1].Trim();
                                break;
                            case "INITIAL CATALOG":
                            case "DATABASE":
                                m_KeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = param.Split('=')[1].Trim();
                                break;
                            case "USER ID":
                            case "LOGIN":
                                m_KeyValuePairs[ConnectionStringParams.USER_ID] = param.Split('=')[1].Trim();
                                break;
                            case "PASSWORD":
                            case "PWD":
                                m_KeyValuePairs[ConnectionStringParams.PASSWORD] = param.Split('=')[1].Trim();
                                break;
                            case "TIMEOUT":
                                try
                                {
                                    m_KeyValuePairs[ConnectionStringParams.TIMEOUT] = int.Parse(param.Split('=')[1]);
                                }
                                catch
                                {
                                    m_KeyValuePairs[ConnectionStringParams.TIMEOUT] = 0;
                                }
                                break;
                            case "SPACE OPTION":
                                if (param.Split('=')[1].Trim().ToUpper() == "TRUE" || 
                                    param.Split('=')[1].Trim().ToUpper() == "YES" ||
                                    param.Split('=')[1].Trim() == "1")
                                    m_KeyValuePairs[ConnectionStringParams.SPACE_OPTION] = true;
                                break;
                            case "CACHE":
                                m_KeyValuePairs[ConnectionStringParams.CACHE] = param.Split('=')[1].Trim();
                                break;
                            case "CACHE LIMIT":
                                try
                                {
                                    m_KeyValuePairs[ConnectionStringParams.CACHE_LIMIT] = int.Parse(param.Split('=')[1]);
                                }
                                catch
                                {
                                    m_KeyValuePairs[ConnectionStringParams.CACHE_LIMIT] = 0;
                                }
                                break;
                            case "CACHE SIZE":
                                try
                                {
                                    m_KeyValuePairs[ConnectionStringParams.CACHE_SIZE] = int.Parse(param.Split('=')[1]);
                                }
                                catch
                                {
                                    m_KeyValuePairs[ConnectionStringParams.CACHE_SIZE] = 0;
                                }
                                break;
                            case "ENCRYPT":
                                if (param.Split('=')[1].Trim().ToUpper() == "TRUE" || 
                                    param.Split('=')[1].Trim().ToUpper() == "YES" ||
                                    param.Split('=')[1].Trim() == "1")
                                    m_KeyValuePairs[ConnectionStringParams.ENCRYPT] = true;
                                break;
                            case "MODE":
                                string mode = param.Split('=')[1].Trim().ToUpper();
                                if (mode == SqlModeName.Value[SqlMode.Ansi])
                                {
                                    m_KeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Ansi;
                                    break;
                                }
                                if (mode == SqlModeName.Value[SqlMode.Db2])
                                {
                                    m_KeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Db2;
                                    break;
                                }
                                if (mode == SqlModeName.Value[SqlMode.Oracle])
                                {
                                    m_KeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Oracle;
                                    break;
                                }
                                if (mode == SqlModeName.Value[SqlMode.SAPR3])
                                {
                                    m_KeyValuePairs[ConnectionStringParams.MODE] = SqlMode.SAPR3;
                                    break;
                                }
                                m_KeyValuePairs[ConnectionStringParams.MODE] = SqlMode.Internal;
                                break;
                            case "SSL HOST":
                                m_KeyValuePairs[ConnectionStringParams.SSL_HOST] = param.Split('=')[1].Trim();
                                break;
                        }
                }

            }
        }

        public string DataSource
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.DATA_SOURCE];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.DATA_SOURCE] = value;
            }
        }

        public string InitialCatalog
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.INITIAL_CATALOG];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.INITIAL_CATALOG] = value;
            }
        }

        public string UserID
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.USER_ID];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.USER_ID] = value;
            }
        }

        public string Password
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.PASSWORD];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.PASSWORD] = value;
            }
        }

        public int Timeout
        {
            get
            {
                if (m_KeyValuePairs[ConnectionStringParams.TIMEOUT] != null)
                    return (int)m_KeyValuePairs[ConnectionStringParams.TIMEOUT];
                else
                    return 0;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.TIMEOUT] = value;
            }
        }

        public int Mode
        {
            get 
            {
                if (m_KeyValuePairs[ConnectionStringParams.MODE] != null)
                    return (int)m_KeyValuePairs[ConnectionStringParams.MODE];
                else
                    return SqlMode.Internal;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.MODE] = value;
            }
        }

        public bool SpaceOption
        {
            get
            {
                if (m_KeyValuePairs[ConnectionStringParams.SPACE_OPTION] != null)
                    return (bool)m_KeyValuePairs[ConnectionStringParams.SPACE_OPTION];
                else
                    return false;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.SPACE_OPTION] = value;
            }
        }

        public bool Encrypt
        {
            get
            {
                if (m_KeyValuePairs[ConnectionStringParams.ENCRYPT] != null)
                    return (bool)m_KeyValuePairs[ConnectionStringParams.ENCRYPT];
                else
                    return false;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.ENCRYPT] = value;
            }
        }

        public string SslHost
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.SSL_HOST];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.SSL_HOST] = value;
            }
        }

        public string Cache
        {
            get
            {
                return (string)m_KeyValuePairs[ConnectionStringParams.CACHE];
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.CACHE] = value;
            }
        }

        public int CacheSize
        {
            get
            {
                if (m_KeyValuePairs[ConnectionStringParams.CACHE_SIZE] != null)
                    return (int)m_KeyValuePairs[ConnectionStringParams.CACHE_SIZE];
                else
                    return 0;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.CACHE_SIZE] = value;
            }
        }

        public int CacheLimit
        {
            get
            {
                if (m_KeyValuePairs[ConnectionStringParams.CACHE_LIMIT] != null)
                    return (int)m_KeyValuePairs[ConnectionStringParams.CACHE_LIMIT];
                else
                    return 0;
            }
            set
            {
                m_KeyValuePairs[ConnectionStringParams.CACHE_LIMIT] = value;
            }
        }

        #region IDictionary Members

        public void Add(object key, object value)
        {
            m_KeyValuePairs[key] = value;
        }

#if NET20
        public override void Clear()
#else
        public void Clear()
#endif // NET20
        {
            m_KeyValuePairs.Clear();
        }

        public bool Contains(object key)
        {
            return m_KeyValuePairs.ContainsKey(key);
        }

        public IDictionaryEnumerator GetEnumerator()
        {
            return m_KeyValuePairs.GetEnumerator();
        }

#if NET20
        public override bool IsFixedSize
#else
        public bool IsFixedSize
#endif // NET20
        {
            get
            {
                return m_KeyValuePairs.IsFixedSize;
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
                return m_KeyValuePairs.IsReadOnly;
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
                return m_KeyValuePairs.Keys;
            }
        }

        public void Remove(object key)
        {
            m_KeyValuePairs.Remove(key);
        }

#if NET20
        public override ICollection Values
#else
        public ICollection Values
#endif // NET20
        {
            get
            {
                return m_KeyValuePairs.Values;
            }
        }

        public object this[object key]
        {
            get
            {
                return m_KeyValuePairs[key];
            }
            set
            {
                m_KeyValuePairs[key] = value;
            }
        }

        #endregion

        #region ICollection Members

        public void CopyTo(Array array, int index)
        {
            m_KeyValuePairs.CopyTo(array, index);
        }

#if NET20
        public override int Count
#else
        public int Count
#endif
        {
            get
            {
                return m_KeyValuePairs.Count;
            }
        }

        public bool IsSynchronized
        {
            get
            {
                return m_KeyValuePairs.IsSynchronized;
            }
        }

        public object SyncRoot
        {
            get
            {
                return m_KeyValuePairs.SyncRoot;
            }
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return m_KeyValuePairs.GetEnumerator();
        }

        #endregion

        #region ICustomTypeDescriptor Members

        public AttributeCollection GetAttributes()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public string GetClassName()
        {
            return typeof(MaxDBConnectionStringBuilder).Name;
        }

        public string GetComponentName()
        {
            return Assembly.GetExecutingAssembly().FullName;
        }

        public TypeConverter GetConverter()
        {
            return new CollectionConverter();
        }

        public EventDescriptor GetDefaultEvent()
        {
            return null;
        }

        public PropertyDescriptor GetDefaultProperty()
        {
            return null;
        }

        public object GetEditor(Type editorBaseType)
        {
            return null;
        }

        public EventDescriptorCollection GetEvents(Attribute[] attributes)
        {
            return EventDescriptorCollection.Empty;
        }

        public EventDescriptorCollection GetEvents()
        {
            return EventDescriptorCollection.Empty;
        }

        public PropertyDescriptorCollection GetProperties(Attribute[] attributes)
        {
            return PropertyDescriptorCollection.Empty;
        }

        public PropertyDescriptorCollection GetProperties()
        {
            return PropertyDescriptorCollection.Empty;
        }

        public object GetPropertyOwner(PropertyDescriptor pd)
        {
            return null;
        }

        #endregion
    }
}
