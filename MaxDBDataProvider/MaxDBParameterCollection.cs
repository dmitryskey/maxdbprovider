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
using System.Data;
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif // NET20
using System.Globalization;
using System.Data.Common;

namespace MaxDB.Data
{
	public class MaxDBParameterCollection : 
#if NET20
        DbParameterCollection
#else
        IList, ICollection, IEnumerable
#endif
        ,IDataParameterCollection, ICloneable
	{
		private MaxDBParameter[] m_collection = new MaxDBParameter[0];

		#region "IDataParameterCollection implementation"

		object IDataParameterCollection.this[string index]
		{
			get
			{
				return this[IndexOf(index)];
			}
			set
			{
				this[IndexOf(index)] = (MaxDBParameter)value;
			}
		}

#if NET20
        public override bool Contains(string parameterName)
#else
		public bool Contains(string parameterName)
#endif // NET20
		{
			return(-1 != IndexOf(parameterName));
		}

#if NET20
        public override int IndexOf(string parameterName)
#else
		public int IndexOf(string parameterName)
#endif // NET20
		{
			int index = 0;
			foreach(MaxDBParameter item in this) 
			{
				if (_cultureAwareCompare(item.ParameterName, parameterName) == 0)
					return index;
				index++;
			}
			return -1;
		}

#if NET20
        public override void RemoveAt(string parameterName)
#else
		public void RemoveAt(string parameterName)
#endif // NET20
		{
			RemoveAt(IndexOf(parameterName));
		}

		private int _cultureAwareCompare(string strA, string strB)
		{
			return CultureInfo.CurrentCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);
		}

		#endregion

		#region "ICollection implementation"

#if NET20
        public override void CopyTo(Array array, int index)
#else
		public void CopyTo(Array array, int index)
#endif // NET20
		{
			m_collection.CopyTo(array, index);
		}

#if NET20
        public override int Count
#else
		public int Count
#endif // NET20
		{
			get
			{
				return m_collection.Length;
			}
		}

#if NET20
        public override bool IsSynchronized
#else
		public virtual bool IsSynchronized
#endif // NET20
		{
			get
			{
				return m_collection.IsSynchronized;
			}
		}

#if NET20
        public override object SyncRoot
#else
		public object SyncRoot
 #endif // NET20
		{
			get
			{
				return m_collection.SyncRoot;
			}
		}

		#endregion

		#region "IEnumerable implementation"

#if NET20
        public override IEnumerator GetEnumerator()
#else
		public IEnumerator GetEnumerator()
#endif // NET20
        {
			return m_collection.GetEnumerator();
		}

		#endregion

		#region "IList implementation"
		
#if NET20
        public override int Add(object val)
#else
		public int Add(object val)
#endif
		{
			Add((MaxDBParameter)val);
			return m_collection.Length - 1;
		}

#if NET20
        public override void Clear()
#else
		public void Clear()
#endif // NET20
		{
			m_collection = new MaxDBParameter[0];
		}

#if NET20
        public override bool Contains(object parameter)
#else
		public bool Contains(object parameter)
#endif // NET20
		{
			foreach(MaxDBParameter param in m_collection)
				if (param == parameter)
					return true;

			return false;
		}

#if NET20
        public override void RemoveAt(int index)
#else
		public void RemoveAt(int index)
#endif // NET20
		{
#if NET20
            List<MaxDBParameter> tmp_array = new List<MaxDBParameter>(m_collection);
#else
            ArrayList tmp_array = new ArrayList(m_collection);
#endif // NET20
			tmp_array.RemoveAt(index);
			m_collection = new MaxDBParameter[tmp_array.Count];
			tmp_array.CopyTo(m_collection);
		}

#if NET20
        public override int IndexOf(object parameter)
#else
		public int IndexOf(object parameter)
#endif // NET20
		{
			for (int index = 0; index < m_collection.Length; index++)
				if (m_collection[index] == parameter)
					return index;

			return -1;
		}

#if NET20
        public override void Insert(int index, object value)
#else
		public void Insert(int index, object value)
#endif // NET20
		{
#if NET20
            List<MaxDBParameter> tmp_array = new List<MaxDBParameter>(m_collection);
#else
            ArrayList tmp_array = new ArrayList(m_collection);
#endif // NET20
            tmp_array.Insert(index, (MaxDBParameter)value);
            m_collection = new MaxDBParameter[tmp_array.Count];
			tmp_array.CopyTo(m_collection);
		}

#if NET20
        public override bool IsFixedSize
#else
		public bool IsFixedSize
#endif // NET20
		{
			get
			{
				return m_collection.IsFixedSize;
			}
		}

#if NET20
        public override bool IsReadOnly
#else
		public bool IsReadOnly
#endif // NET20
		{
			get
			{
				return m_collection.IsReadOnly;
			}
		}

#if NET20
        public override void Remove(object value)
#else
		public void Remove(object value)
#endif // NET20
		{
			int index = ((IList)this).IndexOf(value);
			if (index >=0)
				RemoveAt(index);
		}

		object IList.this[int index]
		{
			get
			{
				return m_collection[index];
			}
			set
			{
				m_collection[index] = (MaxDBParameter)value;
			}
		}

		#endregion

#if NET20
        public new MaxDBParameter this[int index]
#else
		public MaxDBParameter this[int index]
#endif
		{
			get
			{
				return (MaxDBParameter)m_collection[index];
			}
			set
			{
				m_collection[index] = value;
			}
		}

		public MaxDBParameter Add(MaxDBParameter val)
		{
			if (val.ParameterName != null)
			{
				MaxDBParameter[] new_collection = new MaxDBParameter[m_collection.Length + 1];
				m_collection.CopyTo(new_collection, 0);
				new_collection[m_collection.Length] = val;
				m_collection = new_collection;
				return val;
			}
			else
				throw new ArgumentException(MaxDBMessages.Extract(MaxDBMessages.ERROR_UNNAMED_PARAMETER));
		}

		public MaxDBParameter Add(string parameterName, MaxDBType type)
		{
			return Add(new MaxDBParameter(parameterName, type));
		}

		public MaxDBParameter Add(string parameterName, object val)
		{
			return Add(new MaxDBParameter(parameterName, val));
		}

		public MaxDBParameter Add(string parameterName, MaxDBType dbType, int size)
		{
			return Add(new MaxDBParameter(parameterName, dbType, size));
		}

		public MaxDBParameter Add(string parameterName, MaxDBType type, int size, string sourceColumn)
		{
			return Add(new MaxDBParameter(parameterName, type, size, sourceColumn));
		}

		public MaxDBParameter Add(string parameterName, MaxDBType type, int size, ParameterDirection direction,
			bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object val)
		{
			return Add(new MaxDBParameter(parameterName, type, size, direction, isNullable, precision, scale, sourceColumn, sourceVersion, val));
		}

#if NET20
        public override void AddRange(Array values)
#else
        public void AddRange(Array values)
#endif // NET20
        {
            foreach(MaxDBParameter param in values)
                Add(param);
        }

        public Array ToArray()
        {
            return m_collection;
        }
 
#if NET20
        protected override DbParameter GetParameter(int index)
        {
            return this[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return this[parameterName];
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (MaxDBParameter)value;
        }
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            this[parameterName] = (MaxDBParameter)value;
        }
#endif // NET20

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return this.Clone();
        }

        public MaxDBParameterCollection Clone()
        {
            MaxDBParameterCollection clone = new MaxDBParameterCollection();
            foreach (MaxDBParameter param in this)
                clone.Add(param.Clone());
            return clone;
        }

        #endregion
    }
}
