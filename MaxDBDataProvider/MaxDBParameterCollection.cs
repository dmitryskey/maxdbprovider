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
		private MaxDBParameter[] mCollection = new MaxDBParameter[0];

		#region "IDataParameterCollection implementation"

		object IDataParameterCollection.this[string parameterName]
		{
			get
			{
                return this[IndexOf(parameterName)];
			}
			set
			{
                this[IndexOf(parameterName)] = (MaxDBParameter)value;
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

		private static int _cultureAwareCompare(string strA, string strB)
		{
			return CultureInfo.InvariantCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);
		}

		#endregion

		#region "ICollection implementation"

#if NET20
        public override void CopyTo(Array array, int index)
#else
		public void CopyTo(Array array, int index)
#endif // NET20
		{
			mCollection.CopyTo(array, index);
		}

        public void CopyTo(MaxDBParameter[] array, int index)
        {
            mCollection.CopyTo(array, index);
        }

#if NET20
        public override int Count
#else
		public int Count
#endif // NET20
		{
			get
			{
				return mCollection.Length;
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
				return mCollection.IsSynchronized;
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
				return mCollection.SyncRoot;
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
			return mCollection.GetEnumerator();
		}

		#endregion

		#region "IList implementation"
		
#if NET20
        public override int Add(object value)
#else
		public int Add(object value)
#endif
		{
			Add((MaxDBParameter)value);
			return mCollection.Length - 1;
		}

#if NET20
        public override void Clear()
#else
		public void Clear()
#endif // NET20
		{
			mCollection = new MaxDBParameter[0];
		}

#if NET20
        public override bool Contains(object value)
#else
		public bool Contains(object value)
#endif // NET20
		{
			foreach(MaxDBParameter param in mCollection)
				if (param == value)
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
            List<MaxDBParameter> tmp_array = new List<MaxDBParameter>(mCollection);
#else
            ArrayList tmp_array = new ArrayList(mCollection);
#endif // NET20
			tmp_array.RemoveAt(index);
			mCollection = new MaxDBParameter[tmp_array.Count];
			tmp_array.CopyTo(mCollection);
		}

#if NET20
        public override int IndexOf(object value)
#else
		public int IndexOf(object value)
#endif // NET20
		{
			for (int index = 0; index < mCollection.Length; index++)
				if (mCollection[index] == value)
					return index;

			return -1;
		}

        public void Insert(int index, MaxDBParameter value)
        {
#if NET20
            List<MaxDBParameter> tmp_array = new List<MaxDBParameter>(mCollection);
#else
            ArrayList tmp_array = new ArrayList(mCollection);
#endif // NET20
            tmp_array.Insert(index, (MaxDBParameter)value);
            mCollection = new MaxDBParameter[tmp_array.Count];
            tmp_array.CopyTo(mCollection);
        }

#if NET20
        public override void Insert(int index, object value)
#else
		public void Insert(int index, object value)
#endif // NET20
		{
            Insert(index, (MaxDBParameter)value);
		}

#if NET20
        public override bool IsFixedSize
#else
		public bool IsFixedSize
#endif // NET20
		{
			get
			{
				return mCollection.IsFixedSize;
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
				return mCollection.IsReadOnly;
			}
		}

#if NET20
        public override void Remove(object value)
#else
		public void Remove(object value)
#endif // NET20
		{
            Remove((MaxDBParameter)value);
		}

        public void Remove(MaxDBParameter value)
        {
            int index = ((IList)this).IndexOf(value);
			if (index >=0)
			    RemoveAt(index);
        }

		object IList.this[int index]
		{
			get
			{
				return mCollection[index];
			}
			set
			{
				mCollection[index] = (MaxDBParameter)value;
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
				return (MaxDBParameter)mCollection[index];
			}
			set
			{
				mCollection[index] = value;
			}
		}

		public MaxDBParameter Add(MaxDBParameter value)
		{
            if (value == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

			if (value.ParameterName != null)
			{
				MaxDBParameter[] new_collection = new MaxDBParameter[mCollection.Length + 1];
				mCollection.CopyTo(new_collection, 0);
				new_collection[mCollection.Length] = value;
				mCollection = new_collection;
				return value;
			}
			else
				throw new ArgumentException(MaxDBMessages.Extract(MaxDBError.UNNAMED_PARAMETER));
		}

		public MaxDBParameter Add(string parameterName, MaxDBType type)
		{
			return Add(new MaxDBParameter(parameterName, type));
		}

		public MaxDBParameter Add(string parameterName, object value)
		{
			return Add(new MaxDBParameter(parameterName, value));
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
			bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object value)
		{
			return Add(new MaxDBParameter(parameterName, type, size, direction, isNullable, precision, scale, sourceColumn, sourceVersion, value));
		}

#if NET20
        public override void AddRange(Array values)
#else
        public void AddRange(Array values)
#endif // NET20
        {
            if (values == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));

            foreach(MaxDBParameter param in values)
                Add(param);
        }

        public Array ToArray()
        {
            return mCollection;
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
