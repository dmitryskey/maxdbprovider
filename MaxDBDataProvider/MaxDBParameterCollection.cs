using System;
using System.Data;
using System.Collections;
using System.Globalization;

namespace MaxDBDataProvider
{
	public class MaxDBParameterCollection : IDataParameterCollection, IList, ICollection, IEnumerable
	{
		private MaxDBParameter[] collection = new MaxDBParameter[0];

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

		public bool Contains(string parameterName)
		{
			return(-1 != IndexOf(parameterName));
		}

		public int IndexOf(string parameterName)
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

		public void RemoveAt(string parameterName)
		{
			RemoveAt(IndexOf(parameterName));
		}

		private int _cultureAwareCompare(string strA, string strB)
		{
			return CultureInfo.CurrentCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);
		}

		#endregion

		#region "ICollection implementation"

		public void CopyTo(Array array, int index)
		{
			collection.CopyTo(array, index);
		}

		public int Count
		{
			get
			{
				return collection.Length;
			}
		}

		public virtual bool IsSynchronized 
		{
			get
			{
				return collection.IsSynchronized;
			}
		}

		public object SyncRoot 
		{
			get
			{
				return collection.SyncRoot;
			}
		}

		#endregion

		#region "IEnumerable implementation"

		public IEnumerator GetEnumerator()
		{
			return collection.GetEnumerator();
		}

		#endregion

		#region "IList implementation"
		
		int IList.Add(object val)
		{
			Add((MaxDBParameter)val);
			return collection.Length - 1;
		}

		public void Clear()
		{
			collection = new MaxDBParameter[0];
		}

		public bool Contains(object parameter)
		{
			foreach(MaxDBParameter param in collection)
				if (param == parameter)
					return true;

			return false;
		}

		public void RemoveAt(int index)
		{
			ArrayList tmp_array = new ArrayList(collection);
			tmp_array.RemoveAt(index);
			collection = new MaxDBParameter[tmp_array.Count];
			tmp_array.CopyTo(collection);
		}

		public int IndexOf(object parameter)
		{
			for (int index = 0; index < collection.Length; index++)
				if (collection[index] == parameter)
					return index;

			return -1;
		}

		public void Insert(int index, object value)
		{
			ArrayList tmp_array = new ArrayList(collection);
			tmp_array.Insert(index, value);
			collection = new MaxDBParameter[tmp_array.Count];
			tmp_array.CopyTo(collection);
		}

		public bool IsFixedSize
		{
			get
			{
				return collection.IsFixedSize;
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return collection.IsReadOnly;
			}
		}

		public void Remove(object value)
		{
			int index = ((IList)this).IndexOf(value);
			if (index >=0)
				RemoveAt(index);
		}

		object IList.this[int index]
		{
			get
			{
				return collection[index];
			}
			set
			{
				collection[index] = (MaxDBParameter)value;
			}
		}

		#endregion

		public MaxDBParameter this[int index]
		{
			get
			{
				return (MaxDBParameter)collection[index];
			}
			set
			{
				collection[index] = value;
			}
		}

		public MaxDBParameter Add(MaxDBParameter val)
		{
			if (val.ParameterName != null)
			{
				MaxDBParameter[] new_collection = new MaxDBParameter[collection.Length + 1];
				collection.CopyTo(new_collection, 0);
				new_collection[collection.Length] = val;
				collection = new_collection;
				return val;
			}
			else
				throw new ArgumentException(MessageTranslator.Translate(MessageKey.ERROR_UNNAMED_PARAMETER));
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
	}
}
