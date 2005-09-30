using System;
using System.Data;
using System.Collections;
using System.Globalization;

namespace MaxDBDataProvider
{
	/*
		 * Because IDataParameterCollection is primarily an IList,
		 * the sample can use an existing class for most of the implementation.
		 */
	public class MaxDBParameterCollection : ArrayList, IDataParameterCollection
	{
		public object this[string index] 
		{
			get
			{
				return this[IndexOf(index)];
			}
			set
			{
				this[IndexOf(index)] = value;
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
				if (0 == _cultureAwareCompare(item.ParameterName, parameterName))
				{
					return index;
				}
				index++;
			}
			return -1;
		}

		public void RemoveAt(string parameterName)
		{
			RemoveAt(IndexOf(parameterName));
		}

		public override int Add(object value)
		{
			return Add((MaxDBParameter)value);
		}

		public int Add(MaxDBParameter value)
		{
			if (((MaxDBParameter)value).ParameterName != null)
			{
				return base.Add(value);
			}
			else
				throw new ArgumentException("parameter must be named");
		}

		public int Add(string parameterName, DbType type)
		{
			return Add(new MaxDBParameter(parameterName, type));
		}

		public int Add(string parameterName, object value)
		{
			return Add(new MaxDBParameter(parameterName, value));
		}

		public int Add(string parameterName, MaxDBType dbType, string sourceColumn)
		{
			return Add(new MaxDBParameter(parameterName, dbType, sourceColumn));
		}

		private int _cultureAwareCompare(string strA, string strB)
		{
			return CultureInfo.CurrentCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);
		}
	}

}
