//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBParameterCollection.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
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
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;

    /// <summary>
    /// Represents a collection of parameters relevant to a <see cref="MaxDBCommand"/> as
    /// well as their respective mappings to columns in a <see cref="DataSet"/>. This class cannot be inherited.
    /// </summary>
    public sealed class MaxDBParameterCollection : DbParameterCollection, IDataParameterCollection, ICloneable
    {
        private MaxDBParameter[] mCollection = Array.Empty<MaxDBParameter>();

        #region "IDataParameterCollection implementation"

        object IDataParameterCollection.this[string parameterName]
        {
            get => this[this.IndexOf(parameterName)];
            set => this[this.IndexOf(parameterName)] = (MaxDBParameter)value;
        }

        /// <summary>
        /// Gets a value indicating whether a MaxDBParameter exists in the collection.
        /// </summary>
        /// <param name="parameterName">The value of the <see cref="MaxDBParameter"/> object to find. </param>
        /// <returns>true if the collection contains the <see cref="MaxDBParameter"/> object; otherwise, false.</returns>
        public override bool Contains(string parameterName) => -1 != this.IndexOf(parameterName);

        /// <summary>
        /// Gets the location of a <see cref="MaxDBParameter"/> in the collection.
        /// </summary>
        /// <param name="parameterName">The <see cref="MaxDBParameter"/> object to locate. </param>
        /// <returns>The zero-based location of the <see cref="MaxDBParameter"/> in the collection.</returns>
        public override int IndexOf(string parameterName)
        {
            int index = 0;
            foreach (MaxDBParameter item in this)
            {
                if (_cultureAwareCompare(item.ParameterName, parameterName) == 0)
                {
                    return index;
                }

                index++;
            }

            return -1;
        }

        /// <summary>
        /// Removes the specified <see cref="MaxDBParameter"/> from the collection using a name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        public override void RemoveAt(string parameterName) => this.RemoveAt(this.IndexOf(parameterName));

        private static int _cultureAwareCompare(string strA, string strB) =>
            CultureInfo.InvariantCulture.CompareInfo.Compare(strA, strB, CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth | CompareOptions.IgnoreCase);

        #endregion

        #region "ICollection implementation"

        /// <summary>
        /// Copy values to the one-dimensional array starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">A target array.</param>
        /// <param name="index">The index in the array at which to begin copying.</param>
        public override void CopyTo(Array array, int index) => this.mCollection.CopyTo(array, index);

        /// <summary>
        /// Copy values to the one-dimensional <see cref="MaxDBParameter"/> array starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">A target array.</param>
        /// <param name="index">The index in the array at which to begin copying.</param>
        public void CopyTo(MaxDBParameter[] array, int index) => this.mCollection.CopyTo(array, index);

        /// <summary>
        /// Gets the number of <see cref="MaxDBParameter"/> objects in the collection.
        /// </summary>
        public override int Count => this.mCollection.Length;

        /// <summary>
        /// Gets a value indicating whether collection is synchronized.
        /// </summary>
        public override bool IsSynchronized => this.mCollection.IsSynchronized;

        /// <summary>
        /// Gets an object that can be used to synchronize access to the collection.
        /// </summary>
        public override object SyncRoot => this.mCollection.SyncRoot;

        #endregion

        #region "IEnumerable implementation"

        /// <summary>
        /// Returns an enumerator to support iterating through the collection.
        /// </summary>
        /// <returns>An enumerator object.</returns>
        public override IEnumerator GetEnumerator() => this.mCollection.GetEnumerator();

        #endregion

        #region "IList implementation"

        /// <summary>
        /// Adds the specified <see cref="MaxDBParameter"/> object to the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        /// <param name="value">The <see cref="MaxDBParameter"/> to add to the collection.</param>
        /// <returns>The index of the new <see cref="MaxDBParameter"/> object.</returns>
        public override int Add(object value)
        {
            this.Add((MaxDBParameter)value);
            return this.mCollection.Length - 1;
        }

        /// <summary>
        /// Removes all items from the collection.
        /// </summary>
        public override void Clear() => this.mCollection = Array.Empty<MaxDBParameter>();

        /// <summary>
        /// Gets a value indicating whether a <see cref="MaxDBParameter"/> exists in the collection.
        /// </summary>
        /// <param name="value">The value of the <see cref="MaxDBParameter"/> object to find.</param>
        /// <returns>true if the collection contains the <see cref="MaxDBParameter"/> object and false otherwise.</returns>
        public override bool Contains(object value)
        {
            foreach (var param in this.mCollection)
            {
                if (param == value)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Removes the specified <see cref="MaxDBParameter"/> from the collection using a specific index.
        /// </summary>
        /// <param name="index">The zero-based index of the parameter.</param>
        public override void RemoveAt(int index)
        {
            var tmp_array = new List<MaxDBParameter>(this.mCollection);
            tmp_array.RemoveAt(index);
            this.mCollection = new MaxDBParameter[tmp_array.Count];
            tmp_array.CopyTo(this.mCollection);
        }

        /// <summary>
        /// Gets the location of a <see cref="MaxDBParameter"/> in the collection.
        /// </summary>
        /// <param name="value">The <see cref="MaxDBParameter"/> object to locate. </param>
        /// <returns>The zero-based location of the <see cref="MaxDBParameter"/> in the collection.</returns>
        public override int IndexOf(object value)
        {
            for (int index = 0; index < this.mCollection.Length; index++)
            {
                if (this.mCollection[index] == value)
                {
                    return index;
                }
            }

            return -1;
        }

        /// <summary>
        /// Inserts a <see cref="MaxDBParameter"/> into the collection at the specified index.
        /// </summary>
        /// <param name="index">Collection index.</param>
        /// <param name="value"><see cref="MaxDBParameter"/> object to insert.</param>
        public void Insert(int index, MaxDBParameter value)
        {
            var tmp_array = new List<MaxDBParameter>(this.mCollection);
            tmp_array.Insert(index, (MaxDBParameter)value);
            this.mCollection = new MaxDBParameter[tmp_array.Count];
            tmp_array.CopyTo(this.mCollection);
        }

        /// <summary>
        /// Inserts a <see cref="MaxDBParameter"/> into the collection at the specified index.
        /// </summary>
        /// <param name="index">Collection index.</param>
        /// <param name="value">Object to insert.</param>
        public override void Insert(int index, object value)
        {
            this.Insert(index, (MaxDBParameter)value);
        }

        /// <summary>
        /// Gets a value indicating whether the collection has fixed size.
        /// </summary>
        public override bool IsFixedSize => this.mCollection.IsFixedSize;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public override bool IsReadOnly => this.mCollection.IsReadOnly;

        /// <summary>
        /// Removes the specified <see cref="MaxDBParameter"/> from the collection.
        /// </summary>
        /// <param name="value">Object to remove</param>
        public override void Remove(object value)
        {
            this.Remove((MaxDBParameter)value);
        }

        /// <summary>
        /// Removes the specified <see cref="MaxDBParameter"/> from the collection.
        /// </summary>
        /// <param name="value">The <see cref="MaxDBParameter"/> to remove</param>
        public void Remove(MaxDBParameter value)
        {
            int index = this.IndexOf(value);
            if (index >= 0)
            {
                this.RemoveAt(index);
            }
        }

        object IList.this[int index]
        {
            get
            {
                return this.mCollection[index];
            }
            set
            {
                this.mCollection[index] = (MaxDBParameter)value;
            }
        }

        #endregion

        /// <summary>
        /// Gets or sets <see cref="MaxDBParameter"/> by index.
        /// </summary>
        /// <param name="index">The index of the <see cref="MaxDBParameter"/></param>
        /// <returns>The <see cref="MaxDBParameter"/> object.</returns>
        public new MaxDBParameter this[int index]
        {
            get
            {
                return this.mCollection[index];
            }
            set
            {
                this.mCollection[index] = value;
            }
        }

        /// <summary>
        /// Adds the specified <see cref="MaxDBParameter"/> object to the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        /// <param name="value">The <see cref="MaxDBParameter"/> to add to the collection.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(MaxDBParameter value)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            if (value.ParameterName != null)
            {
                var new_collection = new MaxDBParameter[this.mCollection.Length + 1];
                this.mCollection.CopyTo(new_collection, 0);
                new_collection[this.mCollection.Length] = value;
                this.mCollection = new_collection;
                return value;
            }
            else
            {
                throw new ArgumentException(MaxDBMessages.Extract(MaxDBError.UNNAMED_PARAMETER));
            }
        }

        /// <summary>
        /// Adds a <see cref="MaxDBParameter"/> to the <see cref="MaxDBParameterCollection"/> given the specified parameter name and type.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="type">The <see cref="MaxDBType"/> of the <see cref="MaxDBParameter"/>.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(string parameterName, MaxDBType type)
        {
            return this.Add(new MaxDBParameter(parameterName, type));
        }

        /// <summary>
        /// Adds a <see cref="MaxDBParameter"/> to the <see cref="MaxDBParameterCollection"/> given the specified parameter name and value.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="value">The <see cref="MaxDBParameter.Value"/> of the <see cref="MaxDBParameter"/> to add to the collection.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(string parameterName, object value)
        {
            return this.Add(new MaxDBParameter(parameterName, value));
        }

        /// <summary>
        /// Adds a <see cref="MaxDBParameter"/> to the <see cref="MaxDBParameterCollection"/> given the specified parameter name, type and size.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="type">The <see cref="MaxDBType"/> of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="size">The size of the <see cref="MaxDBParameter"/>.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(string parameterName, MaxDBType type, int size)
        {
            return this.Add(new MaxDBParameter(parameterName, type, size));
        }

        /// <summary>
        /// Adds a <see cref="MaxDBParameter"/> to the <see cref="MaxDBParameterCollection"/> given the specified parameter name, type, size
        /// and source column.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="type">The <see cref="MaxDBType"/> of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="size">The size of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="sourceColumn">The source column of the <see cref="MaxDBParameter"/>.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(string parameterName, MaxDBType type, int size, string sourceColumn)
        {
            return this.Add(new MaxDBParameter(parameterName, type, size, sourceColumn));
        }

        /// <summary>
        /// Adds a <see cref="MaxDBParameter"/> to the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="type">The <see cref="MaxDBType"/> of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="size">The size of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="direction">The <see cref="ParameterDirection"/> of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="isNullable">Whether <see cref="MaxDBParameter"/> is nullable.</param>
        /// <param name="precision">The precision of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="scale">The scale of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="sourceColumn">The source column of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="sourceVersion">The row version of the <see cref="MaxDBParameter"/>.</param>
        /// <param name="value">The <see cref="MaxDBParameter.Value"/> of the <see cref="MaxDBParameter"/> to add to the collection.</param>
        /// <returns>The newly added <see cref="MaxDBParameter"/> object.</returns>
        public MaxDBParameter Add(string parameterName, MaxDBType type, int size, ParameterDirection direction,
            bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object value)
        {
            return this.Add(new MaxDBParameter(parameterName, type, size, direction, isNullable, precision, scale, sourceColumn, sourceVersion, value));
        }

        /// <summary>
        /// Adds elements to the end of the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        /// <param name="values">Adds an array of values to the end of the <see cref="MaxDBParameterCollection"/>.</param>
        public override void AddRange(Array values)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));
            }

            foreach (MaxDBParameter param in values)
            {
                this.Add(param);
            }
        }

        /// <summary>
        /// Adds elements to the end of the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        /// <param name="values">Adds an array of <see cref="MaxDBParameter"/>
        /// values to the end of the <see cref="MaxDBParameterCollection"/>.</param>
        public void AddRange(MaxDBParameter[] values)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));
            }

            foreach (MaxDBParameter param in values)
            {
                this.Add(param);
            }
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="index">The index of the <see cref="DbParameter"/>.</param>
        /// <returns>The <see cref="DbParameter"/> object.</returns>
        protected override DbParameter GetParameter(int index)
        {
            return this[index];
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="DbParameter"/>.</param>
        /// <returns>The <see cref="DbParameter"/> object.</returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            return this[parameterName];
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="index">The index of the <see cref="DbParameter"/>.</param>
        /// <param name="value">The <see cref="MaxDBParameter.Value"/> of the <see cref="MaxDBParameter"/> to set.</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (MaxDBParameter)value;
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="DbParameter"/>.</param>
        /// <param name="value">The <see cref="MaxDBParameter.Value"/> of the <see cref="MaxDBParameter"/> to set.</param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            this[parameterName] = (MaxDBParameter)value;
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return this.Clone();
        }

        /// <summary>
        /// Clone <see cref="MaxDBParameterCollection"/> object.
        /// </summary>
        /// <returns>The cloned <see cref="MaxDBParameterCollection"/> object.</returns>
        public MaxDBParameterCollection Clone()
        {
            var clone = new MaxDBParameterCollection();
            foreach (var param in this)
            {
                clone.Add(((ICloneable)param).Clone());
            }

            return clone;
        }

        #endregion
    }
}
