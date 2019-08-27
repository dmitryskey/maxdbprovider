//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBParameter.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
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
    using System.Data;
    using System.Data.Common;
    using System.Globalization;

    /// <summary>
    /// Represents a parameter to a <see cref="MaxDBCommand"/>, and optionally, its mapping to <see cref="DataSet"/> columns.
    /// This class cannot be inherited.
    /// </summary>
    public sealed class MaxDBParameter : DbParameter, ICloneable
    {
        internal MaxDBType dbType = MaxDBType.VarCharA;
        internal ParameterDirection mDirection = ParameterDirection.Input;
        private bool bNullable;
        private bool bSourceNullable;
        private string strParamName;
        private string strSourceColumn;
        private int iSize;
        private DataRowVersion mSourceVersion = DataRowVersion.Current;
        internal object objValue;
        internal object objInputValue;

        /// <summary>
        /// Initializes a new instance of the <b>MaxDBParameter</b> class.
        /// </summary>
        public MaxDBParameter()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name and the data type.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="type">One of the <see cref="MaxDBType"/> values.</param>
        public MaxDBParameter(string parameterName, MaxDBType type)
        {
            this.strParamName = parameterName;
            this.dbType = type;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name,
        /// the <see cref="MaxDBType"/>, and the source column name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="type">One of the <see cref="MaxDBType"/> values.</param>
        /// <param name="sourceColumn">The name of the source column. </param>
        public MaxDBParameter(string parameterName, MaxDBType type, string sourceColumn)
            : this(parameterName, type)
        {
            this.strSourceColumn = sourceColumn;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name
        /// and a value of the new <b>MaxDBParameter</b>.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MaxDBParameter"/>.</param>
        public MaxDBParameter(string parameterName, object value)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            this.strParamName = parameterName;
            this.dbType = InferType(Type.GetTypeCode(value.GetType()));
            this.Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name,
        /// the <see cref="MaxDBType"/>, and the size.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="type">One of the <see cref="MaxDBType"/> values.</param>
        /// <param name="size">The length of the parameter.</param>
        public MaxDBParameter(string parameterName, MaxDBType type, int size)
            : this(parameterName, type) => this.iSize = size;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name,
        /// the <see cref="MaxDBType"/>, the size, and the source column name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="type">One of the <see cref="MaxDBType"/> values.</param>
        /// <param name="size">The length of the parameter. </param>
        /// <param name="sourceColumn">The name of the source column. </param>
        public MaxDBParameter(string parameterName, MaxDBType type, int size, string sourceColumn)
            : this(parameterName, type, size) => this.strSourceColumn = sourceColumn;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParameter"/> class with the parameter name,
        /// the type of the parameter, the size of the parameter, nullability of the parameter, a <see cref="ParameterDirection"/>,
        /// the precision of the parameter, the scale of the parameter, the source column, a <see cref="DataRowVersion"/> to use,
        /// and the value of the parameter.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map.</param>
        /// <param name="type">One of the <see cref="MaxDBType"/> values.</param>
        /// <param name="size">The length of the parameter.</param>
        /// <param name="direction">One of the <see cref="ParameterDirection"/> values.</param>
        /// <param name="isNullable">true if the value of the field can be null, otherwise false.</param>
        /// <param name="precision">The total number of digits to the left and right of the decimal point to which
        /// <see cref="MaxDBParameter.Value"/> is resolved.</param>
        /// <param name="scale">The total number of decimal places to which <see cref="MaxDBParameter.Value"/> is resolved.</param>
        /// <param name="sourceColumn">The name of the source column.</param>
        /// <param name="sourceVersion">One of the <see cref="DataRowVersion"/> values.</param>
        /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MaxDBParameter"/>.</param>
        public MaxDBParameter(string parameterName, MaxDBType type, int size, ParameterDirection direction,
            bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object value)
            : this(parameterName, type, size, sourceColumn)
        {
            this.bNullable = isNullable;
            this.mDirection = direction;
            this.mSourceVersion = sourceVersion;
            this.Value = value;
            this.Scale = scale;
            this.Precision = precision;
        }

        /// <summary>
        /// Gets or sets the length of the <b>MaxDBParameter</b>.
        /// </summary>
        public override int Size
        {
            get => this.iSize;
            set => this.iSize = value;
        }

        private static MaxDBType InferType(TypeCode type)
        {
            switch (type)
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALID_DATATYPE));

                case TypeCode.Object:
                    return MaxDBType.LongB;

                case TypeCode.Char:
                    return MaxDBType.CharB;

                case TypeCode.SByte:
                    return MaxDBType.CharA;

                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return MaxDBType.Fixed;

                case TypeCode.Boolean:
                    return MaxDBType.Boolean;

                case TypeCode.Byte:
                    return MaxDBType.CharB;

                case TypeCode.Int16:
                    return MaxDBType.SmallInt;

                case TypeCode.Int32:
                    return MaxDBType.Integer;

                case TypeCode.Int64:
                    return MaxDBType.Fixed;

                case TypeCode.Single:
                    return MaxDBType.Float;

                case TypeCode.Double:
                    return MaxDBType.Float;

                case TypeCode.Decimal:
                    return MaxDBType.Float;

                case TypeCode.DateTime:
                    return MaxDBType.Timestamp;

                case TypeCode.String:
                    return MaxDBType.StrUni;

                default:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.UNKNOWN_DATATYPE));
            }
        }

        #region ICloneable Members

        object ICloneable.Clone() => new MaxDBParameter(this.strParamName, this.dbType, this.iSize, this.mDirection, this.bNullable, 0, 0, this.strSourceColumn, this.mSourceVersion, this.objValue);

        #endregion

        #region IDbDataParameter Members

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional,
        /// or a stored procedure return value parameter.
        /// </summary>
        public override ParameterDirection Direction
        {
            get => this.mDirection;
            set => this.mDirection = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="DbType"/> of the parameter.
        /// </summary>
        public override DbType DbType
        {
            get
            {
                switch (this.dbType)
                {
                    case MaxDBType.Boolean:
                        return DbType.Boolean;

                    case MaxDBType.Fixed:
                    case MaxDBType.Float:
                    case MaxDBType.VFloat:
                    case MaxDBType.Number:
                    case MaxDBType.NoNumber:
                        return DbType.Double;

                    case MaxDBType.Integer:
                        return DbType.Int32;

                    case MaxDBType.SmallInt:
                        return DbType.Int16;

                    case MaxDBType.CharA:
                    case MaxDBType.CharE:
                    case MaxDBType.StrA:
                    case MaxDBType.StrE:
                    case MaxDBType.VarCharA:
                    case MaxDBType.VarCharE:
                        return DbType.AnsiString;

                    case MaxDBType.Date:
                        return DbType.Date;
                    case MaxDBType.Time:
                        return DbType.Time;
                    case MaxDBType.Timestamp:
                        return DbType.DateTime;

                    case MaxDBType.Unicode:
                    case MaxDBType.VarCharUni:
                    case MaxDBType.StrUni:
                        return DbType.String;

                    default:
                        return DbType.Binary;
                }
            }
            set
            {
                switch (value)
                {
                    case DbType.SByte:
                    case DbType.Byte:
                        this.dbType = MaxDBType.CharB;
                        break;
                    case DbType.UInt16:
                    case DbType.UInt32:
                    case DbType.UInt64:
                    case DbType.Int64:
                        this.dbType = MaxDBType.Fixed;
                        break;
                    case DbType.Boolean:
                        this.dbType = MaxDBType.Boolean;
                        break;
                    case DbType.Int16:
                        this.dbType = MaxDBType.SmallInt;
                        break;
                    case DbType.Int32:
                        this.dbType = MaxDBType.Integer;
                        break;
                    case DbType.Single:
                        this.dbType = MaxDBType.Number;
                        break;
                    case DbType.Double:
                        this.dbType = MaxDBType.Number;
                        break;
                    case DbType.Decimal:
                        this.dbType = MaxDBType.Number;
                        break;
                    case DbType.Date:
                        this.dbType = MaxDBType.Date;
                        break;
                    case DbType.Time:
                        this.dbType = MaxDBType.Time;
                        break;
                    case DbType.DateTime:
                        this.dbType = MaxDBType.Timestamp;
                        break;
                    case DbType.AnsiString:
                    case DbType.AnsiStringFixedLength:
                    case DbType.Guid:
                        this.dbType = MaxDBType.VarCharA;
                        break;
                    case DbType.String:
                        this.dbType = MaxDBType.VarCharUni;// ?? unicode
                        break;
                    case DbType.StringFixedLength:
                        this.dbType = MaxDBType.CharE;// ?? unicode
                        break;
                    default:
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONNETSQL, value.ToString(), string.Empty));
                }

            }
        }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        public override object Value
        {
            get
            {
                return this.objValue ?? DBNull.Value;
            }
            set
            {
                this.objInputValue = this.objValue = value;

                if (value == null)
                {
                    this.objInputValue = DBNull.Value;
                    return;
                }

                if (value != DBNull.Value)
                {
                    switch (this.dbType)
                    {
                        case MaxDBType.Boolean:
                            this.objInputValue = bool.Parse(value.ToString());
                            break;

                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            this.objInputValue = double.Parse(value.ToString(), CultureInfo.CurrentCulture);
                            break;

                        case MaxDBType.Integer:
                            this.objInputValue = int.Parse(value.ToString(), CultureInfo.CurrentCulture);
                            break;

                        case MaxDBType.SmallInt:
                            this.objInputValue = short.Parse(value.ToString(), CultureInfo.CurrentCulture);
                            break;

                        default:
                            this.objInputValue = value;
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter accepts null values.
        /// </summary>
        public override bool IsNullable
        {
            get => this.bNullable;
            set => this.bNullable = value;
        }

        /// <summary>
        /// Sets or gets a value which indicates whether the source column is nullable. This allows <see cref="MaxDBCommandBuilder"/>
        /// to correctly generate Update statements for nullable columns.
        /// </summary>
        public override bool SourceColumnNullMapping
        {
            get => this.bSourceNullable;
            set => this.bSourceNullable = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="DataRowVersion"/> to use when loading <see cref="Value"/>.
        /// </summary>
        public override DataRowVersion SourceVersion
        {
            get => this.mSourceVersion;
            set => this.mSourceVersion = value;
        }

        /// <summary>
        /// Gets or sets the name of the <b>MaxDBParameter</b>.
        /// </summary>
        public override string ParameterName
        {
            get => this.strParamName;
            set => this.strParamName = value;
        }

        /// <summary>
        /// Gets or sets the name of the source column that is mapped to the <see cref="DataSet"/> and used for loading or
        /// returning the <see cref="Value"/>.
        /// </summary>
        public override string SourceColumn
        {
            get => this.strSourceColumn;
            set => this.strSourceColumn = value;
        }

        /// <summary>
        /// Resets the type associated with this <b>MaxDBParameter</b>.
        /// </summary>
        public override void ResetDbType() => throw new NotImplementedException();

        #endregion
    }
}