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
using System.Data.Common;
using MaxDB.Data.MaxDBProtocol;
using System.Globalization;

namespace MaxDB.Data
{
    public class MaxDBParameter :
#if NET20
        DbParameter,
#else
        IDbDataParameter, IDataParameter,
#endif // NET20
        ICloneable
    {
        internal MaxDBType dbType = MaxDBType.VarCharA;
        internal ParameterDirection mDirection = ParameterDirection.Input;
        private bool bNullable, bSourceNullable;
        private string strParamName;
        private string strSourceColumn;
        private int iSize;
        private byte byPrecision, byScale;
        private DataRowVersion mSourceVersion = DataRowVersion.Current;
        internal object objValue;
        internal object objInputValue;

        public MaxDBParameter()
        {
        }

        public MaxDBParameter(string parameterName, MaxDBType type)
        {
            strParamName = parameterName;
            dbType = type;
        }

        public MaxDBParameter(string parameterName, object value)
        {
            if (value == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

            strParamName = parameterName;
            dbType = _inferType(Type.GetTypeCode(value.GetType()));
            Value = value;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size)
            : this(parameterName, type)
        {
            iSize = size;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size, string sourceColumn)
            : this(parameterName, type, size)
        {
            strSourceColumn = sourceColumn;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size, ParameterDirection direction,
            bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object value)
            : this(parameterName, type, size, sourceColumn)
        {
            bNullable = isNullable;
            mDirection = direction;
            mSourceVersion = sourceVersion;
            Value = value;
            Scale = scale;
            Precision = precision;
        }

#if NET20
        public override int Size
#else
		public int Size
#endif // NET20
        {
            get
            {
                return iSize;
            }
            set
            {
                iSize = value;
            }
        }

        private static MaxDBType _inferType(TypeCode type)
        {
            switch (type)
            {
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DBNull:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALID_DATATYPE));

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

        public object Clone()
        {
            return new MaxDBParameter(strParamName, dbType, iSize, mDirection, bNullable, 0, 0, strSourceColumn, mSourceVersion, objValue);
        }

        #endregion

        #region IDbDataParameter Members

#if NET20
        public override ParameterDirection Direction
#else
		public ParameterDirection Direction
#endif // NET20
        {
            get
            {
                return mDirection;
            }
            set
            {
                mDirection = value;
            }
        }

#if NET20
        public override DbType DbType
#else
		public DbType DbType
#endif // NET20
        {
            get
            {
                switch (dbType)
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
                        dbType = MaxDBType.CharB;
                        break;
                    case DbType.UInt16:
                    case DbType.UInt32:
                    case DbType.UInt64:
                    case DbType.Int64:
                        dbType = MaxDBType.Fixed;
                        break;
                    case DbType.Boolean:
                        dbType = MaxDBType.Boolean;
                        break;
                    case DbType.Int16:
                        dbType = MaxDBType.SmallInt;
                        break;
                    case DbType.Int32:
                        dbType = MaxDBType.Integer;
                        break;
                    case DbType.Single:
                        dbType = MaxDBType.Number;
                        break;
                    case DbType.Double:
                        dbType = MaxDBType.Number;
                        break;
                    case DbType.Decimal:
                        dbType = MaxDBType.Number;
                        break;
                    case DbType.Date:
                        dbType = MaxDBType.Date;
                        break;
                    case DbType.Time:
                        dbType = MaxDBType.Time;
                        break;
                    case DbType.DateTime:
                        dbType = MaxDBType.Timestamp;
                        break;
                    case DbType.AnsiString:
                    case DbType.Guid:
                        dbType = MaxDBType.VarCharA;
                        break;
                    case DbType.String:
                        dbType = MaxDBType.VarCharUni;//?? unicode
                        break;
                    case DbType.StringFixedLength:
                        dbType = MaxDBType.CharE;//?? unicode
                        break;
                    default:
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONNETSQL, value.ToString(), string.Empty));
                }

            }
        }

#if NET20
        public override object Value
#else
		public object Value
#endif // NET20
        {
            get
            {
                return objValue == null ? DBNull.Value : objValue;
            }
            set
            {
                objInputValue = objValue = value;

                if (value == null)
                {
                    objInputValue = DBNull.Value;
                    return;
                }

                if (value != DBNull.Value)
                {
                    switch (dbType)
                    {
                        case MaxDBType.Boolean:
                            objInputValue = bool.Parse(value.ToString());
                            break;

                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            objInputValue = double.Parse(value.ToString(), CultureInfo.InvariantCulture);
                            break;

                        case MaxDBType.Integer:
                            objInputValue = int.Parse(value.ToString(), CultureInfo.InvariantCulture);
                            break;

                        case MaxDBType.SmallInt:
                            objInputValue = short.Parse(value.ToString(), CultureInfo.InvariantCulture);
                            break;

                        default:
                            objInputValue = value;
                            break;
                    }
                }
            }
        }

#if NET20
        public override bool IsNullable
#else
		public bool IsNullable
#endif // NET20
        {
            get
            {
                return bNullable;
            }
            set
            {
                bNullable = value;
            }
        }

#if NET20
        public override bool SourceColumnNullMapping
#else
		public bool SourceColumnNullMapping 
#endif // NET20
        {
            get
            {
                return bSourceNullable;
            }
            set
            {
                bSourceNullable = value;
            }
        }

#if NET20
        public override DataRowVersion SourceVersion
#else
		public DataRowVersion SourceVersion
#endif // NET20
        {
            get
            {
                return mSourceVersion;
            }
            set
            {
                mSourceVersion = value;
            }
        }

#if NET20
        public override string ParameterName
#else
		public string ParameterName
#endif // NET20
        {
            get
            {
                return strParamName;
            }
            set
            {
                strParamName = value;
            }
        }

#if NET20
        public override string SourceColumn
#else
        public string SourceColumn
#endif // NET20
        {
            get
            {
                return strSourceColumn;
            }
            set
            {
                strSourceColumn = value;
            }
        }

#if NET20 && MONO
        public override byte Precision
#else
        public byte Precision
#endif // NET20 && MONO
        {
            get
            {
                return byPrecision;
            }
            set
            {
                byPrecision = value;
            }
        }

#if NET20 && MONO
        public override byte Scale
#else
        public byte Scale
#endif // NET20 && MONO
        {
            get
            {
                return byScale;
            }
            set
            {
                byScale = value;
            }
        }

#if NET20
        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }
#endif // NET20

        #endregion
    }
}

