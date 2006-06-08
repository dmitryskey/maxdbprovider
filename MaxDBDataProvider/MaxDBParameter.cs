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
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
    public class MaxDBParameter :
#if NET20
        DbParameter,
#else
        IDbDataParameter, IDataParameter,
#endif // NET20
        ICloneable
    {
        internal MaxDBType m_dbType = MaxDBType.VarCharA;
        internal ParameterDirection m_direction = ParameterDirection.Input;
        private bool m_fNullable = false, m_fSourceNullable = false;
        private string m_sParamName;
        private string m_sSourceColumn;
        private int m_size;
        private byte m_precision, m_scale;
        private DataRowVersion m_sourceVersion = DataRowVersion.Current;
        internal object m_value;
        internal object m_inputValue;

        public MaxDBParameter()
        {
        }

        public MaxDBParameter(string parameterName, MaxDBType type)
        {
            m_sParamName = parameterName;
            m_dbType = type;
        }

        public MaxDBParameter(string parameterName, object val)
        {
            m_sParamName = parameterName;
            m_dbType = _inferType(Type.GetTypeCode(val.GetType()));
            Value = val;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size)
            : this(parameterName, type)
        {
            m_size = size;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size, string sourceColumn)
            : this(parameterName, type, size)
        {
            m_sSourceColumn = sourceColumn;
        }

        public MaxDBParameter(string parameterName, MaxDBType type, int size, ParameterDirection direction,
            bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object val)
            :
                this(parameterName, type, size, sourceColumn)
        {
            m_fNullable = isNullable;
            m_direction = direction;
            m_sourceVersion = sourceVersion;
            Value = val;
        }

#if NET20
        public override int Size
#else
		public int Size
#endif // NET20
        {
            get
            {
                return m_size;
            }
            set
            {
                m_size = value;
            }
        }

        private MaxDBType _inferType(TypeCode type)
        {
            switch (type)
            {
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DBNull:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INVALID_DATATYPE));

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
                    return MaxDBType.TimeStamp;

                case TypeCode.String:
                    return MaxDBType.StrUni;

                default:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_UNKNOWN_DATATYPE));
			}
        }

        #region ICloneable Members

        public object Clone()
        {
            return new MaxDBParameter(m_sParamName, m_dbType, m_size, m_direction, m_fNullable, 0, 0, m_sSourceColumn, m_sourceVersion, m_value);
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
                return m_direction;
            }
            set
            {
                m_direction = value;
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
                switch (m_dbType)
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
                    case MaxDBType.TimeStamp:
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
                        m_dbType = MaxDBType.CharB;
                        break;
                    case DbType.UInt16:
                    case DbType.UInt32:
                    case DbType.UInt64:
                    case DbType.Int64:
                        m_dbType = MaxDBType.Fixed;
                        break;
                    case DbType.Boolean:
                        m_dbType = MaxDBType.Boolean;
                        break;
                    case DbType.Int16:
                        m_dbType = MaxDBType.SmallInt;
                        break;
                    case DbType.Int32:
                        m_dbType = MaxDBType.Integer;
                        break;
                    case DbType.Single:
                        m_dbType = MaxDBType.Number;
                        break;
                    case DbType.Double:
                        m_dbType = MaxDBType.Number;
                        break;
                    case DbType.Decimal:
                        m_dbType = MaxDBType.Number;
                        break;
                    case DbType.Date:
                        m_dbType = MaxDBType.Date;
                        break;
                    case DbType.Time:
                        m_dbType = MaxDBType.Time;
                        break;
                    case DbType.DateTime:
                        m_dbType = MaxDBType.TimeStamp;
                        break;
                    case DbType.AnsiString:
                    case DbType.Guid:
                        m_dbType = MaxDBType.VarCharA;
                        break;
                    case DbType.String:
                        m_dbType = MaxDBType.VarCharUni;//?? unicode
                        break;
                    case DbType.StringFixedLength:
                        m_dbType = MaxDBType.CharE;//?? unicode
                        break;
                    default:
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSIONNETSQL, value.ToString(), string.Empty));
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
                return m_value == null ? DBNull.Value : m_value;
            }
            set
            {
                m_value = value;

                if (value == null || value == DBNull.Value)
                    m_inputValue = DBNull.Value;
                else
                {
                    switch (m_dbType)
                    {
                        case MaxDBType.Boolean:
                            m_inputValue = bool.Parse(value.ToString());
                            break;

                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            m_inputValue = double.Parse(value.ToString());
                            break;

                        case MaxDBType.Integer:
                            m_inputValue = int.Parse(value.ToString());
                            break;

                        case MaxDBType.SmallInt:
                            m_inputValue = short.Parse(value.ToString());
                            break;

                        default:
                            m_inputValue = value;
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
                return m_fNullable;
            }
            set
            {
                m_fNullable = value;
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
                return m_fSourceNullable;
            }
            set
            {
                m_fSourceNullable = value;
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
                return m_sourceVersion;
            }
            set
            {
                m_sourceVersion = value;
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
                return m_sParamName;
            }
            set
            {
                m_sParamName = value;
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
                return m_sSourceColumn;
            }
            set
            {
                m_sSourceColumn = value;
            }
        }

        public byte Precision
        {
            get
            {
                return m_precision;
            }
            set
            {
                m_precision = value;
            }
        }

        public byte Scale
        {
            get
            {
                return m_scale;
            }
            set
            {
                m_scale = value;
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

