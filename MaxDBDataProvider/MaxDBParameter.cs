using System;
using System.Data;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	public class MaxDBParameter : IDataParameter
	{
		internal MaxDBType m_dbType = MaxDBType.VarCharA;
		internal ParameterDirection m_direction = ParameterDirection.Input;
		bool m_fNullable  = false;
		string m_sParamName;
		string m_sSourceColumn;
		int m_size;
		DataRowVersion m_sourceVersion = DataRowVersion.Current;
		object m_value;

		public MaxDBParameter()
		{
		}

		public MaxDBParameter(string parameterName, MaxDBType type)
		{
			m_sParamName = parameterName;
			m_dbType   = type;
		}

		public MaxDBParameter(string parameterName, object val)
		{
			m_sParamName = parameterName;
			m_value = val;   
		}

		public MaxDBParameter(string parameterName, MaxDBType type, int size) : this(parameterName, type)
		{
			m_size = size;
		}

		public MaxDBParameter(string parameterName, MaxDBType type, int size, string sourceColumn) : this(parameterName, type, size)
		{
			m_sSourceColumn = sourceColumn;
		}

		public MaxDBParameter(string parameterName, MaxDBType type, int size, ParameterDirection direction,
			bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object val) : 
				this (parameterName, type, size, sourceColumn)
		{
			m_fNullable = isNullable;
			m_direction = direction;
			m_sourceVersion = sourceVersion;
			m_value = val;
		}

		public DbType DbType 
		{
			get  
			{ 
				switch (m_dbType)
				{
					case MaxDBType.Boolean:
						return DbType.Boolean;

					case MaxDBType.CharA:
						return DbType.SByte;

					case MaxDBType.CharB:
						return DbType.Byte;

					case MaxDBType.Date:
						return DbType.Date;

					case MaxDBType.Fixed:
					case MaxDBType.Float:
					case MaxDBType.VFloat:
						return DbType.Double;

					case MaxDBType.Integer:
						return DbType.Int32;

					case MaxDBType.SmallInt:
						return DbType.Int16;

					case MaxDBType.StrA:
					case MaxDBType.StrB:
					case MaxDBType.VarCharA:
					case MaxDBType.VarCharB:
						return DbType.AnsiString;

					case MaxDBType.Time:
						return DbType.Time;
					case MaxDBType.TimeStamp:
						return DbType.DateTime;

					case MaxDBType.Unicode:
					case MaxDBType.VarCharUni:
					case MaxDBType.StrUni:
						return DbType.String;

					default:
						throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONSQLNET, 
							DataType.stringValues[(int)m_dbType], string.Empty));
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
						throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONNETSQL, value.ToString(), string.Empty));
				}

			}
		}

		public ParameterDirection Direction 
		{
			get { return m_direction; }
			set { m_direction = value; }
		}

		public Boolean IsNullable 
		{
			get { return m_fNullable; }
		}

		public String ParameterName 
		{
			get { return m_sParamName; }
			set { m_sParamName = value; }
		}

		public String SourceColumn 
		{
			get { return m_sSourceColumn; }
			set { m_sSourceColumn = value; }
		}

		public DataRowVersion SourceVersion 
		{
			get { return m_sourceVersion; }
			set { m_sourceVersion = value; }
		}

		public object Value 
		{
			get
			{
				return m_value;
			}
			set
			{
				m_value    = value;
				m_dbType  = _inferType(Type.GetTypeCode(value.GetType()));
			}
		}

		private MaxDBType _inferType(TypeCode type)
		{
			switch (type)
			{
				case TypeCode.Empty:
				case TypeCode.Object:
				case TypeCode.DBNull:
					throw new MaxDBException("Invalid data type");

				case TypeCode.Char:
					return MaxDBType.Unicode;

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
					throw new MaxDBException("Value is of unknown data type");
			}
		}
	}
}

