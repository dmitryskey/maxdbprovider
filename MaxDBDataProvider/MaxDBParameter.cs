using System;
using System.Data;

namespace MaxDBDataProvider
{
	public class MaxDBParameter : IDataParameter
	{
		internal MaxDBType m_dbType  = MaxDBType.Fixed;
		internal ParameterDirection m_direction = ParameterDirection.Input;
		bool m_fNullable  = false;
		string m_sParamName;
		string m_sSourceColumn;
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

		public MaxDBParameter(string parameterName, object value)
		{
			m_sParamName = parameterName;
			Value = value;   
		}

		public MaxDBParameter( string parameterName, MaxDBType dbType, object p_value )
		{
			m_sParamName  = parameterName;
			m_dbType    = dbType;
			switch (m_dbType)
			{
				case MaxDBType.Boolean:
					m_value = (bool)p_value;
					break;
				case MaxDBType.CharA:
					m_value = (sbyte)p_value;
					break;
				case MaxDBType.CharB:
					m_value = (byte)p_value;
					break;
				case MaxDBType.Date:
				case MaxDBType.Time:
				case MaxDBType.TimeStamp:
					m_value = (DateTime)p_value;
					break;
				case MaxDBType.Fixed:
				case MaxDBType.Float:
				case MaxDBType.VFloat:
					m_value = (double)p_value;
					break;
				case MaxDBType.Integer:
					m_value = (int)p_value;
					break;
				case MaxDBType.SmallInt:
					m_value = (short)p_value;
					break;
				case MaxDBType.StrA:
				case MaxDBType.StrB:
				case MaxDBType.VarCharA:
				case MaxDBType.VarCharB:
				case MaxDBType.Unicode:
				case MaxDBType.VarCharUni:
				case MaxDBType.StrUni:
					m_value = (string)p_value;
					break;
				default:
					m_value = p_value;
					break;
			}
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
					case MaxDBType.TimeStamp:
						return DbType.DateTime;

					case MaxDBType.Unicode:
					case MaxDBType.VarCharUni:
					case MaxDBType.StrUni:
						return DbType.String;

					default:
						throw new MaxDBException("Parameter is of unknown data type");
				}
			}
			set  { m_dbType = this._inferDbType(value); }
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

		private MaxDBType _inferDbType(DbType type)
		{
			switch (type)
			{
				case DbType.Object:
					throw new MaxDBException("Invalid data type");

				case DbType.SByte:
					return MaxDBType.CharA;

				case DbType.UInt16:
				case DbType.UInt32:
				case DbType.UInt64:
					return MaxDBType.Fixed;

				case DbType.Boolean:
					return MaxDBType.Boolean;

				case DbType.Byte:
					return MaxDBType.CharB;

				case DbType.Int16:
					return MaxDBType.SmallInt;

				case DbType.Int32:
					return MaxDBType.Integer;

				case DbType.Int64:
					return MaxDBType.Fixed;

				case DbType.Single:
					return MaxDBType.Float;

				case DbType.Double:
					return MaxDBType.Float;

				case DbType.Decimal:
					return MaxDBType.Float;

				case DbType.DateTime:
					return MaxDBType.TimeStamp;

				case DbType.String:
					return MaxDBType.StrUni;

				default:
					throw new MaxDBException("Value is of unknown data type");
			}
		}

	}
}

