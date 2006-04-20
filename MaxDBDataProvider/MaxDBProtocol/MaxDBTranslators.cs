using System;
using System.IO;
using System.Text;
using System.Threading;
using System.ComponentModel;
using System.Globalization;
using System.Data;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE

	#region "DB Tech translator class"

	internal abstract class DBTechTranslator
	{
		protected int m_logicalLength;
		protected int m_physicalLength;
		protected int m_bufpos;   // bufpos points to actual data, defbyte is at -1
		protected byte m_mode;
		protected byte m_ioType;
		protected byte m_dataType;
		protected bool m_writeAllowed = false;
		protected string m_charDatatypePostfix = "";
		private string m_colName;
		private int m_colIndex;

		public const int nullDefineByte = 1;
		public const int specialNullValueDefineByte = 2;
		public const int unknownDefineByte = -1;

		protected DBTechTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
		{
			m_mode = (byte) mode;
			m_ioType = (byte) ioType;
			m_dataType = (byte) dataType;
			m_logicalLength = len;
			m_physicalLength = ioLen;
			m_bufpos = bufpos;
		}

		public void AllowWrites() 
		{
			m_writeAllowed = true;
		}

		public int BufPos 
		{
			get
			{
				return m_bufpos;
			}
		}

		public virtual int Precision
		{
			get
			{
				return m_logicalLength;
			}
		}

		public virtual int Scale
		{
			get
			{
				return 0;
			}
		}

		public int PhysicalLength
		{
			get
			{
				return m_physicalLength;
			}
		}

		public int ColumnIndex 
		{
			get
			{
				return m_colIndex;
			}
			set
			{
				m_colIndex = value;
			}
		}

		public string ColumnTypeName 
		{
			get
			{
				return GeneralColumnInfo.GetTypeName(m_dataType);
			}
		}

		public Type ColumnDataType
		{
			get
			{
				return GeneralColumnInfo.GetType(m_dataType);
			}
		}

		public MaxDBType ColumnProviderType
		{
			get
			{
				return GeneralColumnInfo.GetMaxDBType(m_dataType);
			}
		}

		public virtual int ColumnDisplaySize
		{
			get
			{
				return m_logicalLength;
			}
		}

		public string ColumnName
		{
			get
			{
				return m_colName;
			}
			set
			{
				m_colName = value;
			}
		}

		protected bool IsUnicodeColumn 
		{
			get
			{
				switch (m_dataType) 
				{
					case DataType.STRUNI:
					case DataType.LONGUNI:
						return true;
					default:
						return false;
				}
			}
		}

		protected MaxDBConversionException CreateGetException(string requestedType)
		{
			return new MaxDBConversionException(MaxDBMessages.Extract(
				MaxDBMessages.ERROR_CONVERSIONSQLNET, ColumnTypeName, requestedType));
		}

		protected MaxDBConversionException CreateSetException(string requestedType)
		{
			return new MaxDBConversionException(MaxDBMessages.Extract(
				MaxDBMessages.ERROR_CONVERSIONNETSQL, requestedType, ColumnTypeName));
		}

		protected MaxDBConversionException CreateParseException(string data, string requestedType)
		{
			if (requestedType == null) 
				requestedType = ColumnTypeName;
			return new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSIONDATA, data, requestedType));
		}

		public virtual Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw CreateGetException("ASCIIStream");
		}

		public virtual Stream GetUnicodeStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw CreateGetException("UnicodeStream");
		}

		public virtual Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw CreateGetException("BinaryStream");
		}

		public virtual bool GetBoolean(ByteArray mem)
		{
			throw CreateGetException("bool");
		}

		public virtual byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			throw CreateGetException("byte");
		}

		public virtual byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			throw CreateGetException("byte[]");
		}

		public virtual long GetBytes(ISQLParamController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
		{
			byte[] bytes = GetBytes(controller, mem);
			if (bytes == null) return 0;
			Array.Copy(bytes, fldOffset, buffer, bufferoffset, bytes.Length);
			return bytes.Length;
		}

		public virtual DateTime GetDateTime(ByteArray mem)
		{
			throw this.CreateGetException("DateTime");
		}

		public virtual double GetDouble(ByteArray mem)
		{
			throw CreateGetException("double");
		}

		public virtual float GetFloat(ByteArray mem)
		{
			throw CreateGetException("float");
		}

		public virtual BigDecimal GetBigDecimal(ByteArray mem)
		{
			throw CreateGetException("decimal");
		}

		public virtual decimal GetDecimal(ByteArray mem)
		{
			throw CreateGetException("decimal");
		}

		public virtual short GetInt16(ByteArray mem)
		{
			throw CreateGetException("Int16");
		}

		public virtual int GetInt32(ByteArray mem)
		{
			throw CreateGetException("Int32");
		}

		public virtual long GetInt64(ByteArray mem)
		{
			throw CreateGetException("Int64");
		}

		public virtual object GetValue(ISQLParamController controller, ByteArray mem)
		{
			throw CreateGetException("object");
		}

		public virtual object[] GetValues(ByteArray mem)
		{
			throw CreateGetException("object[]");
		}

		public virtual string GetString(ISQLParamController controller, ByteArray mem)
		{
			object rawResult = GetValue(controller, mem);

			if (rawResult == null) 
				return null;
			else 
				return rawResult.ToString();
		}

		public virtual long GetChars(ISQLParamController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
		{
			string str = GetString(controller, mem);
			if (str == null) return 0;
			char[] chars = str.Substring((int)fldOffset, length).ToCharArray();
			Array.Copy(chars, 0, buffer, bufferoffset, chars.Length);
			return chars.Length;
		}

		public virtual bool IsCaseSensitive 
		{
			get
			{
				return false;
			}
		}

		public virtual bool IsCurrency
		{
			get
			{
				return false;
			}
		}

		public virtual bool IsDefinitelyWritable
		{
			get
			{
				return false;
			}
		}

		public virtual bool IsInput 
		{
			get
			{
				return (m_ioType != ParamInfo.Output);
			}
		}

		public virtual bool IsOutput 
		{
			get
			{
				return (m_ioType != ParamInfo.Input);
			}
		}

		public virtual bool IsLongKind 
		{
			get
			{
				return GeneralColumnInfo.IsLong(m_dataType);
			}
		}

		public bool IsDBNull(ByteArray mem) 
		{
			return (mem.ReadByte(m_bufpos - 1) == 0xFF);
		}

		public int CheckDefineByte(ByteArray mem) 
		{
			int defByte = mem.ReadByte(m_bufpos - 1);
			switch(defByte)
			{
				case -1:
					return nullDefineByte;
				case -2:
					return specialNullValueDefineByte;
				default:
					return unknownDefineByte;
			}
		}

		public bool IsNullable 
		{
			get
			{
				if ((m_mode & ParamInfo.Mandatory) != 0) 
					return false;

				if ((m_mode & ParamInfo.Optional) != 0) 
					return true;
				
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_DBNULL_UNKNOWN));
			}
		}

		public bool IsSearchable 
		{
			get
			{
				return true;
			}
		}

		public bool IsSigned 
		{
			get
			{
				return false;
			}
		}

		public bool IsWritable
		{
			get
			{
				return m_writeAllowed;
			}
		}

		public bool IsStreamKind
		{
			get
			{
				return false;
			}
		}

		public void Put(DataPart dataPart, object data)
		{
			if (m_ioType != ParamInfo.Output) 
			{
				if (data == null) 
					dataPart.WriteNull(m_bufpos, m_physicalLength - 1);
				else 
				{
					PutSpecific(dataPart, data);
					dataPart.AddArg(m_bufpos, m_physicalLength - 1);
				}
			}
		}

		public void PutProcOutput(DataPart dataPart, object data)
		{
			if (m_ioType != ParamInfo.Input) 
			{
				if (data == null) 
					dataPart.WriteNull(m_bufpos, m_physicalLength - 1);
				else 
				{
					PutSpecific(dataPart, data);
					dataPart.AddArg(m_bufpos, m_physicalLength - 1);
				}
			}
		}

		protected void CheckFieldLimits(int byteLength)
		{
			if (byteLength > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], m_colIndex + 1);
		}

		protected abstract void PutSpecific(DataPart dataPart, object data);
		protected abstract object TransSpecificForInput(object obj);

		public virtual object TransObjectForInput(object val)
		{
			object result;
			if (val == null) 
				return null;
			
			result = TransSpecificForInput(val);
			if (result != null) 
				return result;
			
			if (val is string) 
				return TransStringForInput((string) val);

			if (val is BigDecimal) 
				return TransStringForInput(VDNNumber.BigDecimal2PlainString((BigDecimal)val));

			if (val.GetType().IsArray) 
			{
				if (val is byte[])
					return TransBytesForInput((byte[]) val);
					
				if (val is char[])
					return TransStringForInput (new String ((char[]) val));
	
				// cannot convert other arrays
				throw CreateSetException(val.GetType().FullName);
			}
			else 
				// default conversion to string
				return TransStringForInput(val.ToString());
		}

		public virtual object TransCharacterStreamForInput(Stream stream, int length)
		{
			return TransObjectForInput(stream);
		}

		public virtual object TransBinaryStreamForInput(Stream stream, int length)
		{
			return TransObjectForInput(stream);
		}

		public virtual object TransBigDecimalForInput(BigDecimal bigDecimal)
		{
			return TransObjectForInput(bigDecimal);
		}

		public virtual object TransBooleanForInput(bool val)
		{
			return TransInt32ForInput(val ? 1 : 0);
		}

		public virtual object TransByteForInput(byte val)
		{
			return TransObjectForInput(new BigDecimal(val));
		}

		public virtual object TransBytesForInput(byte[] val)
		{
			throw CreateGetException("Bytes");
		}

		public virtual object TransDateTimeForInput(DateTime val)
		{
			return TransObjectForInput(val);
		}

		public virtual object TransDoubleForInput(double val)
		{
			if(double.IsInfinity(val) || double.IsNaN(val)) 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));

			return TransObjectForInput(new BigDecimal(val));
		}
 
		public virtual object TransFloatForInput(float val)
		{
			if(float.IsInfinity(val) || float.IsNaN(val)) 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));

			return TransObjectForInput(new BigDecimal(val));
		}

		public virtual object TransInt16ForInput(short val)
		{
			return TransObjectForInput(new BigDecimal(val));
		}
 
		public virtual object TransInt32ForInput(int val)
		{
			return TransObjectForInput(new BigDecimal(val));
		}

		public virtual object TransInt64ForInput(long val)
		{
			return TransObjectForInput(new BigDecimal(val));
		}
	
		public virtual object TransStringForInput(string val)
		{
			throw CreateSetException("String");
		}

		public byte BigDecimal2Byte(BigDecimal bd)
		{
			return (bd == null ? (byte)0 : (byte)bd); 
		}

		public double BigDecimal2Double(BigDecimal bd)
		{
			return (bd == null ? 0.0 : (double)bd); 		
		}

		public float BigDecimal2Float(BigDecimal bd)
		{
			return (bd == null ? 0 : (float)bd); 
		}

		public decimal BigDecimal2Decimal(BigDecimal bd)
		{
			return (bd == null ? 0 : (decimal)bd); 
		}

		public short BigDecimal2Int16(BigDecimal bd)
		{
			return (bd == null ? (short)0 : (short)bd); 
		}

		public int BigDecimal2Int32(BigDecimal bd)
		{
			return (bd == null ? 0 : (int)bd); 
		}

		public long BigDecimal2Int64(BigDecimal bd)
		{
			return (bd == null ? 0 : (long)bd); 
		}

		public virtual void SetProcParamInfo(DBProcParameterInfo info)
		{
		}
	}

	#endregion

	#region "Character data translator class"

	internal abstract class CharDataTranslator : DBTechTranslator
	{
		protected static char[] HighTime = { '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3'};
		protected static char[] LowTime  = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1'};

		protected CharDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			m_charDatatypePostfix = " ASCII";
		}

		public override bool IsCaseSensitive 
		{
			get
			{
				return true;
			}
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = Encoding.GetEncoding(1251).GetBytes(data.ToString());
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte((byte) ' ', m_bufpos - 1);
			dataPart.WriteASCIIBytes(bytes, m_bufpos, m_physicalLength - 1);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			String strValue = GetString(null, mem);
			if (strValue == null) 
				return DateTime.MinValue;
			try 
			{
				return DateTime.Parse(strValue);
			}
			catch 
			{
				throw CreateParseException(strValue, "DateTime");
			}
		}
	}

	#endregion

	#region "String data translator class"

	internal class StringTranslator : CharDataTranslator
	{
		public StringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) : base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetString(controller, mem);
		}

		public override BigDecimal GetBigDecimal(ByteArray mem)
		{
			string val = GetString(null, mem);

			if (val == null) 
				return new BigDecimal(0);
			try 
			{
				return new BigDecimal(val.Trim());
			}
			catch
			{
				throw CreateParseException(val, "BigDecimal");
			}
		}

		public override bool GetBoolean(ByteArray mem)
		{
			string val = GetString(null, mem);

			if (val == null) 
				return false;

			try 
			{
				return (int.Parse(val.Trim()) != 0);
			}
			catch
			{
				throw CreateParseException(val, "Boolean");
			}
		}

		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			string result = GetString(controller, mem);
			if (result != null)
				return Encoding.Unicode.GetBytes(result);
			else
				return null;
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			byte[] bytes = GetBytes(controller, mem);
			if (bytes == null || bytes.Length == 0)
				return 0;
			else
				return bytes[0];
		}

		public override Stream GetUnicodeStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			string asString = GetString(controller, mem);

			if (asString == null) 
				return null;
        
			return new MemoryStream(Encoding.Unicode.GetBytes(asString));
		}

		public override double GetDouble(ByteArray mem)
		{
			return BigDecimal2Double(GetBigDecimal(mem));
		}

		public override float GetFloat(ByteArray mem)
		{
			return BigDecimal2Float(GetBigDecimal(mem));
		}

		public override decimal GetDecimal(ByteArray mem)
		{
			return BigDecimal2Decimal(GetBigDecimal(mem));
		}

		public override short GetInt16(ByteArray mem)
		{
			return BigDecimal2Int16(GetBigDecimal(mem));
		}

		public override int GetInt32(ByteArray mem)
		{
			return BigDecimal2Int32(GetBigDecimal(mem));
		}

		public override long GetInt64(ByteArray mem)
		{
			return BigDecimal2Int64(GetBigDecimal(mem));
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return mem.ReadASCII(m_bufpos, m_logicalLength);
			else
				return null;
		}

		public override object TransBytesForInput(byte[] val) 
		{
			if (val == null) 
				return null;
			else 
				return TransStringForInput(Encoding.GetEncoding(1252).GetString(val));
     
		}

		public override object TransCharacterStreamForInput(Stream stream, int length)
		{
			if(length <= 0) 
				return null;
			try 
			{
				byte[] ba = new byte[length];
				int r = stream.Read(ba, 0, length);
				if(r != length) 
				{
					if(r == -1) 
						r=0;
					byte[] ba2 = ba;
					ba = new byte[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				return TransStringForInput(Encoding.GetEncoding(1252).GetString(ba));
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}
		}

		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			if(length <= 0) 
				return null;
        
			try 
			{
				byte[] ba = new byte[length];
				int r = stream.Read(ba, 0, length);
				if(r != length) 
				{
					if (r == -1) r = 0;
					byte[] ba2 = ba;
					ba = new byte[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				return TransBytesForInput(ba);
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}    
		}

		protected override object TransSpecificForInput(object obj)
		{
			// conversion to string handled by super.putObject ()
			return null;
		}

		/**
		 * Performs specific string checks for string insert (length check).
		 * (The string is not inserted here, but will be used unmodified on
		 *  packet creation later).
		 * @param arg the String to insert
		 * @return <code>arg</code> unmodified.
		 */
		public override object TransStringForInput(string arg)
		{
			if (arg == null) 
				return null;
        
			byte[] bytes = Encoding.GetEncoding(1252).GetBytes(arg);
			CheckFieldLimits(bytes.Length);
			return arg;
		}
	}

	#endregion

	#region "Unicode string data translator class"

	internal class UnicodeStringTranslator : StringTranslator
	{
		private Encoding m_enc; 

		public UnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			m_charDatatypePostfix = " UNICODE";
			m_enc = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return m_enc.GetString(mem.ReadBytes(m_bufpos, m_logicalLength * Consts.UnicodeWidth));
			else
				return null;
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetString(controller, mem);
		}
     
		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			string result = GetString(controller, mem);
			if (result != null)
				return Encoding.Unicode.GetBytes(result);
			else
				return null;
		}

		public override object TransBytesForInput(byte[] val)
		{
			if (val == null)
				return val;
        
			CheckFieldLimits(val.Length);
			return val;
		}

		public override object TransStringForInput(string arg)
		{
			if (arg == null) 
				return null;
        
			byte[] bytes = m_enc.GetBytes(arg);
			CheckFieldLimits (bytes.Length);
			return bytes;
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			dataPart.WriteDefineByte ((byte) 1, m_bufpos - 1);
			dataPart.WriteUnicodeBytes ((byte []) data, m_bufpos, m_physicalLength - 1);
		}
	}

	#endregion

	#region "Space option string data translator class"

	internal class SpaceOptionStringTranslator : StringTranslator
	{
		public SpaceOptionStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			string result = null;

			if (!IsDBNull(mem)) 
			{
				result = base.GetString(controller, mem).TrimStart();
				if (result.Length == 0)
					result = " ";
			}
			return result;
		}
	}

	#endregion

	#region "Space option unicode string data translator class"

	internal class SpaceOptionUnicodeStringTranslator : UnicodeStringTranslator
	{
		public SpaceOptionUnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode) :
			base(mode, ioType, dataType, len, ioLen, bufpos, swapMode)
		{
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			string result = null;

			if (!IsDBNull(mem)) 
			{
				result = base.GetString(controller, mem).TrimStart();
				if (result.Length == 0)
					result = " ";
			}
			return result;
		}
	}

	#endregion

	#region "Binary data translator class"

	internal abstract class BinaryDataTranslator : DBTechTranslator 
	{
		public BinaryDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		protected override void PutSpecific (DataPart dataPart, object data)
		{
			byte[] bytes = (byte[]) data;
			dataPart.WriteDefineByte(0, m_bufpos - 1);
			dataPart.WriteBytes(bytes, m_bufpos, m_physicalLength - 1);
		}
	}

	#endregion

	#region "Bytes data translator class"

	internal class BytesTranslator : BinaryDataTranslator 
	{
		public BytesTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem))
				return 0;
			else
				result = mem.ReadBytes(m_bufpos, 1);
			return result[0];
		}
    
		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			if (IsDBNull(mem))
				return null;
			else
				return mem.ReadBytes(m_bufpos, 1);
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetBytes(controller, mem);
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			byte[] rawResult;

			rawResult = GetBytes(null, mem);
			if (rawResult == null) 
				return null;
			else 
				return Encoding.Unicode.GetString(rawResult);
		}

		public override object TransByteForInput(byte val)
		{
			byte[] barr=new byte[1];
			barr[0]=val;
			return TransBytesForInput(barr);
		}

		public override object TransBytesForInput(byte[] arg)
		{
			if (arg == null) 
				return null;
        
			CheckFieldLimits(arg.Length);
			return arg;
		}

		protected override object TransSpecificForInput(object obj)
		{
			if (obj is byte[]) 
				return TransBytesForInput((byte[]) obj);
			else
				return null;
		}

		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
			else 
				return TransBytesForInput(Encoding.Unicode.GetBytes(val));
		}

		public object TransUnicodeStreamForInput(TextReader stream, int length)
		{
			if(length <= 0) 
				return null;
        
			try 
			{
				char[] ba = new char[length];
				int r = stream.Read(ba, 0, length);
				if(r != length) 
				{
					if(r == -1) r=0;
					char[] ba2 = ba;
					ba = new char[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				return TransStringForInput(new string(ba));
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}
		}

		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			if (length <= 0) 
				return null;
        
			try 
			{
				byte[] ba = new byte[length];
				int r = stream.Read(ba, 0, length);
				if (r != length) 
				{
					if (r == -1) r=0;
					byte[] ba2 = ba;
					ba = new byte[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				return TransBytesForInput(ba);
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}    
		}

		public override Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			byte[] asBytes = GetBytes(null, mem);

			if (asBytes == null) 
				return null;
        
			return new MemoryStream(asBytes);
		}
	}

	#endregion

	#region "Boolean data translator class"

	internal class BooleanTranslator : BinaryDataTranslator 
	{
		public BooleanTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override BigDecimal GetBigDecimal(ByteArray mem)
		{
			if (IsDBNull(mem)) 
				return null;

			if (GetBoolean(mem)) 
				return new BigDecimal(1);
			else 
				return new BigDecimal(0);
		}

		public override bool GetBoolean(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
				return (mem.ReadByte(m_bufpos) == 0x00 ? false: true); 
			else
				return false;
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			return GetBoolean(mem) ? (byte) 1 : (byte) 0;
		}

		public override float GetFloat(ByteArray mem)
		{
			return GetByte(null, mem);
		}

		public override double GetDouble(ByteArray mem)
		{
			return GetByte(null, mem);
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return GetBoolean(mem);
			else
				return null;
		}

		public override int GetInt32(ByteArray mem)
		{
			return GetByte(null, mem);
		}

		public override long GetInt64(ByteArray mem)
		{
			return GetByte(null, mem);
		}

		public override short GetInt16(ByteArray mem)
		{
			return GetByte(null, mem);
		}

		public override object TransBooleanForInput(bool newValue) 
		{
			byte[] result = new byte[1];

			result [0] = (newValue ? (byte) 1 : (byte) 0);
			return result;
		}

		protected override object TransSpecificForInput(object obj)
		{
			if (obj is bool || obj is byte || obj is short || obj is int ||
				obj is long || obj is ushort || obj is uint || obj is ulong ||
				obj is float || obj is double || obj is decimal)
				return TransBooleanForInput((bool) obj);
			else
				throw CreateSetException(obj.GetType().FullName);

		}

		public override object TransStringForInput (string val) 
		{
			if (val == null) 
				return null;

			return TransBooleanForInput(bool.Parse(val));
		}
	}

	#endregion

	#region "Numeric data translator"

	internal class NumericTranslator : BinaryDataTranslator
	{
		protected int frac;
		protected bool isFloatingPoint = false;

		public NumericTranslator(int mode, int ioType, int dataType, int len, int frac, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			switch (dataType) 
			{
				case DataType.FLOAT:
				case DataType.VFLOAT:
					// more digits are unreliable anyway
					frac = 38;
					isFloatingPoint=true;
					break;
				default:
					this.frac = frac;
					break;
			}
        
		}

		public BigDecimal GetBigDecimal(int scale, ByteArray mem)
		{
			BigDecimal result = null;
			try 
			{
				switch (CheckDefineByte(mem)) 
				{
					case (DBTechTranslator.nullDefineByte):
						return result;
					case (DBTechTranslator.specialNullValueDefineByte):
						throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSIONSpecialNullValue), "", -10811);
				}
				result = VDNNumber.Number2BigDecimal(mem.ReadBytes(m_bufpos, m_physicalLength - 1));
				result = result.setScale(scale);
				return result;
			} 
			catch
			{
				throw CreateParseException(result + " scale: " + scale, null);
			}
		}

		public override BigDecimal GetBigDecimal(ByteArray mem)
		{
			BigDecimal result = null;
			try 
			{
				switch(CheckDefineByte(mem)) 
				{
					case DBTechTranslator.nullDefineByte:
						return result;
					case DBTechTranslator.specialNullValueDefineByte:
						throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSIONSpecialNullValue), "" , -10811);
				}
				result = VDNNumber.Number2BigDecimal(mem.ReadBytes(m_bufpos, m_physicalLength - 1));
				if (!isFloatingPoint)
					result = result.setScale(frac);
				return result;
			} 
			catch
			{
				throw CreateParseException(result + " scale: " + frac, null);
			}
		}

		public override bool GetBoolean(ByteArray mem)
		{
			return GetInt32(mem) != 0;
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			return (byte)GetInt64(mem);
		}

		public override double GetDouble(ByteArray mem)
		{
			switch (CheckDefineByte(mem)) 
			{
				case DBTechTranslator.nullDefineByte:
					return 0;
				case DBTechTranslator.specialNullValueDefineByte:
					return double.NaN;
			}
			return BigDecimal2Double(VDNNumber.Number2BigDecimal(mem.ReadBytes(m_bufpos, m_physicalLength - 1)));
		}

		public override float GetFloat(ByteArray mem)
		{
			switch (CheckDefineByte(mem)) 
			{
				case DBTechTranslator.nullDefineByte:
					return 0;
				case DBTechTranslator.specialNullValueDefineByte:
					return float.NaN;
			}
			return BigDecimal2Float(VDNNumber.Number2BigDecimal(mem.ReadBytes(m_bufpos, m_physicalLength - 1)));
		}

		public override int GetInt32(ByteArray mem)
		{
			return (int)GetInt64(mem);
		}

		public override long GetInt64(ByteArray mem)
		{
			switch (CheckDefineByte(mem)) 
			{
				case DBTechTranslator.nullDefineByte:
					return 0;
				case DBTechTranslator.specialNullValueDefineByte:
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSIONSpecialNullValue), "" , -10811);
			}
			return VDNNumber.Number2Long(mem.ReadBytes(m_bufpos, m_physicalLength - 1));
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			if(IsDBNull(mem))
				return null;
        
			switch (m_dataType) 
			{
				case DataType.FLOAT:
					if(m_logicalLength <15)
						return GetFloat(mem);
					else
						return GetDouble(mem);
				case DataType.INTEGER: // isNull catched before
					return GetInt32(mem);
				case DataType.SMALLINT:
					return GetInt16(mem);
				default:
					return (decimal)GetBigDecimal(mem);
			}
		}

		public override int Precision
		{
			get
			{
				return m_logicalLength;
			}
		}

		public override int Scale
		{
			get
			{
				switch(m_dataType) 
				{
					case DataType.FIXED:
						return frac;
					default:
						return 0;
				}
			}
		}

		public override short GetInt16(ByteArray mem)
		{
			return (short)GetInt64(mem);
		}

		public override object TransBigDecimalForInput(BigDecimal val)
		{
			if (val == null)
				return null;
			else 
				return VDNNumber.BigDecimal2Number(val.setScale(frac));
		}

		public override object TransDoubleForInput(double val)
		{
			try 
			{
				BigDecimal bigD = new BigDecimal(val);
				if (m_dataType == DataType.FIXED)
					bigD = bigD.setScale(frac);
            
				return VDNNumber.BigDecimal2Number(bigD, 16);
			} 
			catch 
			{
				// Detect nasty double values, and throw an SQL exception.
				if(double.IsInfinity(val) || double.IsNaN(val)) 
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));
				else 
					throw;
			}
		}

		public override object TransFloatForInput(float val)
		{
			try 
			{
				BigDecimal bigD = new BigDecimal(val);
				if (m_dataType == DataType.FIXED)
					bigD = bigD.setScale(frac);
            
				return VDNNumber.BigDecimal2Number(bigD, 14);
			} 
			catch 
			{
				// Detect nasty double values, and throw an SQL exception.
				if(float.IsInfinity(val) || float.IsNaN(val)) 
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));
				else 
					throw;
			}
		}

		public override object TransInt16ForInput(short val)
		{
			return VDNNumber.Long2Number(val);
		}

		public override object TransInt32ForInput(int val)
		{
			return VDNNumber.Long2Number(val);
		}

		public override object TransInt64ForInput(long val)
		{
			return VDNNumber.Long2Number(val);
		}

		protected override object TransSpecificForInput(object obj)
		{
			if (obj == null)
				return null;
			if (obj is BigDecimal) 
				return TransBigDecimalForInput((BigDecimal)obj);
			if (obj is bool) 
				return TransBooleanForInput((bool)obj);
			if (obj is byte)
				return TransByteForInput((byte)obj);
			if (obj is double) 
				return TransDoubleForInput((double)obj);
			if (obj is float)
				return TransFloatForInput((float)obj);
			if (obj is int) 
				return TransInt32ForInput((int)obj);
			if (obj is long)
				return TransInt64ForInput((long)obj);
			if (obj is short)
				return TransInt16ForInput((short)obj);

			return null;
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			switch (CheckDefineByte(mem)) 
			{
				case DBTechTranslator.nullDefineByte:
					return null;
				case DBTechTranslator.specialNullValueDefineByte:
					return ("NaN");
			}
			return VDNNumber.Number2String(mem.ReadBytes(m_bufpos, m_physicalLength - 1),
				(m_dataType != DataType.FLOAT && m_dataType != DataType.VFLOAT), m_logicalLength, frac);
		}

		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			try 
			{
				return TransBigDecimalForInput(new BigDecimal(val.Trim()));
			}
			catch 
			{
				throw CreateParseException(val, null);
			}
		}
	}

	#endregion

	#region "Time data translator"

	internal class TimeTranslator : CharDataTranslator 
	{
		private const int TimeSize = 8;

		public TimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadASCII(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int hour = ((int)raw[0] - '0') * 10 + ((int)raw[1] - '0');
				int min = ((int)raw[3] - '0') * 10 + ((int)raw[4] - '0');
				int sec = ((int)raw[6] - '0') * 10 + ((int)raw[7] - '0');

				return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
			}
			else
				return DateTime.MinValue;
		}

		public override bool IsCaseSensitive
		{
			get
			{
				return false;
			}
		}

		public virtual object TransTimeForInput(DateTime dt)
		{
			byte[] formattedTime = new byte[TimeSize];

			formattedTime[0] = (byte)HighTime[dt.Hour];
			formattedTime[1] = (byte)LowTime[dt.Hour];
			formattedTime[2] = (byte) ':';

			formattedTime[3] = (byte)('0'+(dt.Minute/10));
			formattedTime[4] = (byte)('0'+(dt.Minute%10));
			formattedTime[5] = (byte) ':';

			formattedTime[6] = (byte)('0'+(dt.Second/10));
			formattedTime[7] = (byte)('0'+(dt.Second%10));

			return formattedTime;
		}

		protected override object TransSpecificForInput(object obj)
		{
			return (obj is DateTime) ? TransTimeForInput((DateTime)obj) : null;
		}
 
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			try 
			{
				return TransSpecificForInput(DateTime.Parse(val));
			}
			catch
			{
				// ignore
			}
			throw CreateParseException(val, "Time");
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte((byte) ' ', m_bufpos - 1);
			dataPart.WriteASCIIBytes(bytes, m_bufpos, m_physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode time data translator"

	internal class UnicodeTimeTranslator : TimeTranslator 
	{
		public UnicodeTimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) 
			: base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadUnicode(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int hour = ((int)raw[1] - '0') * 10 + ((int)raw[3] - '0');
				int min = ((int)raw[7] - '0') * 10 + ((int)raw[9] - '0');
				int sec = ((int)raw[13] - '0') * 10 + ((int)raw[15] - '0');

				return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
			}
			else
				return DateTime.MinValue;
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte ((byte) 1, m_bufpos - 1);
			dataPart.WriteUnicodeBytes(bytes, m_bufpos, m_physicalLength - 1);
		}

		public override object TransTimeForInput(DateTime dt)
		{
			byte[] chars = (byte[])base.TransTimeForInput(dt);
			byte[] bytes = Encoding.Unicode.GetBytes(Encoding.ASCII.GetString(chars));
			CheckFieldLimits(bytes.Length);
			return bytes;
		}
	}

	#endregion

	#region "Timestamp data translator"

	internal class TimestampTranslator : CharDataTranslator 
	{
		private const int TimestampSize = 26;

		public TimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadASCII(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int year = ((int)raw[0] - '0') * 1000 + ((int)raw[1] - '0') * 100 + ((int)raw[2] - '0') * 10 +((int)raw[3] - '0');
				int month = ((int)raw[5] - '0') * 10 + ((int)raw[6] - '0');
				int day = ((int)raw[8] - '0') * 10 + ((int)raw[9] - '0');
				int hour = ((int)raw[11] - '0') * 10 + ((int)raw[12] - '0');
				int min = ((int)raw[14] - '0') * 10 + ((int)raw[15] - '0');
				int sec = ((int)raw[17] - '0') * 10 + ((int)raw[18] - '0');
				int milli = ((int)raw[20] - '0') * 100 + ((int)raw[21] - '0') * 10 + ((int)raw[22] - '0');
				int nano = ((int)raw[23] - '0') * 100 + ((int)raw[24] - '0') * 10 + ((int)raw[25] - '0');

				return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(nano * TimeSpan.TicksPerMillisecond / 1000);
			}
			else
				return DateTime.MinValue;
		}

		public override bool IsCaseSensitive
		{
			get
			{
				return false;
			}
		}

		public virtual object TransTimestampForInput(DateTime dt)
		{
			byte [] formattedTimestamp = new byte [TimestampSize];

			int year = dt.Year;
			int month = dt.Month;
			int day = dt.Day;
			int hour = dt.Hour;
			int minute = dt.Minute;
			int second = dt.Second;
			int milli = dt.Millisecond;
			long nano = (dt.Ticks % TimeSpan.TicksPerMillisecond) / (TimeSpan.TicksPerMillisecond / 1000);

			formattedTimestamp[0] = (byte)('0' + (year / 1000));
			year %= 1000;
			formattedTimestamp[1] = (byte)('0' + (year / 100));
			year %= 100;
			formattedTimestamp[2] = (byte)('0' + (year / 10));
			year %= 10;
			formattedTimestamp[3] = (byte)('0' + (year));
			formattedTimestamp[4] = (byte) '-';

			formattedTimestamp[5] = (byte)HighTime[month];
			formattedTimestamp[6] = (byte)LowTime[month];
			formattedTimestamp[7] = (byte) '-';

			formattedTimestamp[8] = (byte)HighTime[day];
			formattedTimestamp[9] = (byte)LowTime[day];
			formattedTimestamp[10] = (byte) ' ';

			formattedTimestamp[11] = (byte)HighTime[hour];
			formattedTimestamp[12] = (byte)LowTime[hour];
			formattedTimestamp[13] = (byte) ':';

			formattedTimestamp[14] = (byte)('0' + (minute/10));
			formattedTimestamp[15] = (byte)('0' + (minute%10));
			formattedTimestamp[16] = (byte) ':';

			formattedTimestamp[17] = (byte)('0' + (second/10));
			formattedTimestamp[18] = (byte)('0' + (second%10));
			formattedTimestamp[19] = (byte) '.';
			
			formattedTimestamp[20] = (byte)('0' + (milli/100));
			milli %= 100;
			formattedTimestamp[21] = (byte)('0' + (milli/10));
			milli %= 10;
			formattedTimestamp[22] = (byte)('0' + milli);

			formattedTimestamp[23] = (byte)('0' + (nano/100));
			nano %= 100;
			formattedTimestamp[24] = (byte)('0' + (nano/10));
			nano %= 10;
			formattedTimestamp[25] = (byte)('0' + nano);

			return formattedTimestamp;
		}

		protected override object TransSpecificForInput(object obj)
		{
			return (obj is DateTime) ? TransTimestampForInput((DateTime)obj) : null;
		}
 
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			try 
			{
				return TransSpecificForInput(DateTime.Parse(val));
			}
			catch
			{
				// ignore
			}
			throw CreateParseException (val, "Timestamp");
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte((byte) ' ', m_bufpos - 1);
			dataPart.WriteASCIIBytes(bytes, m_bufpos, m_physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode timestamp data translator"

	internal class UnicodeTimestampTranslator : TimestampTranslator 
	{
		public UnicodeTimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) 
			: base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadUnicode(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int year = ((int)raw[1] - '0') * 1000 + ((int)raw[3] - '0') * 100 + ((int)raw[5] - '0') * 10 +((int)raw[7] - '0');
				int month = ((int)raw[11] - '0') * 10 + ((int)raw[13] - '0');
				int day = ((int)raw[17] - '0') * 10 + ((int)raw[19] - '0');
				int hour = ((int)raw[23] - '0') * 10 + ((int)raw[25] - '0');
				int min = ((int)raw[29] - '0') * 10 + ((int)raw[31] - '0');
				int sec = ((int)raw[35] - '0') * 10 + ((int)raw[37] - '0');
				int milli = ((int)raw[41] - '0') * 100 + ((int)raw[43] - '0') * 10 + ((int)raw[45] - '0');
				int nano = ((int)raw[47] - '0') * 100 + ((int)raw[49] - '0') * 10 + ((int)raw[51] - '0');

				return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(nano * TimeSpan.TicksPerMillisecond / 1000);
			}
			else
				return DateTime.MinValue;
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte ((byte) 1, m_bufpos - 1);
			dataPart.WriteUnicodeBytes(bytes, m_bufpos, m_physicalLength - 1);
		}

		public override object TransTimestampForInput(DateTime dt)
		{
			byte[] chars = (byte[])base.TransTimestampForInput(dt);
			byte[] bytes = Encoding.Unicode.GetBytes(Encoding.ASCII.GetString(chars));
			CheckFieldLimits(bytes.Length);
			return bytes;
		}
	}

	#endregion

	#region "Date data translator"

	internal class DateTranslator : CharDataTranslator 
	{
		private const int DateSize = 10;

		public DateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadASCII(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int year = ((int)raw[0] - '0') * 1000 + ((int)raw[1] - '0') * 100 + ((int)raw[2] - '0') * 10 +((int)raw[3] - '0');
				int month = ((int)raw[5] - '0') * 10 + ((int)raw[6] - '0');
				int day = ((int)raw[8] - '0') * 10 + ((int)raw[9] - '0');

				return new DateTime(year, month, day);
			}
			else
				return DateTime.MinValue;
		}

		public override bool IsCaseSensitive
		{
			get
			{
				return false;
			}
		}

		public virtual object TransDateForInput(DateTime dt)
		{
			int year = dt.Year;
			int month = dt.Month;
			int day = dt.Day;

			byte[] formattedDate = new byte[DateSize];
			formattedDate[0] = (byte)('0' + (year / 1000));
			year %= 1000;
			formattedDate[1] = (byte)('0' + (year / 100));
			year %= 100;
			formattedDate[2] = (byte)('0' + (year / 10));
			year %= 10;
			formattedDate[3] = (byte)('0' + year);
			formattedDate[4] = (byte)'-';

			formattedDate[5] = (byte)HighTime[month];
			formattedDate[6] = (byte)LowTime[month];
			formattedDate[7] = (byte)'-';

			formattedDate[8] = (byte)HighTime[day];
			formattedDate[9] = (byte)LowTime[day];

			return formattedDate;
		}

		protected override object TransSpecificForInput(object obj)
		{
			return (obj is DateTime) ? TransDateForInput((DateTime)obj) : null;
		}
 
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			try 
			{
				return TransSpecificForInput(DateTime.Parse(val));
			}
			catch
			{
				// ignore
			}
			throw CreateParseException (val, "Date");
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte((byte) ' ', m_bufpos - 1);
			dataPart.WriteASCIIBytes(bytes, m_bufpos, m_physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode date data translator"

	internal class UnicodeDateTranslator : DateTranslator 
	{
		public UnicodeDateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
	{
	}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.ReadUnicode(m_bufpos, m_physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.ReadBytes(m_bufpos, m_physicalLength - 1);

				int year = ((int)raw[1] - '0') * 1000 + ((int)raw[3] - '0') * 100 + ((int)raw[5] - '0') * 10 + ((int)raw[7] - '0');
				int month = ((int)raw[11] - '0') * 10 + ((int)raw[13] - '0');
				int day = ((int)raw[17] - '0') * 10 + ((int)raw[19] - '0');

				return new DateTime(year - 1900, month - 1, day);
			}
			else
				return DateTime.MinValue;
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > m_physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[m_dataType], -1);
			dataPart.WriteDefineByte((byte) ' ', m_bufpos - 1);
			dataPart.WriteUnicodeBytes(bytes, m_bufpos, m_physicalLength - 1);
		}

		public override object TransDateForInput(DateTime dt)
		{
			byte[] chars = (byte[])base.TransDateForInput(dt);
			byte[] bytes = Encoding.Unicode.GetBytes(Encoding.ASCII.GetString(chars));
			CheckFieldLimits(bytes.Length);
			return bytes;
		}

	}

	#endregion

	#region "Structured data translator"

	internal class StructureTranslator : DBTechTranslator 
	{
		DBProcParameterInfo      parameterStructure;
		StructMemberTranslator[] structConverter;
		bool unicode;
	
		public StructureTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool unicode) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			this.unicode = unicode;
			this.structConverter = new StructMemberTranslator[0];
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[]) data;
			dataPart.WriteDefineByte(0, m_bufpos - 1);
			dataPart.WriteBytes(bytes, m_bufpos, m_physicalLength - 1);
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem)) 
				return 0;
			else 
				result = mem.ReadBytes(m_bufpos, 1);
			return result[0];
		}

		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return mem.ReadBytes(m_bufpos, m_logicalLength);
			else
				return null;
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			byte[] ba = GetBytes(null, mem);
			if(ba != null) 
			{	
				object[] objArr = new object[structConverter.Length];
				ByteArray sb = new ByteArray(ba, 0, mem.Swapped);
				for(int i = 0; i < objArr.Length; i++)
					objArr[i] = structConverter[i].GetValue(sb, 0);
			
				return new DBProcStructure(objArr, parameterStructure.SQLTypeName);
			} 
			else 
				return null;
		}

		public override object TransByteForInput(byte val)
		{
			return TransBytesForInput(new byte[1]{val});
		}

		public override object TransBytesForInput(byte[] arg)
		{
			if (arg == null) 
				return arg;
		
			CheckFieldLimits(arg.Length);
			return arg;
		}

		protected override object TransSpecificForInput(object obj)
		{
			object result = null;
		
			if (obj is byte[])
				result = TransBytesForInput((byte[]) obj);
			else if(obj is object[]) 
				result = TransObjectArrayForInput((object[])obj);
			else if(obj is DBProcStructure) 
				result = TransObjectArrayForInput(((DBProcStructure)obj).Attributes);
		
			return result;
		}

		public object TransObjectArrayForInput(object[] objectArray) 
		{
			if(objectArray.Length != structConverter.Length)
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCTURE_ARRAYWRONGLENTGH, 
					structConverter.Length, objectArray.Length));
		
			ByteArray sb = new ByteArray(m_physicalLength - 1);
			for(int i=0; i < objectArray.Length; i++) 
			{
				if (objectArray[i] == null) 
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_NULL, i+1));
			
				structConverter[i].PutValue(sb, objectArray[i]);
			}
			return sb.arrayData;
		}

		public override object TransCharacterStreamForInput(Stream stream, int length)
		{
			if (length <= 0) 
				return null;
		
			try 
			{
				byte[] ba = new byte[length];
				int r = stream.Read(ba, 0, length);
				if (r != length) 
				{
					if (r == -1)
						r = 0;
					byte[] ba2 = ba;
					ba = new byte[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				
				return TransStringForInput(unicode ? Encoding.Unicode.GetString(ba) : Encoding.ASCII.GetString(ba));
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}
		}

		public override object TransBinaryStreamForInput(Stream reader, int length)
		{
			if (length <= 0)
				return null;
		
			try 
			{
				byte[] ba = new byte[length];
				int r = reader.Read(ba, 0, length);
				if (r != length) 
				{
					if (r == -1)
						r = 0;
					byte[] ba2 = ba;
					ba = new byte[r];
					Array.Copy(ba2, 0, ba, 0, r);
				}
				return TransBytesForInput(ba);
			} 
			catch (Exception ex) 
			{
				throw new MaxDBSQLException(ex.Message);
			}
		}

		public override Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			byte[] asBytes = GetBytes(null, mem);

			if (asBytes == null) 
				return null;
        
			return new MemoryStream(asBytes);
		}

		public override void SetProcParamInfo(DBProcParameterInfo info) 
		{
			parameterStructure = info;
			structConverter = StructMemberTranslator.CreateStructMemberTranslators(info, unicode);
		}
	}

	#endregion

	#region "Structured member data translator"

	internal abstract class StructMemberTranslator
	{
		protected StructureElement structElement;
		protected int index;
		protected int offset;
	
		public StructMemberTranslator(StructureElement structElement, int index, bool unicode) 
		{
			this.structElement = structElement;
			this.index = index;						
			this.offset = unicode ? structElement.UnicodeOffset : structElement.ASCIIOffset;	  	    
		}
	
		public abstract object GetValue(ByteArray memory, int recordOffset);
		public abstract void PutValue(ByteArray memory, object obj);
	
		protected void ThrowConversionError(string srcObj)
		{
			throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_CONVERSION, structElement.SQLTypeName, srcObj));
		}
	
		/// <summary>
		/// Creates the translator for a structure. 
		/// </summary>
		/// <param name="paramInfo">The extended parameter info</param>
		/// <param name="index">Parameter index.</param>
		/// <param name="unicode">Whether this is an unicode connection.</param>
		/// <returns>Element translator.</returns>
		public static StructMemberTranslator CreateStructMemberTranslator(DBProcParameterInfo paramInfo, int index, bool unicode) 
		{
			StructureElement s = paramInfo[index];
			if(s.TypeName.ToUpper().Trim() == "CHAR") 
			{
				if(s.CodeType.ToUpper().Trim() == "BYTE") 
					return new ByteStructureElementTranslator(s, index, unicode);
				else if(s.CodeType.ToUpper().Trim() == "ASCII") 
					return new CharASCIIStructureElementTranslator(s, index, unicode);
			} 
			else if(s.TypeName.ToUpper().Trim() == "WYDE") 
			{
				if(unicode) 
					return new WydeStructureElementTranslator(s, index, unicode);
				else 
					return new CharASCIIStructureElementTranslator(s, index, unicode);
			} 
			else if(s.TypeName.ToUpper().Trim() == "SMALLINT") 
			{
				if(s.Length == 5) 
					return new ShortStructureElementTranslator(s, index, unicode);
			} 
			else if(s.TypeName.ToUpper().Trim() == "INTEGER") 
			{
				if(s.Length == 10) 
					return new IntStructureElementTranslator(s, index, unicode);
				else if(s.Length == 19) 
					return new LongStructureElementTranslator(s, index, unicode);
			} 
			else if(s.TypeName.ToUpper().Trim() == "FIXED") 
			{
				if(s.Precision == 0) 
				{
					if(s.Length == 5) 
						return new ShortStructureElementTranslator(s, index, unicode);
					else if(s.Length == 10) 
						return new IntStructureElementTranslator(s, index, unicode);
					else if(s.Length == 19) 
						return new LongStructureElementTranslator(s, index, unicode);
				}
			} 
			else if(s.TypeName.ToUpper().Trim() == "FLOAT") 
			{
				if(s.Length == 15) 
					return new DoubleStructureElementTranslator(s, index,unicode);			
				else if(s.Length == 6) 
					return new FloatStructureElementTranslator(s, index, unicode);
			} 
			else if(s.TypeName.ToUpper().Trim() == "BOOLEAN") 
				return new BooleanStructureElementTranslator(s, index, unicode);

			throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSION_STRUCTURETYPE, index, s.SQLTypeName));
		}

		/// <summary>
		/// Creates the translators for a structure.
		/// </summary>
		/// <param name="info">The extended parameter info.</param>
		/// <param name="unicode">Whether this is an unicode connection.</param>
		/// <returns>The converter array.</returns>
		public static StructMemberTranslator[] CreateStructMemberTranslators(DBProcParameterInfo info, bool unicode) 
		{
			StructMemberTranslator[] result = new StructMemberTranslator[info.MemberCount];
		
			for(int i = 0; i< result.Length; ++i) 
				result[i] = CreateStructMemberTranslator(info, i, unicode);
		
			return result;
		}
	}

	#endregion

	#region "BOOLEAN structure element data translator"

	internal class BooleanStructureElementTranslator : StructMemberTranslator 
	{
		public BooleanStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return (memory.ReadByte(offset + recordOffset) != 0);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if(obj is bool)
				memory.WriteByte((byte)((bool)obj ? 1 : 0), offset);
			else
				ThrowConversionError(obj.GetType().FullName);
		}
	}

	#endregion

	#region "CHAR BYTE structure element data translator"

	internal class ByteStructureElementTranslator : StructMemberTranslator 
	{
		public ByteStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			byte[] bytes = memory.ReadBytes(offset + recordOffset, structElement.Length);
			if(structElement.Length == 1) 
				return bytes[0];				
			else 
				return bytes;
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if(obj is byte[]) 
				memory.WriteBytes((byte[])obj, offset);
			else if(obj is byte) 
			{
				byte[] ba = new byte[1];
				ba[0] = (byte)obj;
			} 
			else 
				ThrowConversionError(obj.GetType().FullName);
		}
	}		


	#endregion

	#region "CHAR ASCII structure element data translator"

	internal class CharASCIIStructureElementTranslator : StructMemberTranslator 
	{

		public CharASCIIStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			byte[] bytes = memory.ReadBytes(offset + recordOffset, structElement.Length);
			if(structElement.Length == 1) 
				return (char)bytes[0];		
			else 
				return Encoding.ASCII.GetString(bytes);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			string convStr = null;	
			if(obj is char[]) 
				convStr = new string((char[])obj);
			else if(obj is string) 
				convStr = (string)obj;
			else if(obj is char) 
				convStr = new string(new char[]{(char)obj});
			else 
				ThrowConversionError(obj.GetType().FullName);
			
			memory.WriteASCII(convStr, offset);
		}
	}

	#endregion

	#region "WYDE structure element data translator"

	internal class WydeStructureElementTranslator : StructMemberTranslator 
	{
		public WydeStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			string ca  = memory.ReadUnicode(offset + recordOffset, structElement.Length * 2);
			if(structElement.Length == 1) 
				return ca[0];
			else
				return ca;
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			string convStr = null;
			if(obj is char[]) 
				convStr = new string((char[])obj);
			else if(obj is string) 
				convStr = (string)obj;
			else if(obj is char) 
				convStr = new string(new char[]{(char)obj});
			else 
				ThrowConversionError(obj.GetType().FullName);
			
			memory.WriteUnicode(convStr, offset);
		}
	}

	#endregion

	#region "SMALLINT structure element data translator"

	internal class ShortStructureElementTranslator : StructMemberTranslator 
	{
		public ShortStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.ReadInt16(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= short.MaxValue && (float)obj >= short.MinValue) 
				|| (obj is double && (double)obj <= short.MaxValue && (double)obj >= short.MinValue)
				|| (obj is decimal && (decimal)obj <= short.MaxValue && (decimal)obj >= short.MinValue))
				memory.WriteInt16((short)obj, offset);
			else
			{
				if (obj is float && ((float)obj > short.MaxValue || (float)obj < short.MinValue) 
					|| (obj is double && ((double)obj > short.MaxValue || (double)obj < short.MinValue)
					|| (obj is decimal && ((decimal)obj > short.MaxValue || (decimal)obj < short.MinValue))))
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					ThrowConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "INTEGER structure element data translator"

	internal class IntStructureElementTranslator : StructMemberTranslator 
	{
		public IntStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.ReadInt32(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= int.MaxValue && (float)obj >= int.MinValue) 
				|| (obj is double && (double)obj <= int.MaxValue && (double)obj >= int.MinValue)
				|| (obj is decimal && (decimal)obj <= int.MaxValue && (decimal)obj >= int.MinValue))
				memory.WriteInt32((int)obj, offset);
			else
			{
				if (obj is float && ((float)obj > int.MaxValue || (float)obj < int.MinValue) 
					|| (obj is double && ((double)obj > int.MaxValue || (double)obj < int.MinValue)
					|| (obj is decimal && ((decimal)obj > int.MaxValue || (decimal)obj < int.MinValue))))
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					ThrowConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "LONG structure element data translator"

	internal class LongStructureElementTranslator : StructMemberTranslator 
	{

		public LongStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.ReadInt64(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= long.MaxValue && (float)obj >= long.MinValue) 
				|| (obj is double && (double)obj <= long.MaxValue && (double)obj >= long.MinValue)
				|| (obj is decimal && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
				memory.WriteInt64((long)obj, offset);
			else
			{
				if (obj is float && ((float)obj > long.MaxValue || (float)obj < long.MinValue) 
					|| (obj is double && ((double)obj > long.MaxValue || (double)obj < long.MinValue)
					|| (obj is decimal && ((decimal)obj > long.MaxValue || (decimal)obj < long.MinValue))))
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					ThrowConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "FLOAT structure element data translator"

	internal class FloatStructureElementTranslator : StructMemberTranslator 
	{
		public FloatStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return BitConverter.ToSingle(memory.ReadBytes(offset, 4), 0); 
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong || obj is float 
				|| (obj is double && (double)obj <= long.MaxValue && (double)obj >= float.MinValue)
				|| (obj is decimal && (decimal)obj <= long.MaxValue))
				memory.WriteBytes(BitConverter.GetBytes((float)obj), offset);
			else
			{
				if ((obj is double && ((double)obj > long.MaxValue || (double)obj < float.MinValue)
					|| (obj is decimal && ((decimal)obj > long.MaxValue))))
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					ThrowConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "DOUBLE structure element data translator"

	internal class DoubleStructureElementTranslator : StructMemberTranslator 
	{
		public DoubleStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return BitConverter.ToDouble(memory.ReadBytes(offset, 8), 0); 
		}

		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong || obj is float || obj is double
				|| (obj is decimal && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
				memory.WriteBytes(BitConverter.GetBytes((double)obj), offset);
			else
				ThrowConversionError(obj.GetType().FullName);
		}
	}

	#endregion

	#region "Translator for LONG arguments of DB Procedures"

	internal class ProcedureStreamTranslator : DBTechTranslator
	{
		public ProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) : 
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override int ColumnDisplaySize
		{
			get
			{
				return int.MaxValue;
			}
		}

		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			if (IsBinary) 
			{
				if (stream == null) 
					return null;
            
				return new BinaryProcedurePutValue(this, stream, length);
			} 
			else 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSION_BYTESTREAM));
		}
    
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
	
			if(IsASCII) 
				return new ASCIIProcedurePutValue(this, Encoding.ASCII.GetBytes(val));
			else 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSION_STRINGSTREAM));
		}

		public override object TransCharacterStreamForInput(Stream stream, int length) 
		{
			if(IsASCII) 
			{
				if(stream ==  null) 
					return null;
				else 
					return new ASCIIProcedurePutValue(this, stream, length);            
			} 
			else 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSION_STRINGSTREAM));            
		}
    
		public override object TransBytesForInput(byte[] val)
		{
			if (val == null) 
				return TransBinaryStreamForInput(null, -1);
			else 
				return TransBinaryStreamForInput(new MemoryStream(val), -1);
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			AbstractProcedurePutValue putval = (AbstractProcedurePutValue) data;
			putval.putDescriptor(dataPart);
		}

		protected override object TransSpecificForInput(object obj)
		{
			if (obj is Stream)
				return TransASCIIStreamForInput((Stream) obj, -1);
			else
				return null;
		}
    
		public virtual object TransASCIIStreamForInput(Stream stream, int length)
		{
			if (IsASCII) 
			{
				if (stream == null) 
					return null;
            
				return new ASCIIProcedurePutValue(this, stream, length);
			} 
			else 
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_CONVERSION_STRINGSTREAM));
		}

		private bool IsASCII
		{
			get
			{
				return m_dataType == DataType.STRA || m_dataType == DataType.LONGA;
			}
		}

		private bool IsBinary
		{
			get
			{
				return m_dataType == DataType.STRB || m_dataType == DataType.LONGB;
			}
		}
    
		private Stream GetStream(ISQLParamController controller, ByteArray mem, ByteArray longdata)
		{
			Stream result = null;
			AbstractGetValue getval = null;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.ReadBytes(m_bufpos, m_logicalLength);
				// return also NULL if the LONG hasn't been touched.
				if (DescriptorIsNull(descriptor)) 
					return null;
			
				getval = new GetLOBValue (controller.Connection, descriptor, longdata, m_dataType);
				result = getval.ASCIIStream;
			}
			return result;
		}

		public override Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream(controller, mem, longData);
		}
	
		public override Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			if(IsBinary)
				return GetStream(controller, mem, longData);
			else
				throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BINARYREADFROMLONG));
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem))
				return 0;
			else
				result = GetBytes(controller, mem);
			return result[0];
		}

		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			Stream blobStream;
			MemoryStream tmpStream;

			blobStream = GetBinaryStream(controller, mem, controller.ReplyData);
			if (blobStream == null) 
				return null;
		
			try 
			{
				const int bufSize = 4096;
				byte[] buf = new byte[bufSize];
				int readLen;

				tmpStream = new MemoryStream();
				readLen = blobStream.Read(buf, 0, buf.Length);
				while (readLen > 0) 
				{
					tmpStream.Write(buf, 0, readLen);
					if (readLen < bufSize) 
						break;
				
					readLen = blobStream.Read(buf, 0, buf.Length);
				}
			} 
			catch (StreamIOException sqlExc) 
			{
				throw sqlExc.SqlException;
			}
			catch (IOException ioExc) 
			{
				throw new DataException(ioExc.Message, ioExc);
			}
			return tmpStream.ToArray();
		}

		public virtual TextReader GetCharacterStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream byteStream = GetASCIIStream(controller, mem, longData);
			if (byteStream == null) 
				return null;
			
			return new RawByteReader(byteStream);
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			const int bufSize = 4096;
			TextReader reader;
			StringBuilder result = new StringBuilder();

			reader = GetCharacterStream(controller, mem, controller.ReplyData);
			if (reader == null)
				return null;
        
			try 
			{
				char [] buf = new char[bufSize];
				int charsRead;

				while ((charsRead = reader.Read(buf, 0, bufSize)) > 0) 
				{
					result.Append(new string(buf, 0, charsRead));
					if (charsRead < bufSize)
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message, exc);
			}
			return result.ToString();
		}

		public override long GetChars(ISQLParamController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
		{
			const int bufSize = 4096;
			TextReader reader;
			int alreadyRead = 0;

			reader = GetCharacterStream(controller, mem, controller.ReplyData);
			if (reader == null) 
				return 0;
        
			try 
			{
				char[] buf = new char[bufSize];
				int charsRead;

				while ((charsRead = reader.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0) 
				{
					alreadyRead += charsRead;
					if (charsRead < bufSize) 
						break;
				}

				alreadyRead = 0;
				while ((charsRead = reader.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0) 
				{
					Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, charsRead);
					alreadyRead += charsRead;
					if (charsRead < bufSize) 
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message);
			}

			return alreadyRead;
		}

		public override long GetBytes(ISQLParamController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
		{
			const int bufSize = 4096;
			Stream stream;
			int alreadyRead = 0;

			stream = GetBinaryStream(controller, mem, controller.ReplyData);
			if (stream == null) 
				return 0;
        
			try 
			{
				byte[] buf = new byte[bufSize];
				int bytesRead;

				while ((bytesRead = stream.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0) 
				{
					alreadyRead += bytesRead;
					if (bytesRead < bufSize) 
						break;
				}

				alreadyRead = 0;
				while ((bytesRead = stream.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0) 
				{
					Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, bytesRead);
					alreadyRead += bytesRead;
					if (bytesRead < bufSize) 
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message);
			}

			return alreadyRead;
		}

		protected bool DescriptorIsNull(byte[] descriptor) 
		{
			return descriptor[LongDesc.State] == LongDesc.StateStream;
		}
	}

	#endregion

	#region "Translator for LONG UNICODE arguments of DB Procedures"

	internal class UnicodeProcedureStreamTranslator : ProcedureStreamTranslator 
	{
		public UnicodeProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override int ColumnDisplaySize
		{
			get
			{
				return 1073741824;
			}
		}

		public override object TransStringForInput(string val)
		{
			if (val == null)
				return null;
        
			return new UnicodeProcedurePutValue(this, val.ToCharArray());
		}

		public object TransCharacterStreamForInput(TextReader reader, int length)
		{
			if (reader == null) 
				return null;
        
			return new UnicodeProcedurePutValue(this, reader, length);
		}

		public override Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader reader = GetCharacterStream(controller, mem, longData);
			if (reader == null) 
				return null;
        
			return new ReaderStream(reader, false);
		}

		public override TextReader GetCharacterStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader result = null;
			AbstractGetValue getval;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.ReadBytes(m_bufpos, m_logicalLength);
				if (DescriptorIsNull(descriptor))
					return null;
            
				getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData, IsUnicodeColumn);
				result = getval.CharacterStream;
			}
			return result;
		}
    
		public override object TransASCIIStreamForInput(Stream stream, int length)
		{
			if (stream == null) 
				return null;
        
			TextReader reader = new StreamReader(stream);
			return TransCharacterStreamForInput(reader, length);
		}
	}

	#endregion

	#region "Abstract stream translator"

	internal abstract class StreamTranslator : BinaryDataTranslator
	{
		protected StreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override int ColumnDisplaySize
		{
			get
			{
				switch (m_dataType) 
				{
					case DataType.STRUNI:
					case DataType.LONGUNI:
						return 1073741824 - 4096;
					default:
						return 2147483647 - 8192;
				}
			}
		}

		public override int Precision
		{
			get
			{
				return int.MaxValue;
			}
		}

		public override Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ASCIIREADFROMLONG));
		}

		public override Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream(controller, mem, longData);
		}

		public virtual TextReader GetCharacterStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream byteStream = GetASCIIStream(controller, mem, longData);
			if (byteStream == null) 
				return null;

			return new RawByteReader(byteStream);
		}

		public Stream GetStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream result = null;
			AbstractGetValue getval = null;
			byte [] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.ReadBytes(m_bufpos, m_logicalLength);
				getval = new GetLOBValue(controller.Connection, descriptor, longData, m_dataType);
          
				result = getval.ASCIIStream;
			}
			return result;
		}

		public override string GetString(ISQLParamController controller, ByteArray mem)
		{
			const int bufSize = 4096;
			TextReader reader;
			StringBuilder result = new StringBuilder();

			reader = GetCharacterStream(controller, mem, controller.ReplyData);
			if (reader == null) 
				return null;
        
			try 
			{
				char[] buf = new char[bufSize];
				int charsRead;

				while ((charsRead = reader.Read(buf, 0, bufSize)) > 0) 
				{
					result.Append(new string(buf, 0, charsRead));
					if (charsRead < bufSize) 
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message);
			}

			return result.ToString();
		}

		public override long GetChars(ISQLParamController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
		{
			const int bufSize = 4096;
			TextReader reader;
			int alreadyRead = 0;

			reader = GetCharacterStream(controller, mem, controller.ReplyData);
			if (reader == null) 
				return 0;
        
			try 
			{
				char[] buf = new char[bufSize];
				int charsRead;

				while ((charsRead = reader.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0) 
				{
					alreadyRead += charsRead;
					if (charsRead < bufSize) 
						break;
				}

				alreadyRead = 0;
				while ((charsRead = reader.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0) 
				{
					Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, charsRead);
					alreadyRead += charsRead;
					if (charsRead < bufSize) 
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message);
			}

			return alreadyRead;
		}

		public override long GetBytes(ISQLParamController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
		{
			const int bufSize = 4096;
			Stream stream;
			int alreadyRead = 0;

			stream = GetBinaryStream(controller, mem, controller.ReplyData);
			if (stream == null) 
				return 0;
        
			try 
			{
				byte[] buf = new byte[bufSize];
				int bytesRead;

				while ((bytesRead = stream.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0) 
				{
					alreadyRead += bytesRead;
					if (bytesRead < bufSize) 
						break;
				}

				alreadyRead = 0;
				while ((bytesRead = stream.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0) 
				{
					Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, bytesRead);
					alreadyRead += bytesRead;
					if (bytesRead < bufSize) 
						break;
				}
			}
			catch (StreamIOException streamExc) 
			{
				throw streamExc.SqlException;
			}
			catch (IOException exc) 
			{
				throw new DataException(exc.Message);
			}

			return alreadyRead;
		}

		public override bool IsCaseSensitive
		{
			get
			{
				return true;
			}
		}

		protected override void PutSpecific(DataPart dataPart, object data)
		{
			PutValue putval = (PutValue) data;
			putval.PutDescriptor(dataPart, m_bufpos - 1);
		}

		public virtual object TransASCIIStreamForInput(Stream stream, int length)
		{
			throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ASCIIPUTTOLONG));
		}

		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BINARYPUTTOLONG));
		}

		/**
		 * Translates a byte array. This is done only in derived classes
		 * that accept byte arrays (this one may be a BLOB or a CLOB,
		 * and so does not decide about it).
		 * @param val The byte array to bind.
		 * @return The Putval instance created for this one.
		 */
		public override object TransBytesForInput(byte [] val)
		{
			throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BINARYPUTTOLONG));
		}
    
		protected override object TransSpecificForInput(object obj)
		{
			object result = null;

			if (obj is Stream) 
				result = TransASCIIStreamForInput((Stream) obj, -1);
        
			return result;
		}

		public object TransStreamForInput(Stream stream, int length)
		{
			if (stream == null) 
				return null;
        
			return new PutValue(stream, length, m_bufpos);
		}
   
		public override object TransStringForInput(string val)
		{
			throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ASCIIPUTTOLONG));
		}
	}

	#endregion

	#region "ASCII stream translator"

	internal class ASCIIStreamTranslator : StreamTranslator 
	{
		public ASCIIStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			m_charDatatypePostfix = " ASCII";
		}

		public override Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream (controller, mem, longData);
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetString(controller, mem);
		}

		public override object TransASCIIStreamForInput(Stream stream, int length)
		{
			return TransStreamForInput(stream, length);
		}
	
		public object TransCharacterStreamForInput(TextReader reader, int length)
		{
			if (reader == null) 
				return reader;

			Stream stream = new ReaderStream(reader, false);
			return TransStreamForInput(stream, length);
		}
    
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
			return new PutValue(Encoding.GetEncoding(1252).GetBytes(val), m_bufpos);
		}
	}

	#endregion

	#region "Binary stream translator"

	internal class BinaryStreamTranslator : StreamTranslator 
	{
		public BinaryStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override Stream GetBinaryStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream (controller, mem, longData);
		}

		public override byte GetByte(ISQLParamController controller, ByteArray mem)
		{
			if (IsDBNull(mem))
				return 0;
        
			return GetBytes(controller, mem)[0];
		}
    
		public override byte[] GetBytes(ISQLParamController controller, ByteArray mem)
		{
			Stream blobStream;
			MemoryStream tmpStream;

			blobStream = GetBinaryStream (controller, mem, controller.ReplyData);
			if (blobStream == null) 
				return null;
        
			try 
			{
				const int bufSize = 4096;
				byte[] buf = new byte [bufSize];
				int readLen;

				tmpStream = new MemoryStream();
				readLen = blobStream.Read(buf, 0, bufSize);
				while(readLen > 0) 
				{
					tmpStream.Write(buf, 0, readLen);
					if (readLen < bufSize) 
						break;
                
					readLen = blobStream.Read(buf, 0 , bufSize);
				}
			}
			catch(StreamIOException sqlExc) 
			{
				throw sqlExc.SqlException;
			}
			catch (IOException ioExc) 
			{
				throw new DataException(ioExc.Message);
			}
			return tmpStream.ToArray();
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetBytes(controller, mem);
		}

		public override object TransByteForInput(byte val)
		{
			byte[] barr = new byte[1];
			barr[0] = val;
			return TransBytesForInput(barr);
		}
    
		public override object TransBytesForInput(byte[] val)
		{
			if (val == null) 
				return null;
		 
			return new PutValue(val, m_bufpos);	
		}
  
		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			return TransStreamForInput(stream, length);
		}

		protected override object TransSpecificForInput(object obj)
		{
			object result = null;

			if (obj is Stream) 
				result = TransBinaryStreamForInput((Stream)obj, -1);
        
			return result;
		}
	}

	#endregion

	#region "UNICODE stream translator"

	internal class UnicodeStreamTranslator : StreamTranslator
	{
		public UnicodeStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			m_charDatatypePostfix = " UNICODE";
		}

		public override Stream GetASCIIStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader reader = GetCharacterStream(controller, mem, longData);
			if (reader == null) 
				return null;
        
			return new ReaderStream(reader, false);
		}

		public override object GetValue(ISQLParamController controller, ByteArray mem)
		{
			return GetString(controller, mem);
		}

		public override TextReader GetCharacterStream(ISQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader result = null;
			AbstractGetValue getval;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.ReadBytes(m_bufpos, m_logicalLength);
				getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData, IsUnicodeColumn);
				result = getval.CharacterStream;
			}
			return result;
		}

		public override object TransASCIIStreamForInput(Stream stream, int length)
		{
			if (stream == null)
				return null;
        
			TextReader reader = new StreamReader(stream);
			return TransCharacterStreamForInput(reader, length);
		}

		public object TransCharacterStreamForInput(TextReader reader, int length)
		{
			if (reader == null) 
				return null;
        
			return new PutUnicodeValue(reader, length, m_bufpos);
		}

		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			return new PutUnicodeValue(val.ToCharArray(), m_bufpos);               
		}
	}

	#endregion

#endif
}
