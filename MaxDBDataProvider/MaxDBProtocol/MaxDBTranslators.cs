using System;
using System.IO;
using System.Resources;
using System.Text;
using System.Threading;
using System.ComponentModel;
using System.Globalization;
using System.Data;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "DB Tech translator class"

	public abstract class DBTechTranslator
	{
		protected int logicalLength;
		protected int physicalLength;
		protected int bufpos;   // bufpos points to actual data, defbyte is at -1
		protected byte mode;
		protected byte ioType;
		protected byte dataType;
		protected bool writeAllowed = false;
		protected bool isReadOnly = false;
		protected bool isAutoIncrement = false;
		protected string characterDatatypePostfix = "";
		private string colName;
		private int colIndex;

		public const int nullDefineByte = 1;
		public const int specialNullValueDefineByte = 2;
		public const int unknownDefineByte = -1;

		protected DBTechTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
		{
			mode = (byte) mode;
			ioType = (byte) ioType;
			dataType = (byte) dataType;
			logicalLength = len;
			physicalLength = ioLen;
			bufpos = bufpos;
		}

		public void allowWrites() 
		{
			writeAllowed = true;
		}

		public int BufPos 
		{
			get
			{
				return bufpos;
			}
		}

		public int ColumnIndex 
		{
			get
			{
				return colIndex;
			}
			set
			{
				colIndex = value;
			}
		}

		public string ColumnTypeName 
		{
			get
			{
				switch (dataType) 
				{
					case DataType.CHA:
					case DataType.CHE:
					case DataType.DBYTEEBCDIC:
						return DataType.stringValues[DataType.CHA];
					case DataType.CHB:
					case DataType.ROWID:
						return DataType.stringValues[DataType.CHB];
					case DataType.UNICODE:
						return DataType.stringValues[DataType.UNICODE];
					case DataType.VARCHARA:
					case DataType.VARCHARE:
						return DataType.stringValues[DataType.VARCHARA];
					case DataType.VARCHARB:
						return DataType.stringValues[DataType.VARCHARB];
					case DataType.VARCHARUNI:
						return DataType.stringValues[DataType.VARCHARUNI];
					case DataType.STRA:
					case DataType.STRE:
					case DataType.LONGA:
					case DataType.LONGE:
					case DataType.LONGDB:
						return DataType.stringValues[DataType.LONGA];
					case DataType.STRB:
					case DataType.LONGB:
						return DataType.stringValues[DataType.LONGB];
					case DataType.STRUNI:
					case DataType.LONGUNI:
						return DataType.stringValues[DataType.LONGUNI];
					case DataType.DATE:
						return DataType.stringValues[DataType.DATE];
					case DataType.TIME:
						return DataType.stringValues[DataType.TIME];
					case DataType.TIMESTAMP:
						return DataType.stringValues[DataType.TIMESTAMP];
					case DataType.BOOLEAN:
						return DataType.stringValues[DataType.BOOLEAN];
					case DataType.FIXED:
					case DataType.NUMBER:
						return DataType.stringValues[DataType.FIXED];
					case DataType.FLOAT:
					case DataType.VFLOAT:
						return DataType.stringValues[DataType.FLOAT];
					case DataType.SMALLINT:
						return DataType.stringValues[DataType.SMALLINT];
					case DataType.INTEGER:
						return DataType.stringValues[DataType.INTEGER];
					default:
						return MessageTranslator.Translate(MessageKey.UNKNOWNTYPE);
				}
			}
		}

		public Type ColumnClassName
		{
			get
			{
				switch (dataType) 
				{
					case DataType.FIXED:
					case DataType.FLOAT:
					case DataType.VFLOAT:
					case DataType.NUMBER:
					case DataType.NONUMBER:
						return typeof(decimal);
					case DataType.CHA:
					case DataType.CHE:
						return typeof(string);
					case DataType.CHB:
					case DataType.ROWID:
						return typeof(byte[]);
					case DataType.DATE:
					case DataType.TIME:
					case DataType.TIMESTAMP:
						return typeof(DateTime);
					case DataType.UNKNOWN:
						return typeof(object);
					case DataType.DURATION:
						return typeof(long);
					case DataType.DBYTEEBCDIC:
					case DataType.STRA:
					case DataType.STRE:
					case DataType.LONGA:
					case DataType.LONGE:
					case DataType.STRUNI:
						return typeof(TextReader);
					case DataType.STRB:
					case DataType.LONGB:
					case DataType.LONGDB:
					case DataType.LONGUNI:
						return typeof(BinaryReader);
					case DataType.BOOLEAN:
						return typeof(bool);
					case DataType.UNICODE:
					case DataType.VARCHARUNI:
						return typeof(string);
					case DataType.DTFILLER1:
					case DataType.DTFILLER2:
					case DataType.DTFILLER3:
					case DataType.DTFILLER4:
						return typeof(object);
					case DataType.SMALLINT:
						return typeof(short);
					case DataType.INTEGER:
						return typeof(int);
					case DataType.VARCHARA:
					case DataType.VARCHARE:
						return typeof(string);
					case DataType.VARCHARB:
						return typeof(byte[]);
					default:
						return typeof(object);
				}
			}
		}

		public virtual int ColumnDisplaySize
		{
			get
			{
				return logicalLength;
			}
		}

		public string ColumnName
		{
			get
			{
				return colName;
			}
			set
			{
				colName = value;
			}
		}

		protected bool IsUnicodeColumn 
		{
			get
			{
				switch (dataType) 
				{
					case DataType.STRUNI:
					case DataType.LONGUNI:
						return true;
					default:
						return false;
				}
			}
		}

		protected MaxDBConversionException newGetException(string requestedType)
		{
			return new MaxDBConversionException(MessageTranslator.Translate(
				MessageKey.ERROR_CONVERSIONSQLNET, ColumnTypeName, requestedType));
		}

		protected MaxDBConversionException newSetException(string requestedType)
		{
			return new MaxDBConversionException(MessageTranslator.Translate(
				MessageKey.ERROR_CONVERSIONNETSQL, requestedType, ColumnTypeName));
		}

		protected MaxDBConversionException newParseException(string data, string requestedType)
		{
			if (requestedType == null) 
				requestedType = ColumnTypeName;
			return new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONDATA, data, requestedType));
		}

		public virtual Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw newGetException("ASCIIStream");
		}

		public virtual Stream GetUnicodeStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw newGetException("UnicodeStream");
		}

		public virtual Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw newGetException("BinaryStream");
		}

		public virtual bool GetBoolean(ByteArray mem)
		{
			throw newGetException("boolean");
		}

		public virtual byte GetByte(SQLParamController controller, ByteArray mem)
		{
			throw newGetException("byte");
		}

		public virtual byte[] GetBytes(SQLParamController controller, ByteArray mem)
		{
			throw newGetException("byte[]");
		}

		public virtual DateTime GetDateTime(ByteArray mem)
		{
			throw this.newGetException("DateTime");
		}

		public virtual double GetDouble(ByteArray mem)
		{
			throw newGetException("double");
		}

		public virtual float GetFloat(ByteArray mem)
		{
			throw newGetException("float");
		}

		public virtual BigDecimal GetBigDecimal(ByteArray mem)
		{
			throw newGetException("decimal");
		}

		public virtual decimal GetDecimal(ByteArray mem)
		{
			throw newGetException("decimal");
		}

		public virtual short GetInt16(ByteArray mem)
		{
			throw newGetException("Int16");
		}

		public virtual int GetInt32(ByteArray mem)
		{
			throw newGetException("Int32");
		}

		public virtual long GetInt64(ByteArray mem)
		{
			throw newGetException("Int64");
		}

		public virtual object GetValue(ByteArray mem)
		{
			throw newGetException("object");
		}

		public virtual object[] GetValues(ByteArray mem)
		{
			throw newGetException("object[]");
		}

		public virtual string GetString(SQLParamController controller, ByteArray mem)
		{
			object rawResult = GetValue(mem);

			if (rawResult == null) 
				return null;
			else 
				return rawResult.ToString();
		}

		public ParameterDirection ParameterMode 
		{
			get
			{
				switch (ioType) 
				{
					case (ParamInfo.Output):
						return ParameterDirection.Output;
					case (ParamInfo.Input):
						return ParameterDirection.Input;
					case (ParamInfo.InOut):
						return ParameterDirection.InputOutput;
					default :
						return ParameterDirection.Input;
				}
			}
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
				return (ioType != ParamInfo.Output);
			}
		}

		public virtual bool IsOutput 
		{
			get
			{
				return (ioType != ParamInfo.Input);
			}
		}

		public virtual bool IsLongKind 
		{
			get
			{
				switch (dataType) 
				{
					case DataType.STRA:
					case DataType.STRE:
					case DataType.STRB:
					case DataType.STRUNI:
					case DataType.LONGA:
					case DataType.LONGE:
					case DataType.LONGB:
					case DataType.LONGDB:
					case DataType.LONGUNI:
						return true;
					default:
						return false;
				}
			}
		}

		public bool IsDBNull(ByteArray mem) 
		{
			return (mem.readByte(bufpos - 1) == 0xFF);
		}

		public int CheckDefineByte(ByteArray mem) 
		{
			int defByte = mem.readByte(bufpos - 1);
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
				if ((mode & ParamInfo.Mandatory) != 0) 
					return false;

				if ((mode & ParamInfo.Optional) != 0) 
					return true;
				
				throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_DBNULL_UNKNOWN));
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
				return writeAllowed;
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
			if (ioType != ParamInfo.Output) 
			{
				if (data == null) 
					dataPart.WriteNull(bufpos, physicalLength - 1);
				else 
				{
					putSpecific(dataPart, data);
					dataPart.AddArg(this.bufpos, this.physicalLength - 1);
				}
			}
		}

		public void putProcOutput(DataPart dataPart, object data)
		{
			if (ioType != ParamInfo.Input) 
			{
				if (data == null) 
					dataPart.WriteNull(bufpos, physicalLength - 1);
				else 
				{
					putSpecific(dataPart, data);
					dataPart.AddArg(bufpos, physicalLength - 1);
				}
			}
		}

		protected void CheckFieldLimits(int byteLength)
		{
			if (byteLength > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], colIndex + 1);
		}

		protected abstract void putSpecific(DataPart dataPart, object data);
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
				throw newSetException(val.GetType().FullName);
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
			throw newGetException("Bytes");
		}

		public virtual object TransDateTimeForInput(DateTime val, Calendar cal)
		{
			return TransObjectForInput(val);
		}

		public virtual object TransDoubleForInput(double val)
		{
			if(double.IsInfinity(val) || double.IsNaN(val)) 
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));

			return TransObjectForInput(new BigDecimal(val));
		}
 
		public virtual object TransFloatForInput(float val)
		{
			if(float.IsInfinity(val) || float.IsNaN(val)) 
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));

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
			throw newSetException("String");
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

	public abstract class CharDataTranslator : DBTechTranslator
	{
		protected static char[] HighTime = { '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3'};
		protected static char[] LowTime  = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1'};

		protected CharDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			characterDatatypePostfix = " ASCII";
		}

		public override bool IsCaseSensitive 
		{
			get
			{
				return true;
			}
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = Encoding.GetEncoding(1251).GetBytes(data.ToString());
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte((byte) ' ', bufpos - 1);
			dataPart.writeASCIIBytes(bytes, bufpos, physicalLength - 1);
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
				throw newParseException(strValue, "DateTime");
			}
		}
	}

	#endregion

	#region "String data translator class"

	public class StringTranslator : CharDataTranslator
	{
		public StringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) : base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public object GetObject(ByteArray mem)
		{
			return GetString(null, mem);
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
				throw newParseException(val, "BigDecimal");
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
				throw newParseException(val, "Boolean");
			}
		}

		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
		{
			string result = GetString(controller, mem);
			if (result != null)
				return Encoding.Unicode.GetBytes(result);
			else
				return null;
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
		{
			return BigDecimal2Byte(GetBigDecimal(mem));
		}

		public override Stream GetUnicodeStream(SQLParamController controller, ByteArray mem, ByteArray longData)
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

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return mem.readASCII(bufpos, logicalLength).TrimStart();
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

	public class UnicodeStringTranslator : StringTranslator
	{
 
		public UnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			characterDatatypePostfix = " UNICODE";
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return Encoding.Unicode.GetString(mem.readBytes(bufpos, logicalLength * 2)).TrimStart();
			else
				return null;
		}
     
		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
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
        
			byte[] bytes = Encoding.Unicode.GetBytes(arg);
			CheckFieldLimits (bytes.Length);
			return bytes;
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			dataPart.writeDefineByte ((byte) 1, bufpos - 1);
			dataPart.writeUnicodeBytes ((byte []) data, bufpos, physicalLength - 1);
		}
	}

	#endregion

	#region "Space option string data translator class"

	public class SpaceOptionStringTranslator : StringTranslator
	{
		public SpaceOptionStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
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

	public class SpaceOptionUnicodeStringTranslator : UnicodeStringTranslator
	{
		public SpaceOptionUnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
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

	public abstract class BinaryDataTranslator : DBTechTranslator 
	{
		public BinaryDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		protected override void putSpecific (DataPart dataPart, object data)
		{
			byte[] bytes = (byte[]) data;
			dataPart.writeDefineByte(0, bufpos - 1);
			dataPart.writeBytes (bytes, bufpos, physicalLength - 1);
		}
	}

	#endregion

	#region "Bytes data translator class"

	public class BytesTranslator : BinaryDataTranslator 
	{
		public BytesTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem))
				return 0;
			else
				result = mem.readBytes(bufpos, 1);
			return result[0];
		}
    
		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
		{
			if (IsDBNull(mem))
				return null;
			else
				return mem.readBytes(bufpos, 1);
		}

		public object GetObject(ByteArray mem)
		{
			return GetBytes(null, mem);
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
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

		public override Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			byte[] asBytes = GetBytes(null, mem);

			if (asBytes == null) 
				return null;
        
			return new MemoryStream(asBytes);
		}
	}

	#endregion

	#region "Boolean data translator class"

	class BooleanTranslator : BinaryDataTranslator 
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
				return (mem.readByte(bufpos) == 0x00 ? false: true); 
			else
				return false;
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
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

		public override object GetValue(ByteArray mem)
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
				throw this.newSetException (obj.GetType().FullName);

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

	class NumericTranslator : BinaryDataTranslator
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
						throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONSpecialNullValue), "", -10811);
				}
				result = VDNNumber.Number2BigDecimal(mem.readBytes(bufpos, physicalLength - 1));
				result = result.setScale(scale);
				return result;
			} 
			catch
			{
				throw newParseException(result + " scale: " + scale, null);
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
						throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONSpecialNullValue), "" , -10811);
				}
				result = VDNNumber.Number2BigDecimal(mem.readBytes(bufpos, physicalLength - 1));
				if (!isFloatingPoint)
					result = result.setScale(frac);
				return result;
			} 
			catch
			{
				throw newParseException(result + " scale: " + frac, null);
			}
		}

		public override bool GetBoolean(ByteArray mem)
		{
			return GetInt32(mem) != 0;
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
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
			return BigDecimal2Double(VDNNumber.Number2BigDecimal(mem.readBytes(bufpos, physicalLength - 1)));
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
			return BigDecimal2Float(VDNNumber.Number2BigDecimal(mem.readBytes(bufpos, physicalLength - 1)));
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
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONSpecialNullValue), "" , -10811);
			}
			return VDNNumber.Number2Long(mem.readBytes(bufpos, physicalLength - 1));
		}

		public override object GetValue(ByteArray mem)
		{
			if(IsDBNull(mem))
				return null;
        
			switch (this.dataType) 
			{
				case DataType.FLOAT:
					if(logicalLength <15)
						return GetFloat(mem);
					else
						return GetDouble(mem);
				case DataType.INTEGER: // isnull catched before
					return GetInt32(mem);
				case DataType.SMALLINT:
					return GetInt16(mem);
				default:
					return GetBigDecimal(mem);
			}
		}

		public int Precision
		{
			get
			{
				return logicalLength;
			}
		}

		public int Scale
		{
			get
			{
				switch(dataType) 
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
				if (dataType == DataType.FIXED)
					bigD = bigD.setScale(frac);
            
				return VDNNumber.BigDecimal2Number(bigD, 16);
			} 
			catch 
			{
				// Detect nasty double values, and throw an SQL exception.
				if(double.IsInfinity(val) || double.IsNaN(val)) 
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));
				else 
					throw;
			}
		}

		public override object TransFloatForInput(float val)
		{
			try 
			{
				BigDecimal bigD = new BigDecimal(val);
				if (dataType == DataType.FIXED)
					bigD = bigD.setScale(frac);
            
				return VDNNumber.BigDecimal2Number(bigD, 14);
			} 
			catch 
			{
				// Detect nasty double values, and throw an SQL exception.
				if(float.IsInfinity(val) || float.IsNaN(val)) 
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_SPECIAL_NUMBER_UNSUPPORTED, val.ToString()));
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

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			switch (CheckDefineByte(mem)) 
			{
				case DBTechTranslator.nullDefineByte:
					return null;
				case DBTechTranslator.specialNullValueDefineByte:
					return ("NaN");
			}
			return VDNNumber.Number2String(mem.readBytes(bufpos, physicalLength - 1),
				(dataType != DataType.FLOAT && dataType != DataType.VFLOAT), logicalLength, frac);
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
				throw newParseException(val, null);
			}
		}
	}

	#endregion

	#region "Time data translator"

	public class TimeTranslator : CharDataTranslator 
	{
		private const int TimeSize = 8;

		public TimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readASCII(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int hour = ((int)raw[0]-'0')*10 + ((int)raw[1]-'0');
				int min = ((int)raw[3]-'0')*10 + ((int)raw[4]-'0');
				int sec = ((int)raw[6]-'0')*10 + ((int)raw[7]-'0');

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
			throw this.newParseException (val, "Time");
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte((byte) ' ', bufpos - 1);
			dataPart.writeASCIIBytes(bytes, bufpos, physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode time data translator"

	public class UnicodeTimeTranslator : TimeTranslator 
	{
		public UnicodeTimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) 
			: base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readUnicode(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int hour = ((int)raw[1]-'0')*10 + ((int)raw[3]-'0');
				int min = ((int)raw[7]-'0')*10 + ((int)raw[9]-'0');
				int sec = ((int)raw[13]-'0')*10 + ((int)raw[15]-'0');

				return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
			}
			else
				return DateTime.MinValue;
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte ((byte) 1, bufpos - 1);
			dataPart.writeUnicodeBytes(bytes, bufpos, physicalLength - 1);
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

	public class TimestampTranslator : CharDataTranslator 
	{
		private const int TimestampSize = 26;

		public TimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readASCII(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int year = ((int)raw[0]-'0')*1000 + ((int)raw[1]-'0')*100 + ((int)raw[2]-'0')*10 +((int)raw[3]-'0');
				int month = ((int)raw[5]-'0')*10 + ((int)raw[6]-'0');
				int day = ((int)raw[8]-'0')*10 + ((int)raw[9]-'0');
				int hour = ((int)raw[11]-'0')*10 + ((int)raw[12]-'0');
				int min = ((int)raw[14]-'0')*10 + ((int)raw[15]-'0');
				int sec = ((int)raw[17]-'0')*10 + ((int)raw[18]-'0');
				int milli = ((int)raw[20]-'0')*100 + ((int)raw[21]-'0')*10 + ((int)raw[22]-'0');
				int tick = ((int)raw[23]-'0');

				return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(tick);
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
			int day = dt.Day + 1;
			int hour = dt.Hour;
			int minute = dt.Minute;
			int second = dt.Second;

			formattedTimestamp[0] = (byte)('0'+(year / 1000));
			year %= 1000;
			formattedTimestamp[1] = (byte)('0'+(year / 100));
			year %= 100;
			formattedTimestamp[2] = (byte)('0'+(year / 10));
			year %= 10;
			formattedTimestamp[3] = (byte)('0'+(year));
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

			formattedTimestamp[14] = (byte)('0'+(minute/10));
			formattedTimestamp[15] = (byte)('0'+(minute%10));
			formattedTimestamp[16] = (byte) ':';

			formattedTimestamp[17] = (byte)('0'+(second/10));
			formattedTimestamp[18] = (byte)('0'+(second%10));
			formattedTimestamp[19] = (byte) '.';

			int tmpVal = dt.Millisecond;
			formattedTimestamp[20] = (byte)('0'+(tmpVal/100000));
			tmpVal %= 100000;
			formattedTimestamp[21] = (byte)('0'+(tmpVal/10000));
			tmpVal %= 10000;
			formattedTimestamp[22] = (byte)('0'+(tmpVal/1000));
			tmpVal %= 1000;
			formattedTimestamp[23] = (byte)('0'+(tmpVal/100));
			tmpVal %= 100;
			formattedTimestamp[24] = (byte)('0'+(tmpVal/10));
			tmpVal %= 10;
			formattedTimestamp[25] = (byte)('0'+(tmpVal));

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
			throw this.newParseException (val, "Timestamp");
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte((byte) ' ', bufpos - 1);
			dataPart.writeASCIIBytes(bytes, bufpos, physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode timestamp data translator"

	public class UnicodeTimestampTranslator : TimestampTranslator 
	{
		public UnicodeTimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) 
			: base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readUnicode(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int year = ((int)raw[1]-'0')*1000 + ((int)raw[3]-'0')*100 + ((int)raw[5]-'0')*10 +((int)raw[7]-'0');
				int month = ((int)raw[11]-'0')*10 + ((int)raw[13]-'0');
				int day = ((int)raw[17]-'0')*10 + ((int)raw[19]-'0');
				int hour = ((int)raw[23]-'0')*10 + ((int)raw[25]-'0');
				int min = ((int)raw[29]-'0')*10 + ((int)raw[31]-'0');
				int sec = ((int)raw[35]-'0')*10 + ((int)raw[37]-'0');
				int milli = ((int)raw[41]-'0')*100 + ((int)raw[43]-'0')*10 + ((int)raw[45]-'0');
				int tick = ((int)raw[49]-'0');

				return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(tick);
			}
			else
				return DateTime.MinValue;
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte ((byte) 1, bufpos - 1);
			dataPart.writeUnicodeBytes(bytes, bufpos, physicalLength - 1);
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

	public class DateTranslator : CharDataTranslator 
	{
		private const int DateSize = 10;

		public DateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override object GetValue(ByteArray mem)
		{
			return GetDateTime(mem);
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readASCII(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int year = ((int)raw[0]-'0')*1000 + ((int)raw[1]-'0')*100 + ((int)raw[2]-'0')*10 +((int)raw[3]-'0');
				int month = ((int)raw[5]-'0')*10 + ((int)raw[6]-'0');
				int day = ((int)raw[8]-'0')*10 + ((int)raw[9]-'0');

				return new DateTime(year-1900, month-1, day);
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
			throw this.newParseException (val, "Date");
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte((byte) ' ', bufpos - 1);
			dataPart.writeASCIIBytes(bytes, bufpos, physicalLength - 1);
		}
	}

	#endregion

	#region "Unicode date data translator"

	public class UnicodeDateTranslator : DateTranslator 
	{
		public UnicodeDateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
	{
	}

		public override string GetString(SQLParamController controller, ByteArray mem)
		{
			return (!IsDBNull(mem) ? mem.readUnicode(bufpos, physicalLength - 1) : null);
		}

		public override DateTime GetDateTime(ByteArray mem)
		{
			if (!IsDBNull(mem)) 
			{
				byte[] raw = mem.readBytes(bufpos, physicalLength - 1);

				int year = ((int)raw[1]-'0')*1000 + ((int)raw[3]-'0')*100 + ((int)raw[5]-'0')*10 + ((int)raw[7]-'0');
				int month = ((int)raw[11]-'0')*10 + ((int)raw[13]-'0');
				int day = ((int)raw[17]-'0')*10 + ((int)raw[19]-'0');

				return new DateTime(year - 1900, month - 1, day);
			}
			else
				return DateTime.MinValue;
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte [] bytes = (byte[])data;
			if (bytes.Length > physicalLength - 1) 
				throw new MaxDBValueOverflowException(DataType.stringValues[dataType], -1);
			dataPart.writeDefineByte((byte) ' ', bufpos - 1);
			dataPart.writeUnicodeBytes(bytes, bufpos, physicalLength - 1);
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

	public class StructureTranslator : DBTechTranslator 
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

		protected override void putSpecific(DataPart dataPart, object data)
		{
			byte[] bytes = (byte[]) data;
			dataPart.writeDefineByte(0, bufpos - 1);
			dataPart.writeBytes(bytes, bufpos, physicalLength - 1);
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem)) 
				return 0;
			else 
				result = mem.readBytes(bufpos, 1);
			return result[0];
		}

		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
		{
			if (!IsDBNull(mem))
				return mem.readBytes(bufpos, logicalLength);
			else
				return null;
		}

		public override object GetValue(ByteArray mem)
		{
			byte[] ba = GetBytes(null, mem);
			if(ba != null) 
			{	
				object[] objArr = new object[structConverter.Length];
				ByteArray sb = new ByteArray(ba, mem.Swapped);
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
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCTURE_ARRAYWRONGLENTGH, 
					structConverter.Length, objectArray.Length));
		
			ByteArray sb = new ByteArray(physicalLength - 1);
			for(int i=0; i < objectArray.Length; i++) 
			{
				if (objectArray[i] == null) 
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_NULL, i+1));
			
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

		public override Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			byte[] asBytes = GetBytes(null, mem);

			if (asBytes == null) 
				return null;
        
			return new MemoryStream(asBytes);
		}

		public override void SetProcParamInfo(DBProcParameterInfo info) 
		{
			parameterStructure = info;
			structConverter = StructMemberTranslator.createStructMemberTranslators(info, unicode);
		}
	}

	#endregion

	#region "Structured member data translator"

	public abstract class StructMemberTranslator
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
	
		protected void throwConversionError(string srcObj)
		{
			throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_CONVERSION, structElement.SQLTypeName, srcObj));
		}
	
		public static StructMemberTranslator createStructMemberTranslator(DBProcParameterInfo paramInfo, int index, bool unicode) 
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

			throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSION_STRUCTURETYPE, index, s.SQLTypeName));
		}

		/**
		 * Creates the translators for a structure.
		 * @param info The extended parameter info.
		 * @param unicode Whether this is an unicode connection.
		 * @return The converter array.
		 */
		public static StructMemberTranslator[] createStructMemberTranslators(DBProcParameterInfo info, bool unicode) 
		{
			StructMemberTranslator[] result = new StructMemberTranslator[info.MemberCount];
		
			for(int i = 0; i< result.Length; ++i) 
				result[i] = createStructMemberTranslator(info, i, unicode);
		
			return result;
		}
	}

	#endregion

	#region "BOOLEAN structure element data translator"

	public class BooleanStructureElementTranslator : StructMemberTranslator 
	{
		public BooleanStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return (memory.readByte(offset + recordOffset) != 0);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if(obj is bool)
				memory.writeByte((byte)((bool)obj ? 1 : 0), offset);
			else
				throwConversionError(obj.GetType().FullName);
		}
	}

	#endregion

	#region "CHAR BYTE structure element data translator"

	public class ByteStructureElementTranslator : StructMemberTranslator 
	{
		public ByteStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			byte[] bytes = memory.readBytes(offset + recordOffset, structElement.Length);
			if(structElement.Length == 1) 
				return bytes[0];				
			else 
				return bytes;
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if(obj is byte[]) 
				memory.writeBytes((byte[])obj, offset);
			else if(obj is byte) 
			{
				byte[] ba = new byte[1];
				ba[0] = (byte)obj;
			} 
			else 
				throwConversionError(obj.GetType().FullName);
		}
	}		


	#endregion

	#region "CHAR ASCII structure element data translator"

	class CharASCIIStructureElementTranslator : StructMemberTranslator 
	{

		public CharASCIIStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			byte[] bytes = memory.readBytes(offset + recordOffset, structElement.Length);
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
				throwConversionError(obj.GetType().FullName);
			
			memory.writeASCII(convStr, offset);
		}
	}

	#endregion

	#region "WYDE structure element data translator"

	public class WydeStructureElementTranslator : StructMemberTranslator 
	{
		public WydeStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			string ca  = memory.readUnicode(offset + recordOffset, structElement.Length * 2);
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
				throwConversionError(obj.GetType().FullName);
			
			memory.writeUnicode(convStr, offset);
		}
	}

	#endregion

	#region "SMALLINT structure element data translator"

	public class ShortStructureElementTranslator : StructMemberTranslator 
	{
		public ShortStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.readInt16(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= short.MaxValue && (float)obj >= short.MinValue) 
				|| (obj is double && (double)obj <= short.MaxValue && (double)obj >= short.MinValue)
				|| (obj is decimal && (decimal)obj <= short.MaxValue && (decimal)obj >= short.MinValue))
				memory.writeInt16((short)obj, offset);
			else
			{
				if (obj is float && ((float)obj > short.MaxValue || (float)obj < short.MinValue) 
					|| (obj is double && ((double)obj > short.MaxValue || (double)obj < short.MinValue)
					|| (obj is decimal && ((decimal)obj > short.MaxValue || (decimal)obj < short.MinValue))))
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					throwConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "INTEGER structure element data translator"

	public class IntStructureElementTranslator : StructMemberTranslator 
	{
		public IntStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.readInt32(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= int.MaxValue && (float)obj >= int.MinValue) 
				|| (obj is double && (double)obj <= int.MaxValue && (double)obj >= int.MinValue)
				|| (obj is decimal && (decimal)obj <= int.MaxValue && (decimal)obj >= int.MinValue))
				memory.writeInt32((int)obj, offset);
			else
			{
				if (obj is float && ((float)obj > int.MaxValue || (float)obj < int.MinValue) 
					|| (obj is double && ((double)obj > int.MaxValue || (double)obj < int.MinValue)
					|| (obj is decimal && ((decimal)obj > int.MaxValue || (decimal)obj < int.MinValue))))
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					throwConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "LONG structure element data translator"

	public class LongStructureElementTranslator : StructMemberTranslator 
	{

		public LongStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return memory.readInt64(offset + recordOffset);
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong 
				|| (obj is float && (float)obj <= long.MaxValue && (float)obj >= long.MinValue) 
				|| (obj is double && (double)obj <= long.MaxValue && (double)obj >= long.MinValue)
				|| (obj is decimal && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
				memory.writeInt64((long)obj, offset);
			else
			{
				if (obj is float && ((float)obj > long.MaxValue || (float)obj < long.MinValue) 
					|| (obj is double && ((double)obj > long.MaxValue || (double)obj < long.MinValue)
					|| (obj is decimal && ((decimal)obj > long.MaxValue || (decimal)obj < long.MinValue))))
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					throwConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "FLOAT structure element data translator"

	public class FloatStructureElementTranslator : StructMemberTranslator 
	{

		public FloatStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return BitConverter.ToSingle(memory.readBytes(offset, 4), 0); 
		}
		
		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong || obj is float 
				|| (obj is double && (double)obj <= long.MaxValue && (double)obj >= float.MinValue)
				|| (obj is decimal && (decimal)obj <= long.MaxValue))
				memory.writeBytes(BitConverter.GetBytes((float)obj), offset);
			else
			{
				if ((obj is double && ((double)obj > long.MaxValue || (double)obj < float.MinValue)
					|| (obj is decimal && ((decimal)obj > long.MaxValue))))
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STRUCT_ELEMENT_OVERFLOW, structElement.SQLTypeName, obj.ToString()));
				else
					throwConversionError(obj.GetType().FullName);
			}
		}
	}

	#endregion

	#region "DOUBLE structure element data translator"

	public class DoubleStructureElementTranslator : StructMemberTranslator 
	{

		public DoubleStructureElementTranslator(StructureElement structElement, int index, bool unicode) : base(structElement, index, unicode)
		{
		}

		public override object GetValue(ByteArray memory, int recordOffset) 
		{
			return BitConverter.ToDouble(memory.readBytes(offset, 8), 0); 
		}

		public override void PutValue(ByteArray memory, object obj)
		{
			if (obj is byte || obj is short || obj is int || obj is long 
				|| obj is ushort || obj is uint || obj is ulong || obj is float || obj is double
				|| (obj is decimal && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
				memory.writeBytes(BitConverter.GetBytes((double)obj), offset);
			else
				throwConversionError(obj.GetType().FullName);
		}
	}

	#endregion

	#region "Translator for LONG arguments of DB Procedures"

	public class ProcedureStreamTranslator : DBTechTranslator
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
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSION_BYTESTREAM));
		}
    
		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
	
			if(IsASCII) 
				return new ASCIIProcedurePutValue(this, Encoding.ASCII.GetBytes(val));
			else 
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSION_STRINGSTREAM));
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
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSION_STRINGSTREAM));            
		}
    
		public override object TransBytesForInput(byte[] val)
		{
			if (val == null) 
				return TransBinaryStreamForInput(null, -1);
			else 
				return TransBinaryStreamForInput(new MemoryStream(val), -1);
		}

		protected override void putSpecific(DataPart dataPart, object data)
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
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSION_STRINGSTREAM));
		}

		private bool IsASCII
		{
			get
			{
				return dataType == DataType.STRA || dataType == DataType.LONGA;
			}
		}

		private bool IsBinary
		{
			get
			{
				return dataType == DataType.STRB || dataType == DataType.LONGB;
			}
		}
    
		private Stream GetStream(SQLParamController controller, ByteArray mem, ByteArray longdata)
		{
			Stream result = null;
			AbstractGetValue getval = null;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
				// return also NULL if the LONG hasn't been touched.
				if (descriptorIsNull(descriptor)) 
					return null;
			
				getval = new GetLOBValue (controller.Connection, descriptor, longdata, dataType);
				result = getval.ASCIIStream;
			}
			return result;
		}

		public override Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream(controller, mem, longData);
		}
	
		public override Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			if(IsBinary)
				return GetStream(controller, mem, longData);
			else
				throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_BINARYREADFROMLONG));
		}

		private GetLOBValue GetLOB(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			GetLOBValue result = null;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
				if(descriptorIsNull(descriptor)) 
					return null;
			
				result = new GetLOBValue(controller.Connection, descriptor, longData, dataType);
			}
			return result;
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
		{
			byte[] result = null;
			if (IsDBNull(mem))
				return 0;
			else
				result = GetBytes(controller, mem);
			return result[0];
		}

		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
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

		public virtual TextReader GetCharacterStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream byteStream = GetASCIIStream(controller, mem, longData);
			if (byteStream == null) 
				return null;
			
			return new RawByteReader(byteStream);
		}

		public override string GetString(SQLParamController controller, ByteArray mem)
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

		protected bool descriptorIsNull(byte[] descriptor) 
		{
			return descriptor[LongDesc.State] == LongDesc.StateStream;
		}
	}

	#endregion

	#region "Translator for LONG UNICODE arguments of DB Procedures"

	public class UnicodeProcedureStreamTranslator : ProcedureStreamTranslator 
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

		public override Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader reader = GetCharacterStream(controller, mem, longData);
			if (reader == null) 
				return null;
        
			return new ReaderStream(reader, false);
		}

		public override TextReader GetCharacterStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader result = null;
			AbstractGetValue getval;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
				if (descriptorIsNull(descriptor))
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

	public abstract class StreamTranslator : BinaryDataTranslator
	{
		protected StreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_ASCIIREADFROMLONG));
		}

		public override Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream(controller, mem, longData);
		}

		public virtual TextReader GetCharacterStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream byteStream = GetASCIIStream(controller, mem, longData);
			if (byteStream == null) 
				return null;

			return new RawByteReader(byteStream);
		}

		public override int ColumnDisplaySize
		{
			get
			{
				switch (this.dataType) 
				{
					case DataType.STRUNI:
					case DataType.LONGUNI:
						return 1073741824 - 4096;
					default:
						return 2147483647 - 8192;
				}
			}
		}

		private GetLOBValue GetLOB(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			GetLOBValue result = null;
			byte [] descriptor;

			if (IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
				result = new GetLOBValue(controller.Connection, descriptor, longData, dataType);
			}
			return result;
		}

		public int Precision
		{
			get
			{
				return int.MaxValue;
			}
		}

		public Stream GetStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			Stream result = null;
			AbstractGetValue getval = null;
			byte [] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
				getval = new GetLOBValue(controller.Connection, descriptor, longData, dataType);
          
				result = getval.ASCIIStream;
			}
			return result;
		}

		public override String GetString(SQLParamController controller, ByteArray mem)
		{
			const int bufSize = 4096;
			TextReader reader;
			StringBuilder result = new StringBuilder();

			reader = GetCharacterStream(controller, mem, controller.ReplyData);
			if (reader == null) 
				return null;
        
			try 
			{
				char [] buf = new char [bufSize];
				int charsRead;

				while ((charsRead = reader.Read(buf, 0, bufSize)) > 0) 
				{
					result.Append (new string(buf, 0, charsRead));
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

		public override bool IsCaseSensitive
		{
			get
			{
				return true;
			}
		}

		protected override void putSpecific(DataPart dataPart, object data)
		{
			PutValue putval = (PutValue) data;
			putval.putDescriptor(dataPart, bufpos - 1);
		}

		public virtual object TransASCIIStreamForInput(Stream stream, int length)
		{
			throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_ASCIIPUTTOLONG));
		}

		public override object TransBinaryStreamForInput(Stream stream, int length)
		{
			throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_BINARYPUTTOLONG));
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
			throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_BINARYPUTTOLONG));
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
        
			return new PutValue(stream, length, bufpos);
		}
   
		public override object TransStringForInput(string val)
		{
			throw new MaxDBConversionException(MessageTranslator.Translate(MessageKey.ERROR_ASCIIPUTTOLONG));
		}
	}

	#endregion

	#region "ASCII stream translator"

	public class ASCIIStreamTranslator : StreamTranslator 
	{
		public ASCIIStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			characterDatatypePostfix = " ASCII";
		}

		public override Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream (controller, mem, longData);
		}

		public object GetObject(SQLParamController controller, ByteArray mem)
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
			return new PutValue(Encoding.GetEncoding(1252).GetBytes(val), bufpos);
		}
	}

	#endregion

	#region "Binary stream translator"

	public class BinaryStreamTranslator : StreamTranslator 
	{
		public BinaryStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
		}

		public override Stream GetBinaryStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			return GetStream (controller, mem, longData);
		}

		public override byte GetByte(SQLParamController controller, ByteArray mem)
		{
			if (IsDBNull(mem))
				return 0;
        
			return GetBytes(controller, mem)[0];
		}
    
		public override byte[] GetBytes(SQLParamController controller, ByteArray mem)
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

				tmpStream = new MemoryStream ();
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

		public object GetObject(SQLParamController controller, ByteArray mem)
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
		 
			return new PutValue(val, bufpos);	
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

	public class UnicodeStreamTranslator : StreamTranslator
	{
		public UnicodeStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos) :
			base(mode, ioType, dataType, len, ioLen, bufpos)
		{
			characterDatatypePostfix = " UNICODE";
		}

		public override Stream GetASCIIStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader reader = GetCharacterStream(controller, mem, longData);
			if (reader == null) 
				return null;
        
			return new ReaderStream(reader, false);
		}

		public object GetObject(SQLParamController controller, ByteArray mem)
		{
			return GetString(controller, mem);
		}

		public override TextReader GetCharacterStream(SQLParamController controller, ByteArray mem, ByteArray longData)
		{
			TextReader result = null;
			AbstractGetValue getval;
			byte[] descriptor;

			if (!IsDBNull(mem)) 
			{
				descriptor = mem.readBytes(bufpos, logicalLength);
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
        
			return new PutUnicodeValue(reader, length, bufpos);
		}

		public override object TransStringForInput(string val)
		{
			if (val == null) 
				return null;
        
			return new PutUnicodeValue(val.ToCharArray(), bufpos);               
		}
	}

	#endregion
	
	#region "Message translator class"

	public class MessageTranslator
	{
		private static ResourceManager rm = new ResourceManager("MaxDBDataProvider.MaxDBProtocol.MaxDBMessages", typeof(MessageTranslator).Assembly);

		public static string Translate(string key)
		{
			return Translate(key, null);
		}

		public static string Translate(string key, object o1)
		{
			return Translate(key, new object[]{ o1 });
		}

		public static string Translate(string key, object o1, object o2)
		{
			return Translate(key, new object[]{ o1, o2 });
		}

		public static string Translate(string key, object o1, object o2, object o3)
		{
			return Translate(key, new object[]{ o1, o2, o3 });
		}

		public static string Translate(string key, object[] args) 
		{
			try 
			{
				// retrieve text and format it
				string msg = rm.GetString(key);
				if (args != null)
					return string.Format(msg, args);
				else
					return msg;
			} 
			catch(MissingManifestResourceException) 
			{
				// emergency - create an informative message in this case at least
				StringBuilder result = new StringBuilder("No message available for locale ");
				result.Append(Thread.CurrentThread.CurrentUICulture.EnglishName);
				result.Append(", key ");
				result.Append(key);
				// if arguments given append them
				if(args == null || args.Length==0) 
					result.Append(".");
				else 
				{
					result.Append(", arguments [");
					for(int i=0; i<args.Length - 1; i++) 
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length-1].ToString());
					result.Append("].");
				}
				return result.ToString();
			} 
			catch 
			{
				StringBuilder result = new StringBuilder("No message available for default locale ");
				result.Append("for key ");
				result.Append(key);
				// if arguments given append them
				if(args == null || args.Length==0) 
					result.Append(".");
				else 
				{
					result.Append(", arguments [");
					for(int i=0; i< args.Length - 1; i++) 
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length-1].ToString());
					result.Append("].");
				}
				return result.ToString();
			}
		}
	}

	#endregion
}
