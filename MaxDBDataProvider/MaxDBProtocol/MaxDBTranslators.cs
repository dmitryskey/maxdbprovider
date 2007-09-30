//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.IO;
using System.Text;
using System.Threading;
using System.ComponentModel;
using System.Globalization;
using System.Data;
using MaxDB.Data.Utilities;

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE

    #region "DB Tech translator class"

    internal abstract class DBTechTranslator
    {
        protected int iLogicalLength;
        protected int iPhysicalLength;
        protected int iBufferPosition;   // bufpos points to actual data, defbyte is at -1
        protected byte byMode;
        protected byte byIOType;
        protected byte byDataType;
        private string strColumnName;
        private int iColumnIndex;

        public const int iNullDefineByte = 1;
        public const int iSpecialNullValueDefineByte = 2;
        public const int iUnknownDefineByte = -1;

        protected DBTechTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
        {
            byMode = (byte)mode;
            byIOType = (byte)ioType;
            byDataType = (byte)dataType;
            iLogicalLength = len;
            iPhysicalLength = ioLen;
            iBufferPosition = bufpos;
        }

        public int BufPos
        {
            get
            {
                return iBufferPosition;
            }
        }

        public virtual int Precision
        {
            get
            {
                return iLogicalLength;
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
                return iPhysicalLength;
            }
        }

        public int ColumnIndex
        {
            get
            {
                return iColumnIndex;
            }
            set
            {
                iColumnIndex = value;
            }
        }

        public string ColumnTypeName
        {
            get
            {
                return GeneralColumnInfo.GetTypeName(byDataType);
            }
        }

        public Type ColumnDataType
        {
            get
            {
                return GeneralColumnInfo.GetType(byDataType);
            }
        }

        public MaxDBType ColumnProviderType
        {
            get
            {
                return GeneralColumnInfo.GetMaxDBType(byDataType);
            }
        }

        public virtual int ColumnDisplaySize
        {
            get
            {
                return iLogicalLength;
            }
        }

        public string ColumnName
        {
            get
            {
                return strColumnName;
            }
            set
            {
                strColumnName = value;
            }
        }

        protected MaxDBConversionException CreateGetException(string requestedType)
        {
            return new MaxDBConversionException(MaxDBMessages.Extract(
                MaxDBError.CONVERSIONSQLNET, ColumnTypeName, requestedType));
        }

        protected MaxDBConversionException CreateSetException(string requestedType)
        {
            return new MaxDBConversionException(MaxDBMessages.Extract(
                MaxDBError.CONVERSIONNETSQL, requestedType, ColumnTypeName));
        }

        protected MaxDBConversionException CreateParseException(string data, string requestedType)
        {
            if (requestedType == null)
                requestedType = ColumnTypeName;
            return new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.CONVERSIONDATA, data, requestedType));
        }

        public virtual Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            throw CreateGetException("ASCIIStream");
        }

        public virtual Stream GetUnicodeStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            throw CreateGetException("UnicodeStream");
        }

        public virtual Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            throw CreateGetException("BinaryStream");
        }

        public virtual bool GetBoolean(ByteArray mem)
        {
            throw CreateGetException(typeof(bool).ToString());
        }

        public virtual byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            throw CreateGetException(typeof(byte).ToString());
        }

        public virtual byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            throw CreateGetException(typeof(byte[]).ToString());
        }

        public virtual long GetBytes(ISqlParameterController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
        {
            byte[] bytes = GetBytes(controller, mem);
            if (bytes == null) return 0;
            Array.Copy(bytes, fldOffset, buffer, bufferoffset, bytes.Length);
            return bytes.Length;
        }

        public virtual DateTime GetDateTime(ByteArray mem)
        {
            throw this.CreateGetException(typeof(DateTime).ToString());
        }

        public virtual double GetDouble(ByteArray mem)
        {
            throw CreateGetException(typeof(double).ToString());
        }

        public virtual float GetFloat(ByteArray mem)
        {
            throw CreateGetException(typeof(float).ToString());
        }

        public virtual BigDecimal GetBigDecimal(ByteArray mem)
        {
            throw CreateGetException(typeof(decimal).ToString());
        }

        public virtual decimal GetDecimal(ByteArray mem)
        {
            throw CreateGetException(typeof(decimal).ToString());
        }

        public virtual short GetInt16(ByteArray mem)
        {
            throw CreateGetException(typeof(Int16).ToString());
        }

        public virtual int GetInt32(ByteArray mem)
        {
            throw CreateGetException(typeof(Int32).ToString());
        }

        public virtual long GetInt64(ByteArray mem)
        {
            throw CreateGetException(typeof(Int64).ToString());
        }

        public virtual object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            throw CreateGetException(typeof(object).ToString());
        }

        public virtual object[] GetValues(ByteArray mem)
        {
            throw CreateGetException(typeof(object[]).ToString());
        }

        public virtual string GetString(ISqlParameterController controller, ByteArray mem)
        {
            object rawResult = GetValue(controller, mem);

            if (rawResult == null)
                return null;
            else
                return rawResult.ToString();
        }

        public virtual long GetChars(ISqlParameterController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
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
                return (byIOType != ParamInfo.Output);
            }
        }

        public virtual bool IsOutput
        {
            get
            {
                return (byIOType != ParamInfo.Input);
            }
        }

        public virtual bool IsLongKind
        {
            get
            {
                return GeneralColumnInfo.IsLong(byDataType);
            }
        }

        public virtual bool IsTextualKind
        {
            get
            {
                return GeneralColumnInfo.IsTextual(byDataType);
            }
        }

        public bool IsDBNull(ByteArray mem)
        {
            return (mem.ReadByte(iBufferPosition - 1) == 0xFF);
        }

        public int CheckDefineByte(ByteArray mem)
        {
            int defByte = mem.ReadByte(iBufferPosition - 1);
            switch (defByte)
            {
                case -1:
                    return iNullDefineByte;
                case -2:
                    return iSpecialNullValueDefineByte;
                default:
                    return iUnknownDefineByte;
            }
        }

        public bool IsNullable
        {
            get
            {
                if ((byMode & ParamInfo.Mandatory) != 0)
                    return false;

                if ((byMode & ParamInfo.Optional) != 0)
                    return true;

                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.DBNULL_UNKNOWN));
            }
        }

        public void Put(DataPart dataPart, object data)
        {
            if (byIOType != ParamInfo.Output)
            {
                if (data == null)
                    dataPart.WriteNull(iBufferPosition, iPhysicalLength - 1);
                else
                {
                    PutSpecific(dataPart, data);
                    dataPart.AddArg(iBufferPosition, iPhysicalLength - 1);
                }
            }
        }

        protected void CheckFieldLimits(int byteLength)
        {
            if (byteLength > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(iColumnIndex + 1);
        }

        protected abstract void PutSpecific(DataPart dataPart, object data);
        protected abstract object TransSpecificForInput(object obj);

        public virtual object TransObjectForInput(object value)
        {
            object result;
            if (value == null)
                return null;

            result = TransSpecificForInput(value);
            if (result != null)
                return result;

            if (value.GetType() == typeof(string))
                return TransStringForInput((string)value);

            if (value.GetType() == typeof(BigDecimal))
                return TransStringForInput(VDNNumber.BigDecimal2PlainString((BigDecimal)value));

            if (value.GetType().IsArray)
            {
                if (value.GetType() == typeof(byte[]))
                    return TransBytesForInput((byte[])value);

                if (value.GetType() == typeof(char[]))
                    return TransStringForInput(new String((char[])value));

                // cannot convert other arrays
                throw CreateSetException(value.GetType().FullName);
            }
            else
                // default conversion to string
                return TransStringForInput(value.ToString());
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

        public virtual object TransTimeSpanForInput(TimeSpan val)
        {
            return TransObjectForInput(val);
        }

        public virtual object TransDoubleForInput(double val)
        {
            if (double.IsInfinity(val) || double.IsNaN(val))
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIAL_NUMBER_UNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));

            return TransObjectForInput(new BigDecimal(val));
        }

        public virtual object TransFloatForInput(float val)
        {
            if (float.IsInfinity(val) || float.IsNaN(val))
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIAL_NUMBER_UNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));

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

        public static double BigDecimal2Double(BigDecimal bd)
        {
            return (bd == null ? 0.0 : (double)bd);
        }

        public static float BigDecimal2Float(BigDecimal bd)
        {
            return (bd == null ? 0 : (float)bd);
        }

        public static decimal BigDecimal2Decimal(BigDecimal bd)
        {
            return (bd == null ? 0 : (decimal)bd);
        }

        public static short BigDecimal2Int16(BigDecimal bd)
        {
            return (bd == null ? (short)0 : (short)bd);
        }

        public static int BigDecimal2Int32(BigDecimal bd)
        {
            return (bd == null ? 0 : (int)bd);
        }

        public static long BigDecimal2Int64(BigDecimal bd)
        {
            return (bd == null ? 0 : (long)bd);
        }

        public virtual void SetProcParamInfo(DBProcParameterInfo info)
        {
        }

        internal static void SetEncoding(DBTechTranslator[] translators, Encoding encoding)
        {
            if (translators != null)
            {
                foreach (DBTechTranslator tt in translators)
                {
                    CharDataTranslator cdt = tt as CharDataTranslator;
                    if (cdt != null)
                    {
                        cdt.AsciiEncoding = encoding;
                    }
                    else
                    {
                        ASCIIStreamTranslator ast = tt as ASCIIStreamTranslator;
                        if (ast != null)
                        {
                            ast.AsciiEncoding = encoding;
                        }
                    }
                }
            }
        }
    }

    #endregion

    #region "Character data translator class"

    internal abstract class CharDataTranslator : DBTechTranslator
    {
        protected static char[] HighTime = { '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3' };
        protected static char[] LowTime = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1' };

        protected CharDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        private Encoding asciiEncoding;

        public Encoding AsciiEncoding
        {
            get
            {
                if (asciiEncoding == null)
                {
                    return Encoding.GetEncoding(1252);
                }
                return asciiEncoding;
            }
            set { asciiEncoding = value; }
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
            byte[] bytes = AsciiEncoding.GetBytes(data.ToString());

            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(-1);
            dataPart.WriteDefineByte((byte)' ', iBufferPosition - 1);
            dataPart.WriteAsciiBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            String strValue = GetString(null, mem);
            if (strValue == null)
                return DateTime.MinValue;
            try
            {
                return DateTime.Parse(strValue, CultureInfo.InvariantCulture);
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
        public StringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
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
                return (int.Parse(val.Trim(), CultureInfo.InvariantCulture) != 0);
            }
            catch
            {
                throw CreateParseException(val, typeof(bool).Name);
            }
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            string result = GetString(controller, mem);
            if (result != null)
                return Encoding.Unicode.GetBytes(result);
            else
                return null;
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            byte[] bytes = GetBytes(controller, mem);
            if (bytes == null || bytes.Length == 0)
                return 0;
            else
                return bytes[0];
        }

        public override Stream GetUnicodeStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
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

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                string result = mem.ReadEncoding(AsciiEncoding, iBufferPosition, iLogicalLength);
                switch (byDataType)
                {
                    case DataType.VARCHARA:
                    case DataType.VARCHARB:
                    case DataType.VARCHARE:
                    case DataType.VARCHARUNI:
                        return result.TrimEnd();
                    default:
                        return result;
                }
            }
            else
                return null;
        }

        public override object TransBytesForInput(byte[] val)
        {
            if (val == null)
                return null;
            else
                return TransStringForInput(AsciiEncoding.GetString(val));
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
                return TransStringForInput(AsciiEncoding.GetString(ba));
            }
            catch (Exception ex)
            {
                throw new MaxDBException(ex.Message, ex);
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
                    if (r == -1) r = 0;
                    byte[] ba2 = ba;
                    ba = new byte[r];
                    Array.Copy(ba2, 0, ba, 0, r);
                }
                return TransBytesForInput(ba);
            }
            catch (Exception ex)
            {
                throw new MaxDBException(ex.Message, ex);
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

            byte[] bytes = AsciiEncoding.GetBytes(arg);
            CheckFieldLimits(bytes.Length);
            return arg;
        }
    }

    #endregion

    #region "Unicode string data translator class"

    internal class UnicodeStringTranslator : StringTranslator
    {
        private Encoding mEncoding;

        public UnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                string result = mEncoding.GetString(mem.ReadBytes(iBufferPosition, iLogicalLength * Consts.UnicodeWidth));
                switch (byDataType)
                {
                    case DataType.VARCHARA:
                    case DataType.VARCHARB:
                    case DataType.VARCHARE:
                    case DataType.VARCHARUNI:
                        return result.TrimEnd();
                    default:
                        return result;
                }
            }
            else
                return null;
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetString(controller, mem);
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
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

            byte[] bytes = mEncoding.GetBytes(arg);
            CheckFieldLimits(bytes.Length);
            return bytes;
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            dataPart.WriteDefineByte((byte)1, iBufferPosition - 1);
            dataPart.WriteUnicodeBytes((byte[])data, iBufferPosition, iPhysicalLength - 1);
        }
    }

    #endregion

    #region "Space option string data translator class"

    internal class SpaceOptionStringTranslator : StringTranslator
    {
        public SpaceOptionStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            string result = null;

            if (!IsDBNull(mem))
            {
                result = base.GetString(controller, mem);
                if (result.Length == 0)
                    result = Consts.BlankChar;
            }
            return result;
        }
    }

    #endregion

    #region "Space option unicode string data translator class"

    internal class SpaceOptionUnicodeStringTranslator : UnicodeStringTranslator
    {
        public SpaceOptionUnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos, swapMode)
        {
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            string result = null;

            if (!IsDBNull(mem))
            {
                result = base.GetString(controller, mem);
                if (result.Length == 0)
                    result = Consts.BlankChar;
            }
            return result;
        }
    }

    #endregion

    #region "Binary data translator class"

    internal abstract class BinaryDataTranslator : DBTechTranslator
    {
        public BinaryDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            dataPart.WriteDefineByte(0, iBufferPosition - 1);
            dataPart.WriteBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }
    }

    #endregion

    #region "Bytes data translator class"

    internal class BytesTranslator : BinaryDataTranslator
    {
        public BytesTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            byte[] result = null;
            if (IsDBNull(mem))
                return 0;
            else
                result = mem.ReadBytes(iBufferPosition, 1);
            return result[0];
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            if (IsDBNull(mem))
                return null;
            else
                return mem.ReadBytes(iBufferPosition, 1);
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetBytes(controller, mem);
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
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
            byte[] barr = new byte[1];
            barr[0] = val;
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
            if (obj.GetType() == typeof(byte[]))
                return TransBytesForInput((byte[])obj);
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
                    if (r == -1) r = 0;
                    byte[] ba2 = ba;
                    ba = new byte[r];
                    Array.Copy(ba2, 0, ba, 0, r);
                }
                return TransBytesForInput(ba);
            }
            catch (Exception ex)
            {
                throw new MaxDBException(ex.Message, ex);
            }
        }

        public override Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
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
        public BooleanTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
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
                return (mem.ReadByte(iBufferPosition) == 0x00 ? false : true);
            else
                return false;
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            return GetBoolean(mem) ? (byte)1 : (byte)0;
        }

        public override float GetFloat(ByteArray mem)
        {
            return GetByte(null, mem);
        }

        public override double GetDouble(ByteArray mem)
        {
            return GetByte(null, mem);
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
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

            result[0] = (newValue ? (byte)1 : (byte)0);
            return result;
        }

        protected override object TransSpecificForInput(object obj)
        {
            Type type = obj.GetType();
            if (type == typeof(bool) || type == typeof(byte) || type == typeof(short) || type == typeof(int) ||
                type == typeof(long) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong) ||
                type == typeof(float) || type == typeof(double) || type == typeof(decimal))
                return TransBooleanForInput((bool)obj);
            else
                throw CreateSetException(obj.GetType().FullName);

        }

        public override object TransStringForInput(string val)
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

        public NumericTranslator(int mode, int ioType, int dataType, int len, int frac, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            switch (dataType)
            {
                case DataType.FLOAT:
                case DataType.VFLOAT:
                    // more digits are unreliable anyway
                    frac = 38;
                    isFloatingPoint = true;
                    break;
                default:
                    this.frac = frac;
                    break;
            }

        }

        public override BigDecimal GetBigDecimal(ByteArray mem)
        {
            BigDecimal result = null;
            try
            {
                switch (CheckDefineByte(mem))
                {
                    case DBTechTranslator.iNullDefineByte:
                        return result;
                    case DBTechTranslator.iSpecialNullValueDefineByte:
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSpecialNullValue), string.Empty, -10811);
                }
                result = VDNNumber.Number2BigDecimal(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1));
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

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            return (byte)GetInt64(mem);
        }

        public override double GetDouble(ByteArray mem)
        {
            switch (CheckDefineByte(mem))
            {
                case DBTechTranslator.iNullDefineByte:
                    return 0;
                case DBTechTranslator.iSpecialNullValueDefineByte:
                    return double.NaN;
            }
            return BigDecimal2Double(VDNNumber.Number2BigDecimal(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1)));
        }

        public override float GetFloat(ByteArray mem)
        {
            switch (CheckDefineByte(mem))
            {
                case DBTechTranslator.iNullDefineByte:
                    return 0;
                case DBTechTranslator.iSpecialNullValueDefineByte:
                    return float.NaN;
            }
            return BigDecimal2Float(VDNNumber.Number2BigDecimal(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1)));
        }

        public override decimal GetDecimal(ByteArray mem)
        {
            switch (CheckDefineByte(mem))
            {
                case DBTechTranslator.iNullDefineByte:
                case DBTechTranslator.iSpecialNullValueDefineByte:
                    return 0;
            }
            return BigDecimal2Decimal(VDNNumber.Number2BigDecimal(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1)));
        }

        public override int GetInt32(ByteArray mem)
        {
            return (int)GetInt64(mem);
        }

        public override long GetInt64(ByteArray mem)
        {
            switch (CheckDefineByte(mem))
            {
                case DBTechTranslator.iNullDefineByte:
                    return 0;
                case DBTechTranslator.iSpecialNullValueDefineByte:
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSpecialNullValue), string.Empty, -10811);
            }
            return VDNNumber.Number2Long(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1));
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            if (IsDBNull(mem))
                return null;

            switch (byDataType)
            {
                case DataType.FLOAT:
                case DataType.VFLOAT:
                    if (iLogicalLength < 15)
                        return GetFloat(mem);
                    else
                        return GetDouble(mem);
                case DataType.INTEGER:
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
                return iLogicalLength;
            }
        }

        public override int Scale
        {
            get
            {
                switch (byDataType)
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
                if (byDataType == DataType.FIXED)
                    bigD = bigD.setScale(frac);

                return VDNNumber.BigDecimal2Number(bigD, 16);
            }
            catch
            {
                // Detect nasty double values, and throw an SQL exception.
                if (double.IsInfinity(val) || double.IsNaN(val))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIAL_NUMBER_UNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
                else
                    throw;
            }
        }

        public override object TransFloatForInput(float val)
        {
            try
            {
                BigDecimal bigD = new BigDecimal(val);
                if (byDataType == DataType.FIXED)
                    bigD = bigD.setScale(frac);

                return VDNNumber.BigDecimal2Number(bigD, 14);
            }
            catch
            {
                // Detect nasty double values, and throw an SQL exception.
                if (float.IsInfinity(val) || float.IsNaN(val))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIAL_NUMBER_UNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
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
            Type type = obj.GetType();
            if (obj == null)
                return null;
            if (type == typeof(BigDecimal))
                return TransBigDecimalForInput((BigDecimal)obj);
            if (type == typeof(bool))
                return TransBooleanForInput((bool)obj);
            if (type == typeof(byte))
                return TransByteForInput((byte)obj);
            if (type == typeof(double))
                return TransDoubleForInput((double)obj);
            if (type == typeof(float))
                return TransFloatForInput((float)obj);
            if (type == typeof(int))
                return TransInt32ForInput((int)obj);
            if (type == typeof(long))
                return TransInt64ForInput((long)obj);
            if (type == typeof(short))
                return TransInt16ForInput((short)obj);

            return null;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            switch (CheckDefineByte(mem))
            {
                case DBTechTranslator.iNullDefineByte:
                    return null;
                case DBTechTranslator.iSpecialNullValueDefineByte:
                    return double.NaN.ToString(CultureInfo.InvariantCulture);
            }
            return VDNNumber.Number2String(mem.ReadBytes(iBufferPosition, iPhysicalLength - 1),
                (byDataType != DataType.FLOAT && byDataType != DataType.VFLOAT), iLogicalLength, frac);
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

        public TimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetDateTime(mem);
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadAscii(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
                return mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);
            else
                return null;
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

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
            formattedTime[2] = (byte)':';

            formattedTime[3] = (byte)('0' + (dt.Minute / 10));
            formattedTime[4] = (byte)('0' + (dt.Minute % 10));
            formattedTime[5] = (byte)':';

            formattedTime[6] = (byte)('0' + (dt.Second / 10));
            formattedTime[7] = (byte)('0' + (dt.Second % 10));

            return formattedTime;
        }

        public virtual object TransTimeForInput(TimeSpan ts)
        {
            return TransDateTimeForInput(DateTime.MinValue.AddTicks(ts.Ticks));
        }

        protected override object TransSpecificForInput(object obj)
        {
            return (obj is DateTime) ? TransTimeForInput((DateTime)obj) : TransTimeForInput((TimeSpan)obj);
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;

            try
            {
                return TransTimeForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
            }
            catch
            {
                try
                {
                    return TransTimeForInput(TimeSpan.Parse(val));
                }
                catch
                {
                    throw CreateParseException(val, "Time");
                }
            }

        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(-1);
            dataPart.WriteDefineByte((byte)' ', iBufferPosition - 1);
            dataPart.WriteAsciiBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }
    }

    #endregion

    #region "Unicode time data translator"

    internal class UnicodeTimeTranslator : TimeTranslator
    {
        private Encoding mEncoding;

        public UnicodeTimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadUnicode(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

                int offset = (mEncoding == Encoding.Unicode ? 1 : 0);

                int hour = ((int)raw[1 - offset] - '0') * 10 + ((int)raw[3 - offset] - '0');
                int min = ((int)raw[7 - offset] - '0') * 10 + ((int)raw[9 - offset] - '0');
                int sec = ((int)raw[13 - offset] - '0') * 10 + ((int)raw[15 - offset] - '0');

                return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
            }
            else
                return DateTime.MinValue;
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(DataType.StrValues[byDataType], -1);
            dataPart.WriteDefineByte(Consts.DefinedUnicode, iBufferPosition - 1);
            dataPart.WriteUnicodeBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }

        public override object TransTimeForInput(DateTime dt)
        {
            byte[] bytes = mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransTimeForInput(dt)));
            CheckFieldLimits(bytes.Length);
            return bytes;
        }
    }

    #endregion

    #region "Timestamp data translator"

    internal class TimestampTranslator : CharDataTranslator
    {
        private const int TimestampSize = 26;

        public TimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetDateTime(mem);
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadAscii(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
                return mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);
            else
                return null;
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

                int year = ((int)raw[0] - '0') * 1000 + ((int)raw[1] - '0') * 100 + ((int)raw[2] - '0') * 10 + ((int)raw[3] - '0');
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
            byte[] formattedTimestamp = new byte[TimestampSize];

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
            formattedTimestamp[4] = (byte)'-';

            formattedTimestamp[5] = (byte)HighTime[month];
            formattedTimestamp[6] = (byte)LowTime[month];
            formattedTimestamp[7] = (byte)'-';

            formattedTimestamp[8] = (byte)HighTime[day];
            formattedTimestamp[9] = (byte)LowTime[day];
            formattedTimestamp[10] = (byte)' ';

            formattedTimestamp[11] = (byte)HighTime[hour];
            formattedTimestamp[12] = (byte)LowTime[hour];
            formattedTimestamp[13] = (byte)':';

            formattedTimestamp[14] = (byte)('0' + (minute / 10));
            formattedTimestamp[15] = (byte)('0' + (minute % 10));
            formattedTimestamp[16] = (byte)':';

            formattedTimestamp[17] = (byte)('0' + (second / 10));
            formattedTimestamp[18] = (byte)('0' + (second % 10));
            formattedTimestamp[19] = (byte)'.';

            formattedTimestamp[20] = (byte)('0' + (milli / 100));
            milli %= 100;
            formattedTimestamp[21] = (byte)('0' + (milli / 10));
            milli %= 10;
            formattedTimestamp[22] = (byte)('0' + milli);

            formattedTimestamp[23] = (byte)('0' + (nano / 100));
            nano %= 100;
            formattedTimestamp[24] = (byte)('0' + (nano / 10));
            nano %= 10;
            formattedTimestamp[25] = (byte)('0' + nano);

            return formattedTimestamp;
        }

        protected override object TransSpecificForInput(object obj)
        {
            return (obj.GetType() == typeof(DateTime)) ? TransTimestampForInput((DateTime)obj) : null;
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;

            try
            {
                return TransSpecificForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
            }
            catch
            {
                throw CreateParseException(val, "Timestamp");
            }
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(-1);
            dataPart.WriteDefineByte((byte)' ', iBufferPosition - 1);
            dataPart.WriteAsciiBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }
    }

    #endregion

    #region "Unicode timestamp data translator"

    internal class UnicodeTimestampTranslator : TimestampTranslator
    {
        private Encoding mEncoding;

        public UnicodeTimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadUnicode(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

                int offset = (mEncoding == Encoding.Unicode ? 1 : 0);

                int year = ((int)raw[1 - offset] - '0') * 1000 + ((int)raw[3 - offset] - '0') * 100 +
                    ((int)raw[5 - offset] - '0') * 10 + ((int)raw[7 - offset] - '0');
                int month = ((int)raw[11 - offset] - '0') * 10 + ((int)raw[13 - offset] - '0');
                int day = ((int)raw[17 - offset] - '0') * 10 + ((int)raw[19 - offset] - '0');
                int hour = ((int)raw[23 - offset] - '0') * 10 + ((int)raw[25 - offset] - '0');
                int min = ((int)raw[29 - offset] - '0') * 10 + ((int)raw[31 - offset] - '0');
                int sec = ((int)raw[35 - offset] - '0') * 10 + ((int)raw[37 - offset] - '0');
                int milli = ((int)raw[41 - offset] - '0') * 100 + ((int)raw[43 - offset] - '0') * 10 + ((int)raw[45 - offset] - '0');
                int nano = ((int)raw[47 - offset] - '0') * 100 + ((int)raw[49 - offset] - '0') * 10 + ((int)raw[51 - offset] - '0');

                return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(nano * TimeSpan.TicksPerMillisecond / 1000);
            }
            else
                return DateTime.MinValue;
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(DataType.StrValues[byDataType], -1);
            dataPart.WriteDefineByte(Consts.DefinedUnicode, iBufferPosition - 1);
            dataPart.WriteUnicodeBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }

        public override object TransTimestampForInput(DateTime dt)
        {
            byte[] bytes = mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransTimestampForInput(dt)));
            CheckFieldLimits(bytes.Length);
            return bytes;
        }
    }

    #endregion

    #region "Date data translator"

    internal class DateTranslator : CharDataTranslator
    {
        private const int DateSize = 10;

        public DateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetDateTime(mem);
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadAscii(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
                return mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);
            else
                return null;
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

                int year = ((int)raw[0] - '0') * 1000 + ((int)raw[1] - '0') * 100 + ((int)raw[2] - '0') * 10 + ((int)raw[3] - '0');
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
            return (obj.GetType() == typeof(DateTime)) ? TransDateForInput((DateTime)obj) : null;
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;

            try
            {
                return TransSpecificForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
            }
            catch
            {
                throw CreateParseException(val, "Date");
            }
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(-1);
            dataPart.WriteDefineByte((byte)' ', iBufferPosition - 1);
            dataPart.WriteAsciiBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }
    }

    #endregion

    #region "Unicode date data translator"

    internal class UnicodeDateTranslator : DateTranslator
    {
        Encoding mEncoding;

        public UnicodeDateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
        {
            return (!IsDBNull(mem) ? mem.ReadUnicode(iBufferPosition, iPhysicalLength - 1) : null);
        }

        public override DateTime GetDateTime(ByteArray mem)
        {
            if (!IsDBNull(mem))
            {
                byte[] raw = mem.ReadBytes(iBufferPosition, iPhysicalLength - 1);

                int offset = (mEncoding == Encoding.Unicode ? 1 : 0);

                int year = ((int)raw[1 - offset] - '0') * 1000 + ((int)raw[3 - offset] - '0') * 100 +
                    ((int)raw[5 - offset] - '0') * 10 + ((int)raw[7 - offset] - '0');
                int month = ((int)raw[11 - offset] - '0') * 10 + ((int)raw[13 - offset] - '0');
                int day = ((int)raw[17 - offset] - '0') * 10 + ((int)raw[19 - offset] - '0');

                return new DateTime(year, month, day);
            }
            else
                return DateTime.MinValue;
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            if (bytes.Length > iPhysicalLength - 1)
                throw new MaxDBValueOverflowException(DataType.StrValues[byDataType], -1);
            dataPart.WriteDefineByte(Consts.DefinedUnicode, iBufferPosition - 1);
            dataPart.WriteUnicodeBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }

        public override object TransDateForInput(DateTime dt)
        {
            byte[] bytes = mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransDateForInput(dt)));
            CheckFieldLimits(bytes.Length);
            return bytes;
        }
    }

    #endregion

    #region "Structured data translator"

    internal class StructureTranslator : DBTechTranslator
    {
        StructMemberTranslator[] mStructureConverter;
        bool bUnicode;

        public StructureTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool unicode)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
            bUnicode = unicode;
            mStructureConverter = new StructMemberTranslator[0];
        }

        protected override void PutSpecific(DataPart dataPart, object data)
        {
            byte[] bytes = (byte[])data;
            dataPart.WriteDefineByte(0, iBufferPosition - 1);
            dataPart.WriteBytes(bytes, iBufferPosition, iPhysicalLength - 1);
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            byte[] result = null;
            if (IsDBNull(mem))
                return 0;
            else
                result = mem.ReadBytes(iBufferPosition, 1);
            return result[0];
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
        {
            if (!IsDBNull(mem))
                return mem.ReadBytes(iBufferPosition, iLogicalLength);
            else
                return null;
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            byte[] ba = GetBytes(null, mem);
            if (ba != null)
            {
                object[] objArr = new object[mStructureConverter.Length];
                ByteArray sb = new ByteArray(ba, 0, mem.Swapped);
                for (int i = 0; i < objArr.Length; i++)
                    objArr[i] = mStructureConverter[i].GetValue(sb, 0);

                return new DBProcStructure(objArr);
            }
            else
                return null;
        }

        public override object TransByteForInput(byte val)
        {
            return TransBytesForInput(new byte[1] { val });
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

            if (obj.GetType() == typeof(byte[]))
                result = TransBytesForInput((byte[])obj);
            else if (obj.GetType() == typeof(object[]))
                result = TransObjectArrayForInput((object[])obj);
            else if (obj.GetType() == typeof(DBProcStructure))
                result = TransObjectArrayForInput(((DBProcStructure)obj).Attributes);

            return result;
        }

        public object TransObjectArrayForInput(object[] objectArray)
        {
            if (objectArray.Length != mStructureConverter.Length)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTURE_ARRAYWRONGLENTGH,
                    mStructureConverter.Length, objectArray.Length));

            ByteArray sb = new ByteArray(iPhysicalLength - 1);
            for (int i = 0; i < objectArray.Length; i++)
            {
                if (objectArray[i] == null)
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_NULL, i + 1));

                mStructureConverter[i].PutValue(sb, objectArray[i]);
            }
            return sb.GetArrayData();
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

                return TransStringForInput(bUnicode ? Encoding.Unicode.GetString(ba) : Encoding.ASCII.GetString(ba));
            }
            catch (Exception ex)
            {
                throw new MaxDBException(ex.Message, ex);
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
                throw new MaxDBException(ex.Message, ex);
            }
        }

        public override Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            byte[] asBytes = GetBytes(null, mem);

            if (asBytes == null)
                return null;

            return new MemoryStream(asBytes);
        }

        public override void SetProcParamInfo(DBProcParameterInfo info)
        {
            mStructureConverter = StructMemberTranslator.CreateStructMemberTranslators(info, bUnicode);
        }
    }

    #endregion

    #region "Structured member data translator"

    internal abstract class StructMemberTranslator
    {
        protected StructureElement mStructElement;
        protected int iOffset;

        public StructMemberTranslator(StructureElement structElement, bool unicode)
        {
            mStructElement = structElement;
            iOffset = unicode ? structElement.iUnicodeOffset : structElement.iASCIIOffset;
        }

        public abstract object GetValue(ByteArray memory, int recordOffset);
        public abstract void PutValue(ByteArray memory, object obj);

        protected void ThrowConversionError(string srcObj)
        {
            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_CONVERSION, mStructElement.SqlTypeName, srcObj));
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
            if (string.Compare(s.strTypeName.Trim(), "CHAR", true, CultureInfo.InvariantCulture) == 0)
            {
                if (string.Compare(s.strCodeType.Trim(), "BYTE", true, CultureInfo.InvariantCulture) == 0)
                    return new ByteStructureElementTranslator(s, unicode);
                else if (string.Compare(s.strCodeType.Trim(), "ASCII", true, CultureInfo.InvariantCulture) == 0)
                    return new CharASCIIStructureElementTranslator(s, unicode);
            }
            else if (string.Compare(s.strTypeName.Trim(), "WYDE", true, CultureInfo.InvariantCulture) == 0)
            {
                if (unicode)
                    return new WydeStructureElementTranslator(s, unicode);
                else
                    return new CharASCIIStructureElementTranslator(s, unicode);
            }
            else if (string.Compare(s.strTypeName.Trim(), "SMALLINT", true, CultureInfo.InvariantCulture) == 0)
            {
                if (s.iLength == 5)
                    return new ShortStructureElementTranslator(s, unicode);
            }
            else if (string.Compare(s.strTypeName.Trim(), "INTEGER", true, CultureInfo.InvariantCulture) == 0)
            {
                if (s.iLength == 10)
                    return new IntStructureElementTranslator(s, unicode);
                else if (s.iLength == 19)
                    return new LongStructureElementTranslator(s, unicode);
            }
            else if (string.Compare(s.strTypeName.Trim(), "FIXED", true, CultureInfo.InvariantCulture) == 0)
            {
                if (s.iPrecision == 0)
                {
                    if (s.iLength == 5)
                        return new ShortStructureElementTranslator(s, unicode);
                    else if (s.iLength == 10)
                        return new IntStructureElementTranslator(s, unicode);
                    else if (s.iLength == 19)
                        return new LongStructureElementTranslator(s, unicode);
                }
            }
            else if (string.Compare(s.strTypeName.Trim(), "FLOAT", true, CultureInfo.InvariantCulture) == 0)
            {
                if (s.iLength == 15)
                    return new DoubleStructureElementTranslator(s, unicode);
                else if (s.iLength == 6)
                    return new FloatStructureElementTranslator(s, unicode);
            }
            else if (string.Compare(s.strTypeName.Trim(), "BOOLEAN", true, CultureInfo.InvariantCulture) == 0)
                return new BooleanStructureElementTranslator(s, unicode);

            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSION_STRUCTURETYPE, index, s.SqlTypeName));
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

            for (int i = 0; i < result.Length; ++i)
                result[i] = CreateStructMemberTranslator(info, i, unicode);

            return result;
        }
    }

    #endregion

    #region "BOOLEAN structure element data translator"

    internal class BooleanStructureElementTranslator : StructMemberTranslator
    {
        public BooleanStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return (memory.ReadByte(iOffset + recordOffset) != 0);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            if (obj.GetType() == typeof(bool))
                memory.WriteByte((byte)((bool)obj ? 1 : 0), iOffset);
            else
                ThrowConversionError(obj.GetType().FullName);
        }
    }

    #endregion

    #region "CHAR BYTE structure element data translator"

    internal class ByteStructureElementTranslator : StructMemberTranslator
    {
        public ByteStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            byte[] bytes = memory.ReadBytes(iOffset + recordOffset, mStructElement.iLength);
            if (mStructElement.iLength == 1)
                return bytes[0];
            else
                return bytes;
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            if (obj.GetType() == typeof(byte[]))
                memory.WriteBytes((byte[])obj, iOffset);
            else if (obj.GetType() == typeof(byte))
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

        public CharASCIIStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            byte[] bytes = memory.ReadBytes(iOffset + recordOffset, mStructElement.iLength);
            if (mStructElement.iLength == 1)
                return (char)bytes[0];
            else
                return Encoding.ASCII.GetString(bytes);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            string convStr = null;
            if (obj.GetType() == typeof(char[]))
                convStr = new string((char[])obj);
            else if (obj.GetType() == typeof(string))
                convStr = (string)obj;
            else if (obj.GetType() == typeof(char))
                convStr = new string(new char[] { (char)obj });
            else
                ThrowConversionError(obj.GetType().FullName);

            memory.WriteAscii(convStr, iOffset);
        }
    }

    #endregion

    #region "WYDE structure element data translator"

    internal class WydeStructureElementTranslator : StructMemberTranslator
    {
        public WydeStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            string ca = memory.ReadUnicode(iOffset + recordOffset, mStructElement.iLength * 2);
            if (mStructElement.iLength == 1)
                return ca[0];
            else
                return ca;
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            string convStr = null;
            if (obj.GetType() == typeof(char[]))
                convStr = new string((char[])obj);
            else if (obj.GetType() == typeof(string))
                convStr = (string)obj;
            else if (obj.GetType() == typeof(char))
                convStr = new string(new char[] { (char)obj });
            else
                ThrowConversionError(obj.GetType().FullName);

            memory.WriteUnicode(convStr, iOffset);
        }
    }

    #endregion

    #region "SMALLINT structure element data translator"

    internal class ShortStructureElementTranslator : StructMemberTranslator
    {
        public ShortStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return memory.ReadInt16(iOffset + recordOffset);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            Type type = obj.GetType();
            if (type == typeof(byte) || type == typeof(short) || type == typeof(int) || type == typeof(long)
                || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong)
                || (type == typeof(float) && (float)obj <= short.MaxValue && (float)obj >= short.MinValue)
                || (type == typeof(double) && (double)obj <= short.MaxValue && (double)obj >= short.MinValue)
                || (type == typeof(decimal) && (decimal)obj <= short.MaxValue && (decimal)obj >= short.MinValue))
                memory.WriteInt16((short)obj, iOffset);
            else
            {
                if (type == typeof(float) && ((float)obj > short.MaxValue || (float)obj < short.MinValue)
                    || (type == typeof(double) && ((double)obj > short.MaxValue || (double)obj < short.MinValue)
                    || (type == typeof(decimal) && ((decimal)obj > short.MaxValue || (decimal)obj < short.MinValue))))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_OVERFLOW, mStructElement.SqlTypeName, obj.ToString()));
                else
                    ThrowConversionError(obj.GetType().FullName);
            }
        }
    }

    #endregion

    #region "INTEGER structure element data translator"

    internal class IntStructureElementTranslator : StructMemberTranslator
    {
        public IntStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return memory.ReadInt32(iOffset + recordOffset);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            Type type = obj.GetType();
            if (type == typeof(byte) || type == typeof(short) || type == typeof(int) || type == typeof(long)
                || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong)
                || (type == typeof(float) && (float)obj <= int.MaxValue && (float)obj >= int.MinValue)
                || (type == typeof(double) && (double)obj <= int.MaxValue && (double)obj >= int.MinValue)
                || (type == typeof(decimal) && (decimal)obj <= int.MaxValue && (decimal)obj >= int.MinValue))
                memory.WriteInt32((int)obj, iOffset);
            else
            {
                if (type == typeof(float) && ((float)obj > int.MaxValue || (float)obj < int.MinValue)
                    || (type == typeof(double) && ((double)obj > int.MaxValue || (double)obj < int.MinValue)
                    || (type == typeof(decimal) && ((decimal)obj > int.MaxValue || (decimal)obj < int.MinValue))))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_OVERFLOW, mStructElement.SqlTypeName, obj.ToString()));
                else
                    ThrowConversionError(obj.GetType().FullName);
            }
        }
    }

    #endregion

    #region "LONG structure element data translator"

    internal class LongStructureElementTranslator : StructMemberTranslator
    {

        public LongStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return memory.ReadInt64(iOffset + recordOffset);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            Type type = obj.GetType();
            if (type == typeof(byte) || type == typeof(short) || type == typeof(int) || type == typeof(long)
                || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong)
                || (type == typeof(float) && (float)obj <= long.MaxValue && (float)obj >= long.MinValue)
                || (type == typeof(double) && (double)obj <= long.MaxValue && (double)obj >= long.MinValue)
                || (type == typeof(decimal) && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
                memory.WriteInt64((long)obj, iOffset);
            else
            {
                if (type == typeof(float) && ((float)obj > long.MaxValue || (float)obj < long.MinValue)
                    || (type == typeof(double) && ((double)obj > long.MaxValue || (double)obj < long.MinValue)
                    || (type == typeof(decimal) && ((decimal)obj > long.MaxValue || (decimal)obj < long.MinValue))))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_OVERFLOW, mStructElement.SqlTypeName, obj.ToString()));
                else
                    ThrowConversionError(obj.GetType().FullName);
            }
        }
    }

    #endregion

    #region "FLOAT structure element data translator"

    internal class FloatStructureElementTranslator : StructMemberTranslator
    {
        public FloatStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return BitConverter.ToSingle(memory.ReadBytes(iOffset, 4), 0);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            Type type = obj.GetType();
            if (type == typeof(byte) || type == typeof(short) || type == typeof(int) || type == typeof(long)
                || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong) || type == typeof(float)
                || (type == typeof(double) && (double)obj <= long.MaxValue && (double)obj >= float.MinValue)
                || (type == typeof(decimal) && (decimal)obj <= long.MaxValue))
                memory.WriteBytes(BitConverter.GetBytes((float)obj), iOffset);
            else
            {
                if ((type == typeof(double) && ((double)obj > long.MaxValue || (double)obj < float.MinValue)
                    || (type == typeof(decimal) && ((decimal)obj > long.MaxValue))))
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCT_ELEMENT_OVERFLOW, mStructElement.SqlTypeName, obj.ToString()));
                else
                    ThrowConversionError(obj.GetType().FullName);
            }
        }
    }

    #endregion

    #region "DOUBLE structure element data translator"

    internal class DoubleStructureElementTranslator : StructMemberTranslator
    {
        public DoubleStructureElementTranslator(StructureElement structElement, bool unicode)
            : base(structElement, unicode)
        {
        }

        public override object GetValue(ByteArray memory, int recordOffset)
        {
            return BitConverter.ToDouble(memory.ReadBytes(iOffset, 8), 0);
        }

        public override void PutValue(ByteArray memory, object obj)
        {
            if (obj is byte || obj is short || obj is int || obj is long
                || obj is ushort || obj is uint || obj is ulong || obj is float || obj is double
                || (obj is decimal && (decimal)obj <= long.MaxValue && (decimal)obj >= long.MinValue))
                memory.WriteBytes(BitConverter.GetBytes((double)obj), iOffset);
            else
                ThrowConversionError(obj.GetType().FullName);
        }
    }

    #endregion

    #region "Translator for LONG arguments of DB Procedures"

    internal class ProcedureStreamTranslator : DBTechTranslator
    {
        public ProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
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
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSION_BYTESTREAM));
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;

            if (IsASCII)
                return new ASCIIProcedurePutValue(this, Encoding.ASCII.GetBytes(val));
            else
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSION_STRINGSTREAM));
        }

        public override object TransCharacterStreamForInput(Stream stream, int length)
        {
            if (IsASCII)
            {
                if (stream == null)
                    return null;
                else
                    return new ASCIIProcedurePutValue(this, stream, length);
            }
            else
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSION_STRINGSTREAM));
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
            AbstractProcedurePutValue putval = (AbstractProcedurePutValue)data;
            putval.putDescriptor(dataPart);
        }

        protected override object TransSpecificForInput(object obj)
        {
            if (obj.GetType() == typeof(Stream))
                return TransASCIIStreamForInput((Stream)obj, -1);
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
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSION_STRINGSTREAM));
        }

        private bool IsASCII
        {
            get
            {
                return byDataType == DataType.STRA || byDataType == DataType.LONGA;
            }
        }

        private bool IsBinary
        {
            get
            {
                return byDataType == DataType.STRB || byDataType == DataType.LONGB;
            }
        }

        private Stream GetStream(ISqlParameterController controller, ByteArray mem, ByteArray longdata)
        {
            Stream result = null;
            AbstractGetValue getval = null;
            byte[] descriptor;

            if (!IsDBNull(mem))
            {
                descriptor = mem.ReadBytes(iBufferPosition, iLogicalLength);
                // return also NULL if the LONG hasn't been touched.
                if (IsDescriptorNull(descriptor))
                    return null;

                getval = new GetLOBValue(controller.Connection, descriptor, longdata);
                result = getval.ASCIIStream;
            }
            return result;
        }

        public override Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            return GetStream(controller, mem, longData);
        }

        public override Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            if (IsBinary)
                return GetStream(controller, mem, longData);
            else
                throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYREADFROMLONG));
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            byte[] result = null;
            if (IsDBNull(mem))
                return 0;
            else
                result = GetBytes(controller, mem);
            return result[0];
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
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

        public virtual TextReader GetCharacterStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            Stream byteStream = GetASCIIStream(controller, mem, longData);
            if (byteStream == null)
                return null;

            return new RawByteReader(byteStream);
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
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
                throw new DataException(exc.Message, exc);
            }
            return result.ToString();
        }

        public override long GetChars(ISqlParameterController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
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

        public override long GetBytes(ISqlParameterController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
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

        protected static bool IsDescriptorNull(byte[] descriptor)
        {
            return descriptor[LongDesc.State] == LongDesc.StateStream;
        }
    }

    #endregion

    #region "Translator for LONG UNICODE arguments of DB Procedures"

    internal class UnicodeProcedureStreamTranslator : ProcedureStreamTranslator
    {
        public UnicodeProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
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

        public override Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            TextReader reader = GetCharacterStream(controller, mem, longData);
            if (reader == null)
                return null;

            return new ReaderStream(reader, false);
        }

        public override TextReader GetCharacterStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            TextReader result = null;
            AbstractGetValue getval;
            byte[] descriptor;

            if (!IsDBNull(mem))
            {
                descriptor = mem.ReadBytes(iBufferPosition, iLogicalLength);
                if (IsDescriptorNull(descriptor))
                    return null;

                getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData);
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
        protected StreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        public override int ColumnDisplaySize
        {
            get
            {
                switch (byDataType)
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

        public override Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIREADFROMLONG));
        }

        public override Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            return GetStream(controller, mem, longData);
        }

        public virtual TextReader GetCharacterStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            Stream byteStream = GetASCIIStream(controller, mem, longData);
            if (byteStream == null)
                return null;

            return new RawByteReader(byteStream);
        }

        public Stream GetStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            Stream result = null;
            AbstractGetValue getval = null;
            byte[] descriptor;

            if (!IsDBNull(mem))
            {
                descriptor = mem.ReadBytes(iBufferPosition, iLogicalLength);
                getval = new GetLOBValue(controller.Connection, descriptor, longData);

                result = getval.ASCIIStream;
            }
            return result;
        }

        public override string GetString(ISqlParameterController controller, ByteArray mem)
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

        public override long GetChars(ISqlParameterController controller, ByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
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

        public override long GetBytes(ISqlParameterController controller, ByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
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
            PutValue putval = (PutValue)data;
            putval.PutDescriptor(dataPart, iBufferPosition - 1);
        }

        public virtual object TransASCIIStreamForInput(Stream stream, int length)
        {
            throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIPUTTOLONG));
        }

        public override object TransBinaryStreamForInput(Stream stream, int length)
        {
            throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYPUTTOLONG));
        }

        /**
         * Translates a byte array. This is done only in derived classes
         * that accept byte arrays (this one may be a BLOB or a CLOB,
         * and so does not decide about it).
         * @param val The byte array to bind.
         * @return The Putval instance created for this one.
         */
        public override object TransBytesForInput(byte[] val)
        {
            throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYPUTTOLONG));
        }

        protected override object TransSpecificForInput(object obj)
        {
            if (obj.GetType() == typeof(Stream))
                return TransASCIIStreamForInput((Stream)obj, -1);
            else
                return null;
        }

        public object TransStreamForInput(Stream stream, int length)
        {
            if (stream == null)
                return null;

            return new PutValue(stream, length, iBufferPosition);
        }

        public override object TransStringForInput(string val)
        {
            throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIPUTTOLONG));
        }
    }

    #endregion

    #region "ASCII stream translator"

    internal class ASCIIStreamTranslator : StreamTranslator
    {
        public ASCIIStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            : base(mode, ioType, dataType, len, ioLen, bufpos)
        {
        }

        private Encoding asciiEncoding;

        public Encoding AsciiEncoding
        {
            get
            {
                if (asciiEncoding == null)
                {
                    return Encoding.GetEncoding(1252);
                }
                return asciiEncoding;
            }
            set { asciiEncoding = value; }
        }

        public override Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            return GetStream(controller, mem, longData);
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetString(controller, mem);
        }

        public override object TransASCIIStreamForInput(Stream stream, int length)
        {
            return TransStreamForInput(stream, length);
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;
            return new PutValue(AsciiEncoding.GetBytes(val), iBufferPosition);
        }
    }

    #endregion

    #region "Binary stream translator"

    internal class BinaryStreamTranslator : StreamTranslator
    {
        public BinaryStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen , bufpos)
        {
        }

        public override Stream GetBinaryStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            return GetStream(controller, mem, longData);
        }

        public override byte GetByte(ISqlParameterController controller, ByteArray mem)
        {
            if (IsDBNull(mem))
                return 0;

            return GetBytes(controller, mem)[0];
        }

        public override byte[] GetBytes(ISqlParameterController controller, ByteArray mem)
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
                readLen = blobStream.Read(buf, 0, bufSize);
                while (readLen > 0)
                {
                    tmpStream.Write(buf, 0, readLen);
                    if (readLen < bufSize)
                        break;

                    readLen = blobStream.Read(buf, 0, bufSize);
                }
            }
            catch (StreamIOException sqlExc)
            {
                throw sqlExc.SqlException;
            }
            catch (IOException ioExc)
            {
                throw new DataException(ioExc.Message);
            }
            return tmpStream.ToArray();
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
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

            return new PutValue(val, iBufferPosition);
        }

        public override object TransBinaryStreamForInput(Stream stream, int length)
        {
            return TransStreamForInput(stream, length);
        }

        protected override object TransSpecificForInput(object obj)
        {
            if (obj.GetType() == typeof(Stream))
                return TransBinaryStreamForInput((Stream)obj, -1);
            else
                return null;
        }
    }

    #endregion

    #region "UNICODE stream translator"

    internal class UnicodeStreamTranslator : StreamTranslator
    {
        public UnicodeStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            :
            base(mode, ioType, dataType, len, ioLen , bufpos)
        {
        }

        public override Stream GetASCIIStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            TextReader reader = GetCharacterStream(controller, mem, longData);
            if (reader == null)
                return null;

            return new ReaderStream(reader, false);
        }

        public override object GetValue(ISqlParameterController controller, ByteArray mem)
        {
            return GetString(controller, mem);
        }

        public override TextReader GetCharacterStream(ISqlParameterController controller, ByteArray mem, ByteArray longData)
        {
            TextReader result = null;
            AbstractGetValue getval;
            byte[] descriptor;

            if (!IsDBNull(mem))
            {
                descriptor = mem.ReadBytes(iBufferPosition, iLogicalLength);
                getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData);
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

            return new PutUnicodeValue(reader, length, iBufferPosition);
        }

        public override object TransStringForInput(string val)
        {
            if (val == null)
                return null;

            return new PutUnicodeValue(val.ToCharArray(), iBufferPosition);
        }
    }

    #endregion

#endif // SAFE
}
