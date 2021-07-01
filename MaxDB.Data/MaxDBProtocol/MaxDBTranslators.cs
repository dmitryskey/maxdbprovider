//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBTranslators.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data.MaxDBProtocol
{
    using MaxDB.Data.Interfaces.MaxDBProtocol;
    using MaxDB.Data.Interfaces.Utils;
    using MaxDB.Data.Utils;
    using System;
    using System.Data;
    using System.Globalization;
    using System.IO;
    using System.Text;

    /// <summary>
    /// MaxDB translators.
    /// </summary>
    internal class MaxDBTranslators
    {
        /// <summary>
        /// DB Tech translator class.
        /// </summary>
        internal abstract class DBTechTranslator : IDBTechTranslator
        {
            public const int iNullDefineByte = 1;
            public const int iSpecialNullValueDefineByte = 2;
            public const int iUnknownDefineByte = -1;

            protected int iLogicalLength;
            protected int iPhysicalLength;
            protected int iBufferPosition;   // bufpos points to actual data, defbyte is at -1
            protected byte byMode;
            protected byte byIOType;
            protected byte byDataType;

            protected DBTechTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
            {
                this.byMode = (byte)mode;
                this.byIOType = (byte)ioType;
                this.byDataType = (byte)dataType;
                this.iLogicalLength = len;
                this.iPhysicalLength = ioLen;
                this.iBufferPosition = bufpos;
            }

            public int BufPos => this.iBufferPosition;

            public virtual int Precision => this.iLogicalLength;

            public virtual int Scale => 0;

            public int PhysicalLength => this.iPhysicalLength;

            public int ColumnIndex
            {
                get; set;
            }

            public string ColumnTypeName => GeneralColumnInfo.GetTypeName(this.byDataType);

            public Type ColumnDataType => GeneralColumnInfo.GetType(this.byDataType);

            public MaxDBType ColumnProviderType => GeneralColumnInfo.GetMaxDBType(this.byDataType);

            public virtual int ColumnDisplaySize => this.iLogicalLength;

            public string ColumnName
            {
                get; set;
            }

            protected MaxDBConversionException CreateGetException(string requestedType) =>
                new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSQLNET, this.ColumnTypeName, requestedType));

            protected MaxDBConversionException CreateSetException(string requestedType) =>
                new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.CONVERSIONNETSQL, requestedType, this.ColumnTypeName));

            protected MaxDBConversionException CreateParseException(string data, string requestedType) =>
                new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.CONVERSIONDATA, data, requestedType ?? this.ColumnTypeName));

            public virtual Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => throw this.CreateGetException("ASCIIStream");

            public virtual Stream GetUnicodeStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => throw this.CreateGetException("UnicodeStream");

            public virtual Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => throw this.CreateGetException("BinaryStream");

            public virtual bool GetBoolean(IByteArray mem) => throw this.CreateGetException(typeof(bool).ToString());

            public virtual byte GetByte(ISqlParameterController controller, IByteArray mem) => throw this.CreateGetException(typeof(byte).ToString());

            public virtual byte[] GetBytes(ISqlParameterController controller, IByteArray mem) => throw this.CreateGetException(typeof(byte[]).ToString());

            public virtual long GetBytes(ISqlParameterController controller, IByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
            {
                byte[] bytes = this.GetBytes(controller, mem);
                if (bytes == null)
                {
                    return 0;
                }

                Array.Copy(bytes, fldOffset, buffer, bufferoffset, bytes.Length);
                return bytes.Length;
            }

            public virtual DateTime GetDateTime(IByteArray mem) => throw this.CreateGetException(typeof(DateTime).ToString());

            public virtual double GetDouble(IByteArray mem) => throw this.CreateGetException(typeof(double).ToString());

            public virtual float GetFloat(IByteArray mem) => throw this.CreateGetException(typeof(float).ToString());

            public virtual BigDecimal GetBigDecimal(IByteArray mem) => throw this.CreateGetException(typeof(decimal).ToString());

            public virtual decimal GetDecimal(IByteArray mem) => throw this.CreateGetException(typeof(decimal).ToString());

            public virtual short GetInt16(IByteArray mem) => throw this.CreateGetException(typeof(short).ToString());

            public virtual int GetInt32(IByteArray mem) => throw this.CreateGetException(typeof(int).ToString());

            public virtual long GetInt64(IByteArray mem) => throw this.CreateGetException(typeof(long).ToString());

            public virtual object GetValue(ISqlParameterController controller, IByteArray mem) => throw this.CreateGetException(typeof(object).ToString());

            public virtual object[] GetValues(IByteArray mem) => throw this.CreateGetException(typeof(object[]).ToString());

            public virtual string GetString(ISqlParameterController controller, IByteArray mem) => this.GetValue(controller, mem)?.ToString();

            public virtual long GetChars(ISqlParameterController controller, IByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
            {
                string str = this.GetString(controller, mem);
                if (str == null)
                {
                    return 0;
                }

                char[] chars = str.Substring((int)fldOffset, length).ToCharArray();
                Array.Copy(chars, 0, buffer, bufferoffset, chars.Length);
                return chars.Length;
            }

            public virtual bool IsCaseSensitive => false;

            public virtual bool IsCurrency => false;

            public virtual bool IsDefinitelyWritable => false;

            public virtual bool IsInput => this.byIOType != ParamInfo.Output;

            public virtual bool IsOutput => this.byIOType != ParamInfo.Input;

            public virtual bool IsLongKind => GeneralColumnInfo.IsLong(this.byDataType);

            public virtual bool IsTextualKind => GeneralColumnInfo.IsTextual(this.byDataType);

            public bool IsDBNull(IByteArray mem) => mem.ReadByte(this.iBufferPosition - 1) == 0xFF;

            public int CheckDefineByte(IByteArray mem)
            {
                int defByte = mem.ReadByte(this.iBufferPosition - 1);
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
                    if ((this.byMode & ParamInfo.Mandatory) != 0)
                    {
                        return false;
                    }

                    if ((this.byMode & ParamInfo.Optional) != 0)
                    {
                        return true;
                    }

                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.DBNULLUNKNOWN));
                }
            }

            public void Put(IMaxDBDataPart dataPart, object data)
            {
                if (this.byIOType != ParamInfo.Output)
                {
                    if (data == null)
                    {
                        dataPart.WriteNull(this.iBufferPosition, this.iPhysicalLength - 1);
                    }
                    else
                    {
                        this.PutSpecific(dataPart, data);
                        dataPart.AddArg(this.iBufferPosition, this.iPhysicalLength - 1);
                    }
                }
            }

            protected void CheckFieldLimits(int byteLength)
            {
                if (byteLength > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(this.ColumnIndex + 1);
                }
            }

            protected abstract void PutSpecific(IMaxDBDataPart dataPart, object data);

            protected abstract object TransSpecificForInput(object obj);

            public virtual object TransObjectForInput(object value)
            {
                object result;
                if (value == null)
                {
                    return null;
                }

                result = this.TransSpecificForInput(value);
                if (result != null)
                {
                    return result;
                }

                if (value is string strVal)
                {
                    return this.TransStringForInput(strVal);
                }

                if (value is BigDecimal bigDecVal)
                {
                    return this.TransStringForInput(VDNNumber.BigDecimal2PlainString(bigDecVal));
                }

                if (value.GetType().IsArray)
                {
                    if (value is byte[] byteVal)
                    {
                        return this.TransBytesForInput(byteVal);
                    }

                    if (value is char[] charVal)
                    {
                        return this.TransStringForInput(new string(charVal));
                    }

                    // cannot convert other arrays
                    throw this.CreateSetException(value.GetType().FullName);
                }
                else
                {
                    // default conversion to string
                    return this.TransStringForInput(value.ToString());
                }
            }

            public virtual object TransCharacterStreamForInput(Stream stream, int length) => this.TransObjectForInput(stream);

            public virtual object TransBinaryStreamForInput(Stream stream, int length) => this.TransObjectForInput(stream);

            public virtual object TransBigDecimalForInput(BigDecimal bigDecimal) => this.TransObjectForInput(bigDecimal);

            public virtual object TransBooleanForInput(bool val) => this.TransInt32ForInput(val ? 1 : 0);

            public virtual object TransByteForInput(byte val) => this.TransObjectForInput(new BigDecimal(val));

            public virtual object TransBytesForInput(byte[] val) => throw this.CreateGetException("Bytes");

            public virtual object TransDateTimeForInput(DateTime val) => this.TransObjectForInput(val);

            public virtual object TransTimeSpanForInput(TimeSpan val) => this.TransObjectForInput(val);

            public virtual object TransDoubleForInput(double val)
            {
                if (double.IsInfinity(val) || double.IsNaN(val))
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIALNUMBERUNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
                }

                return this.TransObjectForInput(new BigDecimal(val));
            }

            public virtual object TransFloatForInput(float val)
            {
                if (float.IsInfinity(val) || float.IsNaN(val))
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIALNUMBERUNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
                }

                return this.TransObjectForInput(new BigDecimal(val));
            }

            public virtual object TransInt16ForInput(short val) => this.TransObjectForInput(new BigDecimal(val));

            public virtual object TransInt32ForInput(int val) => this.TransObjectForInput(new BigDecimal(val));

            public virtual object TransInt64ForInput(long val) => this.TransObjectForInput(new BigDecimal(val));

            public virtual object TransStringForInput(string val) => this.CreateSetException(nameof(String));

            public double BigDecimal2Double(BigDecimal bd) => bd == null ? 0.0 : (double)bd;

            public float BigDecimal2Float(BigDecimal bd) => bd == null ? 0 : (float)bd;

            public decimal BigDecimal2Decimal(BigDecimal bd) => bd == null ? 0 : (decimal)bd;

            public short BigDecimal2Int16(BigDecimal bd) => bd == null ? (short)0 : (short)bd;

            public int BigDecimal2Int32(BigDecimal bd) => bd == null ? 0 : (int)bd;

            public long BigDecimal2Int64(BigDecimal bd) => bd == null ? 0 : (long)bd;

            public virtual void SetProcParamInfo(IDBProcParameterInfo info)
            {
            }

            internal static void SetEncoding(IDBTechTranslator[] translators, Encoding encoding)
            {
                if (translators != null)
                {
                    foreach (var tt in translators)
                    {
                        if (tt is CharDataTranslator cdt)
                        {
                            cdt.AsciiEncoding = encoding;
                        }
                        else if (tt is ASCIIStreamTranslator ast)
                        {
                            ast.AsciiEncoding = encoding;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Character data translator class.
        /// </summary>
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
                    if (this.asciiEncoding == null)
                    {
                        return Encoding.GetEncoding(1252);
                    }

                    return this.asciiEncoding;
                }

                set
                {
                    this.asciiEncoding = value;
                }
            }

            public override bool IsCaseSensitive => true;

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = this.AsciiEncoding.GetBytes(data.ToString());

                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(-1);
                }

                dataPart.WriteDefineByte((byte)' ', this.iBufferPosition - 1);
                dataPart.WriteAsciiBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }

            public override DateTime GetDateTime(IByteArray mem)
            {
                string strValue = this.GetString(null, mem);
                if (strValue == null)
                {
                    return DateTime.MinValue;
                }

                try
                {
                    return DateTime.Parse(strValue, CultureInfo.InvariantCulture);
                }
                catch
                {
                    throw this.CreateParseException(strValue, nameof(DateTime));
                }
            }
        }

        /// <summary>
        /// String data translator class.
        /// </summary>
        internal class StringTranslator : CharDataTranslator
        {
            public StringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetString(controller, mem);

            public override BigDecimal GetBigDecimal(IByteArray mem)
            {
                string val = this.GetString(null, mem);

                if (val == null)
                {
                    return new BigDecimal(0);
                }

                try
                {
                    return new BigDecimal(val.Trim());
                }
                catch
                {
                    throw this.CreateParseException(val, nameof(BigDecimal));
                }
            }

            public override bool GetBoolean(IByteArray mem)
            {
                string val = this.GetString(null, mem);

                if (val == null)
                {
                    return false;
                }

                try
                {
                    return int.Parse(val.Trim(), CultureInfo.InvariantCulture) != 0;
                }
                catch
                {
                    throw this.CreateParseException(val, typeof(bool).Name);
                }
            }

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem)
            {
                string result = this.GetString(controller, mem);
                return result != null ? Encoding.Unicode.GetBytes(result) : null;
            }

            public override byte GetByte(ISqlParameterController controller, IByteArray mem)
            {
                byte[] bytes = this.GetBytes(controller, mem);
                return bytes == null || bytes.Length == 0 ? (byte)0 : bytes[0];
            }

            public override Stream GetUnicodeStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                string asString = this.GetString(controller, mem);
                return asString != null ? new MemoryStream(Encoding.Unicode.GetBytes(asString)) : null;
            }

            public override double GetDouble(IByteArray mem) => BigDecimal2Double(this.GetBigDecimal(mem));

            public override float GetFloat(IByteArray mem) => BigDecimal2Float(this.GetBigDecimal(mem));

            public override decimal GetDecimal(IByteArray mem) => BigDecimal2Decimal(this.GetBigDecimal(mem));

            public override short GetInt16(IByteArray mem) => BigDecimal2Int16(this.GetBigDecimal(mem));

            public override int GetInt32(IByteArray mem) => BigDecimal2Int32(this.GetBigDecimal(mem));

            public override long GetInt64(IByteArray mem) => BigDecimal2Int64(this.GetBigDecimal(mem));

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    string result = mem.ReadEncoding(this.AsciiEncoding, this.iBufferPosition, this.iLogicalLength);
                    switch (this.byDataType)
                    {
                        case DataType.VARCHARA:
                        case DataType.VARCHARB:
                        case DataType.VARCHARE:
                        case DataType.VARCHARUNI:
                            return result.TrimEnd(' ');
                        default:
                            return result;
                    }
                }

                return null;
            }

            public override object TransBytesForInput(byte[] val) => val == null ? null : this.TransStringForInput(this.AsciiEncoding.GetString(val));

            public override object TransCharacterStreamForInput(Stream stream, int length)
            {
                if (length <= 0)
                {
                    return null;
                }

                try
                {
                    byte[] ba = new byte[length];
                    int r = stream.Read(ba, 0, length);
                    if (r != length)
                    {
                        if (r == -1)
                        {
                            r = 0;
                        }

                        byte[] ba2 = ba;
                        ba = new byte[r];
                        Buffer.BlockCopy(ba2, 0, ba, 0, r);
                    }

                    return this.TransStringForInput(this.AsciiEncoding.GetString(ba));
                }
                catch (Exception ex)
                {
                    throw new MaxDBException(ex.Message, ex);
                }
            }

            public override object TransBinaryStreamForInput(Stream stream, int length)
            {
                if (length <= 0)
                {
                    return null;
                }

                try
                {
                    byte[] ba = new byte[length];
                    int r = stream.Read(ba, 0, length);
                    if (r != length)
                    {
                        if (r == -1)
                        {
                            r = 0;
                        }

                        byte[] ba2 = ba;
                        ba = new byte[r];
                        Buffer.BlockCopy(ba2, 0, ba, 0, r);
                    }

                    return this.TransBytesForInput(ba);
                }
                catch (Exception ex)
                {
                    throw new MaxDBException(ex.Message, ex);
                }
            }

            protected override object TransSpecificForInput(object obj) => null; // conversion to string handled by super.putObject()

            /// <summary>
            /// (The string is not inserted here, but will be used unmodified on packet creation later).
            /// </summary>
            /// <param name="arg">the String to insert</param>
            /// <returns><c>arg</c> unmodified</returns>
            public override object TransStringForInput(string arg)
            {
                if (arg == null)
                {
                    return null;
                }

                byte[] bytes = this.AsciiEncoding.GetBytes(arg);
                this.CheckFieldLimits(bytes.Length);
                return arg;
            }
        }

        /// <summary>
        /// Unicode string data translator class.
        /// </summary>
        internal class UnicodeStringTranslator : StringTranslator
        {
            private readonly Encoding mEncoding;

            public UnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
                this.mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    string result = this.mEncoding.GetString(mem.ReadBytes(this.iBufferPosition, this.iLogicalLength * Consts.UnicodeWidth));
                    switch (this.byDataType)
                    {
                        case DataType.VARCHARA:
                        case DataType.VARCHARB:
                        case DataType.VARCHARE:
                        case DataType.VARCHARUNI:
                            return result.TrimEnd(' ');
                        default:
                            return result;
                    }
                }

                return null;
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetString(controller, mem);

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem)
            {
                string result = this.GetString(controller, mem);
                return result != null ? Encoding.Unicode.GetBytes(result) : null;
            }

            public override object TransBytesForInput(byte[] val)
            {
                if (val == null)
                {
                    return val;
                }

                this.CheckFieldLimits(val.Length);
                return val;
            }

            public override object TransStringForInput(string arg)
            {
                if (arg == null)
                {
                    return null;
                }

                byte[] bytes = this.mEncoding.GetBytes(arg);
                this.CheckFieldLimits(bytes.Length);
                return bytes;
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                dataPart.WriteDefineByte(1, this.iBufferPosition - 1);
                dataPart.WriteUnicodeBytes((byte[])data, this.iBufferPosition, this.iPhysicalLength - 1);
            }
        }

        /// <summary>
        /// Space option string data translator class.
        /// </summary>
        internal class SpaceOptionStringTranslator : StringTranslator
        {
            public SpaceOptionStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                string result = null;

                if (!this.IsDBNull(mem))
                {
                    result = base.GetString(controller, mem);
                    if (result.Length == 0)
                    {
                        result = Consts.BlankChar;
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Space option unicode string data translator class.
        /// </summary>
        internal class SpaceOptionUnicodeStringTranslator : UnicodeStringTranslator
        {
            public SpaceOptionUnicodeStringTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
                : base(mode, ioType, dataType, len, ioLen, bufpos, swapMode)
            {
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                string result = null;

                if (!this.IsDBNull(mem))
                {
                    result = base.GetString(controller, mem);
                    if (result.Length == 0)
                    {
                        result = Consts.BlankChar;
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Binary data translator class.
        /// </summary>
        internal abstract class BinaryDataTranslator : DBTechTranslator
        {
            public BinaryDataTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                dataPart.WriteDefineByte(0, this.iBufferPosition - 1);
                dataPart.WriteBytes(data as byte[], this.iBufferPosition, this.iPhysicalLength - 1);
            }
        }

        /// <summary>
        /// Bytes data translator class.
        /// </summary>
        internal class BytesTranslator : BinaryDataTranslator
        {
            public BytesTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) =>
                !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, 1)[0] : (byte)0;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, 1) : null;

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetBytes(controller, mem);

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                byte[] rawResult;

                rawResult = this.GetBytes(null, mem);

                return rawResult != null ? Encoding.Unicode.GetString(rawResult) : null;
            }

            public override object TransByteForInput(byte val) => this.TransBytesForInput(new byte[] { val });

            public override object TransBytesForInput(byte[] arg)
            {
                if (arg == null)
                {
                    return null;
                }

                this.CheckFieldLimits(arg.Length);
                return arg;
            }

            protected override object TransSpecificForInput(object obj) => obj is byte[] b ? this.TransBytesForInput(b) : null;

            public override object TransStringForInput(string val) => val != null ? this.TransBytesForInput(Encoding.Unicode.GetBytes(val)) : null;

            public override object TransBinaryStreamForInput(Stream stream, int length)
            {
                if (length <= 0)
                {
                    return null;
                }

                try
                {
                    byte[] ba = new byte[length];
                    int r = stream.Read(ba, 0, length);
                    if (r != length)
                    {
                        if (r == -1)
                        {
                            r = 0;
                        }

                        byte[] ba2 = ba;
                        ba = new byte[r];
                        Buffer.BlockCopy(ba2, 0, ba, 0, r);
                    }

                    return this.TransBytesForInput(ba);
                }
                catch (Exception ex)
                {
                    throw new MaxDBException(ex.Message, ex);
                }
            }

            public override Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                byte[] asBytes = this.GetBytes(null, mem);
                return asBytes != null ? new MemoryStream(asBytes) : null;
            }
        }

        /// <summary>
        /// Boolean data translator class.
        /// </summary>
        internal class BooleanTranslator : BinaryDataTranslator
        {
            public BooleanTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override BigDecimal GetBigDecimal(IByteArray mem)
            {
                if (this.IsDBNull(mem))
                {
                    return null;
                }

                return this.GetBoolean(mem) ? new BigDecimal(1) : new BigDecimal(0);
            }

            public override bool GetBoolean(IByteArray mem) => !this.IsDBNull(mem) && (mem.ReadByte(this.iBufferPosition) != 0x00);

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) => (byte)(this.GetBoolean(mem) ? 1 : 0);

            public override float GetFloat(IByteArray mem) => this.GetByte(null, mem);

            public override double GetDouble(IByteArray mem) => this.GetByte(null, mem);

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? this.GetBoolean(mem) : (object)null;

            public override int GetInt32(IByteArray mem) => this.GetByte(null, mem);

            public override long GetInt64(IByteArray mem) => this.GetByte(null, mem);

            public override short GetInt16(IByteArray mem) => this.GetByte(null, mem);

            public override object TransBooleanForInput(bool newValue) => new byte[] { (byte)(newValue ? 1 : 0) };

            protected override object TransSpecificForInput(object obj)
            {
                if (obj is bool || obj is byte || obj is short || obj is int || obj is long || obj is ushort || obj is uint || obj is ulong || obj is float || obj is double || obj is decimal)
                {
                    return this.TransBooleanForInput((bool)obj);
                }
                else
                {
                    throw this.CreateSetException(obj.GetType().FullName);
                }

            }

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                return this.TransBooleanForInput(bool.Parse(val));
            }
        }

        /// <summary>
        /// Numeric data translator.
        /// </summary>
        internal class NumericTranslator : BinaryDataTranslator
        {
            protected int frac;
            protected bool isFloatingPoint = false;

            public NumericTranslator(int mode, int ioType, int dataType, int len, int frac, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
                switch (dataType)
                {
                    case DataType.FLOAT:
                    case DataType.VFLOAT:
                        // more digits are unreliable anyway
                        this.isFloatingPoint = true;
                        break;
                    default:
                        this.frac = frac;
                        break;
                }
            }

            public override BigDecimal GetBigDecimal(IByteArray mem)
            {
                BigDecimal result = null;
                try
                {
                    switch (this.CheckDefineByte(mem))
                    {
                        case iNullDefineByte:
                            return result;
                        case iSpecialNullValueDefineByte:
                            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSpecialNullValue), string.Empty, -10811);
                    }

                    result = VDNNumber.Number2BigDecimal(mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1));
                    if (!this.isFloatingPoint)
                    {
                        result = result.SetScale(this.frac);
                    }

                    return result;
                }
                catch
                {
                    throw this.CreateParseException(result + " scale: " + this.frac, null);
                }
            }

            public override bool GetBoolean(IByteArray mem) => this.GetInt32(mem) != 0;

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) => (byte)this.GetInt64(mem);

            public override double GetDouble(IByteArray mem)
            {
                switch (this.CheckDefineByte(mem))
                {
                    case iNullDefineByte:
                        return 0;
                    case iSpecialNullValueDefineByte:
                        return double.NaN;
                }

                return BigDecimal2Double(VDNNumber.Number2BigDecimal(mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1)));
            }

            public override float GetFloat(IByteArray mem)
            {
                switch (this.CheckDefineByte(mem))
                {
                    case iNullDefineByte:
                        return 0;
                    case iSpecialNullValueDefineByte:
                        return float.NaN;
                }

                return BigDecimal2Float(VDNNumber.Number2BigDecimal(mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1)));
            }

            public override decimal GetDecimal(IByteArray mem)
            {
                switch (this.CheckDefineByte(mem))
                {
                    case iNullDefineByte:
                    case iSpecialNullValueDefineByte:
                        return 0;
                }

                return BigDecimal2Decimal(VDNNumber.Number2BigDecimal(mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1)));
            }

            public override int GetInt32(IByteArray mem) => (int)this.GetInt64(mem);

            public override long GetInt64(IByteArray mem)
            {
                switch (this.CheckDefineByte(mem))
                {
                    case DBTechTranslator.iNullDefineByte:
                        return 0;
                    case DBTechTranslator.iSpecialNullValueDefineByte:
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSpecialNullValue), string.Empty, -10811);
                }

                return VDNNumber.Number2Long(mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1));
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem)
            {
                if (this.IsDBNull(mem))
                {
                    return null;
                }

                switch (this.byDataType)
                {
                    case DataType.FLOAT:
                    case DataType.VFLOAT:
                        return this.iLogicalLength < 15 ? this.GetFloat(mem) : (object)this.GetDouble(mem);
                    case DataType.INTEGER:
                        return this.GetInt32(mem);
                    case DataType.SMALLINT:
                        return this.GetInt16(mem);
                    default:
                        return (decimal)this.GetBigDecimal(mem);
                }
            }

            public override int Precision => this.iLogicalLength;

            public override int Scale
            {
                get
                {
                    switch (this.byDataType)
                    {
                        case DataType.FIXED:
                            return this.frac;
                        default:
                            return 0;
                    }
                }
            }

            public override short GetInt16(IByteArray mem) => (short)this.GetInt64(mem);

            public override object TransBigDecimalForInput(BigDecimal val) => val != null ? VDNNumber.BigDecimal2Number(val.SetScale(this.frac)) : null;

            public override object TransDoubleForInput(double val)
            {
                try
                {
                    var bigD = new BigDecimal(val);
                    if (this.byDataType == DataType.FIXED)
                    {
                        bigD = bigD.SetScale(this.frac);
                    }

                    return VDNNumber.BigDecimal2Number(bigD, 16);
                }
                catch
                {
                    // Detect nasty double values, and throw an SQL exception.
                    if (double.IsInfinity(val) || double.IsNaN(val))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIALNUMBERUNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            public override object TransFloatForInput(float val)
            {
                try
                {
                    var bigD = new BigDecimal(val);
                    if (this.byDataType == DataType.FIXED)
                    {
                        bigD = bigD.SetScale(this.frac);
                    }

                    return VDNNumber.BigDecimal2Number(bigD, 14);
                }
                catch
                {
                    // Detect nasty double values, and throw an SQL exception.
                    if (float.IsInfinity(val) || float.IsNaN(val))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SPECIALNUMBERUNSUPPORTED, val.ToString(CultureInfo.InvariantCulture)));
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            public override object TransInt16ForInput(short val) => VDNNumber.Long2Number(val);

            public override object TransInt32ForInput(int val) => VDNNumber.Long2Number(val);

            public override object TransInt64ForInput(long val) => VDNNumber.Long2Number(val);

            protected override object TransSpecificForInput(object obj)
            {
                if (obj == null)
                {
                    return null;
                }

                if (obj is BigDecimal bd)
                {
                    return this.TransBigDecimalForInput(bd);
                }

                if (obj is bool b)
                {
                    return this.TransBooleanForInput(b);
                }

                if (obj is byte by)
                {
                    return this.TransByteForInput(by);
                }

                if (obj is double dbl)
                {
                    return this.TransDoubleForInput(dbl);
                }

                if (obj is float f)
                {
                    return this.TransFloatForInput(f);
                }

                if (obj is int i)
                {
                    return this.TransInt32ForInput(i);
                }

                if (obj is long l)
                {
                    return this.TransInt64ForInput(l);
                }

                if (obj is short s)
                {
                    return this.TransInt16ForInput(s);
                }

                return null;
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                switch (this.CheckDefineByte(mem))
                {
                    case iNullDefineByte:
                        return null;
                    case iSpecialNullValueDefineByte:
                        return double.NaN.ToString(CultureInfo.InvariantCulture);
                }

                return VDNNumber.Number2String(
                    mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1),
                    this.byDataType != DataType.FLOAT && this.byDataType != DataType.VFLOAT,
                    this.iLogicalLength,
                    this.frac);
            }

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                try
                {
                    return this.TransBigDecimalForInput(new BigDecimal(val.Trim()));
                }
                catch
                {
                    throw this.CreateParseException(val, null);
                }
            }
        }

        /// <summary>
        /// Time data translator.
        /// </summary>
        internal class TimeTranslator : CharDataTranslator
        {
            private const int TimeSize = 8;

            public TimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetDateTime(mem);

            public override string GetString(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadAscii(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int hour = ((raw[0] - '0') * 10) + (raw[1] - '0');
                    int min = ((raw[3] - '0') * 10) + (raw[4] - '0');
                    int sec = ((raw[6] - '0') * 10) + (raw[7] - '0');

                    return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            public override bool IsCaseSensitive => false;

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

            public virtual object TransTimeForInput(TimeSpan ts) => this.TransDateTimeForInput(DateTime.MinValue.AddTicks(ts.Ticks));

            protected override object TransSpecificForInput(object obj) => obj is DateTime dt ? this.TransTimeForInput(dt) : this.TransTimeForInput((TimeSpan)obj);

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                try
                {
                    return this.TransTimeForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
                }
                catch
                {
                    try
                    {
                        return this.TransTimeForInput(TimeSpan.Parse(val, CultureInfo.InvariantCulture));
                    }
                    catch
                    {
                        throw this.CreateParseException(val, "Time");
                    }
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(-1);
                }

                dataPart.WriteDefineByte((byte)' ', this.iBufferPosition - 1);
                dataPart.WriteAsciiBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }
        }

        /// <summary>
        /// Unicode time data translator.
        /// </summary>
        internal class UnicodeTimeTranslator : TimeTranslator
        {
            private readonly Encoding mEncoding;

            public UnicodeTimeTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
                : base(mode, ioType, dataType, len, ioLen, bufpos) => this.mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;

            public override string GetString(ISqlParameterController controller, IByteArray mem) =>
                !this.IsDBNull(mem) ? mem.ReadUnicode(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int offset = this.mEncoding == Encoding.Unicode ? 1 : 0;

                    int hour = ((raw[1 - offset] - '0') * 10) + (raw[3 - offset] - '0');
                    int min = ((raw[7 - offset] - '0') * 10) + (raw[9 - offset] - '0');
                    int sec = ((raw[13 - offset] - '0') * 10) + (raw[15 - offset] - '0');

                    return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, hour, min, sec);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(DataType.StrValues[this.byDataType], -1);
                }

                dataPart.WriteDefineByte(Consts.DefinedUnicode, this.iBufferPosition - 1);
                dataPart.WriteUnicodeBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }

            public override object TransTimeForInput(DateTime dt)
            {
                byte[] bytes = this.mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransTimeForInput(dt)));
                this.CheckFieldLimits(bytes.Length);
                return bytes;
            }
        }

        /// <summary>
        /// Timestamp data translator.
        /// </summary>
        internal class TimestampTranslator : CharDataTranslator
        {
            private const int TimestampSize = 26;

            public TimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetDateTime(mem);

            public override string GetString(ISqlParameterController controller, IByteArray mem) =>
                !this.IsDBNull(mem) ? mem.ReadAscii(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem) =>
                !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int year = ((raw[0] - '0') * 1000) + ((raw[1] - '0') * 100) + ((raw[2] - '0') * 10) + (raw[3] - '0');
                    int month = ((raw[5] - '0') * 10) + (raw[6] - '0');
                    int day = ((raw[8] - '0') * 10) + (raw[9] - '0');
                    int hour = ((raw[11] - '0') * 10) + (raw[12] - '0');
                    int min = ((raw[14] - '0') * 10) + (raw[15] - '0');
                    int sec = ((raw[17] - '0') * 10) + (raw[18] - '0');
                    int milli = ((raw[20] - '0') * 100) + ((raw[21] - '0') * 10) + (raw[22] - '0');
                    int nano = ((raw[23] - '0') * 100) + ((raw[24] - '0') * 10) + (raw[25] - '0');

                    return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(nano * TimeSpan.TicksPerMillisecond / 1000);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            public override bool IsCaseSensitive => false;

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
                formattedTimestamp[3] = (byte)('0' + year);
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

            protected override object TransSpecificForInput(object obj) => obj is DateTime dt ? this.TransTimestampForInput(dt) : null;

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                try
                {
                    return this.TransSpecificForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
                }
                catch
                {
                    throw this.CreateParseException(val, "Timestamp");
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(-1);
                }

                dataPart.WriteDefineByte((byte)' ', this.iBufferPosition - 1);
                dataPart.WriteAsciiBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }
        }

        /// <summary>
        /// Unicode timestamp data translator.
        /// </summary>
        internal class UnicodeTimestampTranslator : TimestampTranslator
        {
            private readonly Encoding mEncoding;

            public UnicodeTimestampTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
                : base(mode, ioType, dataType, len, ioLen, bufpos) => this.mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;

            public override string GetString(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadUnicode(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int offset = this.mEncoding == Encoding.Unicode ? 1 : 0;

                    int year = ((raw[1 - offset] - '0') * 1000) + ((raw[3 - offset] - '0') * 100) +
                        ((raw[5 - offset] - '0') * 10) + (raw[7 - offset] - '0');
                    int month = ((raw[11 - offset] - '0') * 10) + (raw[13 - offset] - '0');
                    int day = ((raw[17 - offset] - '0') * 10) + (raw[19 - offset] - '0');
                    int hour = ((raw[23 - offset] - '0') * 10) + (raw[25 - offset] - '0');
                    int min = ((raw[29 - offset] - '0') * 10) + (raw[31 - offset] - '0');
                    int sec = ((raw[35 - offset] - '0') * 10) + (raw[37 - offset] - '0');
                    int milli = ((raw[41 - offset] - '0') * 100) + ((raw[43 - offset] - '0') * 10) + (raw[45 - offset] - '0');
                    int nano = ((raw[47 - offset] - '0') * 100) + ((raw[49 - offset] - '0') * 10) + (raw[51 - offset] - '0');

                    return new DateTime(year, month, day, hour, min, sec, milli).AddTicks(nano * TimeSpan.TicksPerMillisecond / 1000);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(DataType.StrValues[this.byDataType], -1);
                }

                dataPart.WriteDefineByte(Consts.DefinedUnicode, this.iBufferPosition - 1);
                dataPart.WriteUnicodeBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }

            public override object TransTimestampForInput(DateTime dt)
            {
                byte[] bytes = this.mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransTimestampForInput(dt)));
                this.CheckFieldLimits(bytes.Length);
                return bytes;
            }
        }

        /// <summary>
        /// Date data translator.
        /// </summary>
        internal class DateTranslator : CharDataTranslator
        {
            private const int DateSize = 10;

            public DateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetDateTime(mem);

            public override string GetString(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadAscii(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int year = ((raw[0] - '0') * 1000) + ((raw[1] - '0') * 100) + ((raw[2] - '0') * 10) + (raw[3] - '0');
                    int month = ((raw[5] - '0') * 10) + (raw[6] - '0');
                    int day = ((raw[8] - '0') * 10) + (raw[9] - '0');

                    return new DateTime(year, month, day);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            public override bool IsCaseSensitive => false;

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

            protected override object TransSpecificForInput(object obj) => obj is DateTime dt ? this.TransDateForInput(dt) : null;

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                try
                {
                    return this.TransSpecificForInput(DateTime.Parse(val, CultureInfo.InvariantCulture));
                }
                catch
                {
                    throw this.CreateParseException(val, "Date");
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(-1);
                }

                dataPart.WriteDefineByte((byte)' ', this.iBufferPosition - 1);
                dataPart.WriteAsciiBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }
        }

        /// <summary>
        /// Unicode date data translator.
        /// </summary>
        internal class UnicodeDateTranslator : DateTranslator
        {
            private readonly Encoding mEncoding;

            public UnicodeDateTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool swapMode)
                : base(mode, ioType, dataType, len, ioLen, bufpos) => this.mEncoding = swapMode ? Encoding.Unicode : Encoding.BigEndianUnicode;

            public override string GetString(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadUnicode(this.iBufferPosition, this.iPhysicalLength - 1) : null;

            public override DateTime GetDateTime(IByteArray mem)
            {
                if (!this.IsDBNull(mem))
                {
                    byte[] raw = mem.ReadBytes(this.iBufferPosition, this.iPhysicalLength - 1);

                    int offset = this.mEncoding == Encoding.Unicode ? 1 : 0;

                    int year = ((raw[1 - offset] - '0') * 1000) + ((raw[3 - offset] - '0') * 100) +
                        ((raw[5 - offset] - '0') * 10) + (raw[7 - offset] - '0');
                    int month = ((raw[11 - offset] - '0') * 10) + (raw[13 - offset] - '0');
                    int day = ((raw[17 - offset] - '0') * 10) + (raw[19 - offset] - '0');

                    return new DateTime(year, month, day);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                if (bytes.Length > this.iPhysicalLength - 1)
                {
                    throw new MaxDBValueOverflowException(DataType.StrValues[this.byDataType], -1);
                }

                dataPart.WriteDefineByte(Consts.DefinedUnicode, this.iBufferPosition - 1);
                dataPart.WriteUnicodeBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }

            public override object TransDateForInput(DateTime dt)
            {
                byte[] bytes = this.mEncoding.GetBytes(Encoding.ASCII.GetString((byte[])base.TransDateForInput(dt)));
                this.CheckFieldLimits(bytes.Length);
                return bytes;
            }
        }

        /// <summary>
        /// Structured data translator.
        /// </summary>
        internal class StructureTranslator : DBTechTranslator
        {
            private StructMemberTranslator[] mStructureConverter;
            private readonly bool bUnicode;

            public StructureTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos, bool unicode)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
                this.bUnicode = unicode;
                this.mStructureConverter = Array.Empty<StructMemberTranslator>();
            }

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                byte[] bytes = (byte[])data;
                dataPart.WriteDefineByte(0, this.iBufferPosition - 1);
                dataPart.WriteBytes(bytes, this.iBufferPosition, this.iPhysicalLength - 1);
            }

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, 1)[0] : (byte)0;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? mem.ReadBytes(this.iBufferPosition, this.iLogicalLength) : null;

            public override object GetValue(ISqlParameterController controller, IByteArray mem)
            {
                byte[] ba = this.GetBytes(null, mem);
                if (ba != null)
                {
                    object[] objArr = new object[this.mStructureConverter.Length];
                    var sb = new ByteArray(ba, 0, mem.Swapped);
                    for (int i = 0; i < objArr.Length; i++)
                    {
                        objArr[i] = this.mStructureConverter[i].GetValue(sb, 0);
                    }

                    return new DBProcStructure(objArr);
                }
                else
                {
                    return null;
                }
            }

            public override object TransByteForInput(byte val) => this.TransBytesForInput(new byte[1] { val });

            public override object TransBytesForInput(byte[] arg)
            {
                if (arg == null)
                {
                    return arg;
                }

                this.CheckFieldLimits(arg.Length);
                return arg;
            }

            protected override object TransSpecificForInput(object obj)
            {
                object result = null;

                if (obj is byte[] b)
                {
                    result = this.TransBytesForInput(b);
                }
                else if (obj is object[] o)
                {
                    result = this.TransObjectArrayForInput(o);
                }
                else if (obj is DBProcStructure s)
                {
                    result = this.TransObjectArrayForInput(s.Attributes);
                }

                return result;
            }

            public object TransObjectArrayForInput(object[] objectArray)
            {
                if (objectArray.Length != this.mStructureConverter.Length)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTUREARRAYWRONGLENTGH,
                        this.mStructureConverter.Length, objectArray.Length));
                }

                var sb = new ByteArray(this.iPhysicalLength - 1);
                for (int i = 0; i < objectArray.Length; i++)
                {
                    if (objectArray[i] == null)
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTNULL, i + 1));
                    }

                    this.mStructureConverter[i].PutValue(sb, objectArray[i]);
                }

                return sb.GetArrayData();
            }

            public override object TransCharacterStreamForInput(Stream stream, int length)
            {
                if (length <= 0)
                {
                    return null;
                }

                try
                {
                    byte[] ba = new byte[length];
                    int r = stream.Read(ba, 0, length);
                    if (r != length)
                    {
                        if (r == -1)
                        {
                            r = 0;
                        }

                        byte[] ba2 = ba;
                        ba = new byte[r];
                        Buffer.BlockCopy(ba2, 0, ba, 0, r);
                    }

                    return this.TransStringForInput(this.bUnicode ? Encoding.Unicode.GetString(ba) : Encoding.ASCII.GetString(ba));
                }
                catch (Exception ex)
                {
                    throw new MaxDBException(ex.Message, ex);
                }
            }

            public override object TransBinaryStreamForInput(Stream reader, int length)
            {
                if (length <= 0)
                {
                    return null;
                }

                try
                {
                    byte[] ba = new byte[length];
                    int r = reader.Read(ba, 0, length);
                    if (r != length)
                    {
                        if (r == -1)
                        {
                            r = 0;
                        }

                        byte[] ba2 = ba;
                        ba = new byte[r];
                        Array.Copy(ba2, 0, ba, 0, r);
                    }

                    return this.TransBytesForInput(ba);
                }
                catch (Exception ex)
                {
                    throw new MaxDBException(ex.Message, ex);
                }
            }

            public override Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                byte[] asBytes = this.GetBytes(null, mem);

                if (asBytes == null)
                {
                    return null;
                }

                return new MemoryStream(asBytes);
            }

            public override void SetProcParamInfo(IDBProcParameterInfo info) => this.mStructureConverter = StructMemberTranslator.CreateStructMemberTranslators(info, this.bUnicode);
        }

        /// <summary>
        /// Structured member data translator.
        /// </summary>
        internal abstract class StructMemberTranslator
        {
            protected IStructureElement mStructElement;
            protected int iOffset;

            public StructMemberTranslator(IStructureElement structElement, bool unicode)
            {
                this.mStructElement = structElement;
                this.iOffset = unicode ? structElement.UnicodeOffset : structElement.AsciiOffset;
            }

            public abstract object GetValue(IByteArray memory, int recordOffset);

            public abstract void PutValue(IByteArray memory, object obj);

            protected void ThrowConversionError(string srcObj) => throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTCONVERSION, this.mStructElement.SqlTypeName, srcObj));

            /// <summary>
            /// Creates the translator for a structure.
            /// </summary>
            /// <param name="paramInfo">The extended parameter info</param>
            /// <param name="index">Parameter index.</param>
            /// <param name="unicode">Whether this is an unicode connection.</param>
            /// <returns>Element translator.</returns>
            public static StructMemberTranslator CreateStructMemberTranslator(IDBProcParameterInfo paramInfo, int index, bool unicode)
            {
                var s = paramInfo[index];
                if (string.Compare(s.TypeName.Trim(), "CHAR", true, CultureInfo.InvariantCulture) == 0)
                {
                    if (string.Compare(s.CodeType.Trim(), "BYTE", true, CultureInfo.InvariantCulture) == 0)
                    {
                        return new ByteStructureElementTranslator(s, unicode);
                    }
                    else if (string.Compare(s.CodeType.Trim(), "ASCII", true, CultureInfo.InvariantCulture) == 0)
                    {
                        return new CharASCIIStructureElementTranslator(s, unicode);
                    }
                }
                else if (string.Compare(s.TypeName.Trim(), "WYDE", true, CultureInfo.InvariantCulture) == 0)
                {
                    return unicode ? new WydeStructureElementTranslator(s, unicode) : (StructMemberTranslator)new CharASCIIStructureElementTranslator(s, unicode);
                }
                else if (string.Compare(s.TypeName.Trim(), "SMALLINT", true, CultureInfo.InvariantCulture) == 0)
                {
                    if (s.Length == 5)
                    {
                        return new ShortStructureElementTranslator(s, unicode);
                    }
                }
                else if (string.Compare(s.TypeName.Trim(), "INTEGER", true, CultureInfo.InvariantCulture) == 0)
                {
                    if (s.Length == 10)
                    {
                        return new IntStructureElementTranslator(s, unicode);
                    }
                    else if (s.Length == 19)
                    {
                        return new LongStructureElementTranslator(s, unicode);
                    }
                }
                else if (string.Compare(s.TypeName.Trim(), "FIXED", true, CultureInfo.InvariantCulture) == 0)
                {
                    if (s.Precision == 0)
                    {
                        if (s.Length == 5)
                        {
                            return new ShortStructureElementTranslator(s, unicode);
                        }
                        else if (s.Length == 10)
                        {
                            return new IntStructureElementTranslator(s, unicode);
                        }
                        else if (s.Length == 19)
                        {
                            return new LongStructureElementTranslator(s, unicode);
                        }
                    }
                }
                else if (string.Compare(s.TypeName.Trim(), "FLOAT", true, CultureInfo.InvariantCulture) == 0)
                {
                    if (s.Length == 15)
                    {
                        return new DoubleStructureElementTranslator(s, unicode);
                    }
                    else if (s.Length == 6)
                    {
                        return new FloatStructureElementTranslator(s, unicode);
                    }
                }
                else if (string.Compare(s.TypeName.Trim(), "BOOLEAN", true, CultureInfo.InvariantCulture) == 0)
                {
                    return new BooleanStructureElementTranslator(s, unicode);
                }

                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSTRUCTURETYPE, index, s.SqlTypeName));
            }

            /// <summary>
            /// Creates the translators for a structure.
            /// </summary>
            /// <param name="info">The extended parameter info.</param>
            /// <param name="unicode">Whether this is an unicode connection.</param>
            /// <returns>The converter array.</returns>
            public static StructMemberTranslator[] CreateStructMemberTranslators(IDBProcParameterInfo info, bool unicode)
            {
                var result = new StructMemberTranslator[info.MemberCount];

                for (int i = 0; i < result.Length; ++i)
                {
                    result[i] = CreateStructMemberTranslator(info, i, unicode);
                }

                return result;
            }
        }

        /// <summary>
        /// BOOLEAN structure element data translator.
        /// </summary>
        internal class BooleanStructureElementTranslator : StructMemberTranslator
        {
            public BooleanStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => memory.ReadByte(this.iOffset + recordOffset) != 0;

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is bool b)
                {
                    memory.WriteByte((byte)(b ? 1 : 0), this.iOffset);
                }
                else
                {
                    this.ThrowConversionError(obj.GetType().FullName);
                }
            }
        }

        /// <summary>
        /// CHAR BYTE structure element data translator.
        /// </summary>
        internal class ByteStructureElementTranslator : StructMemberTranslator
        {
            public ByteStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset)
            {
                byte[] bytes = memory.ReadBytes(this.iOffset + recordOffset, this.mStructElement.Length);
                return this.mStructElement.Length == 1 ? bytes[0] : (object)bytes;
            }

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte[] bArray)
                {
                    memory.WriteBytes(bArray, this.iOffset);
                }
                else if (obj is byte b)
                {
                    memory.WriteBytes(new byte[1] { b }, this.iOffset);
                }
                else
                {
                    this.ThrowConversionError(obj.GetType().FullName);
                }
            }
        }

        /// <summary>
        /// CHAR ASCII structure element data translator.
        /// </summary>
        internal class CharASCIIStructureElementTranslator : StructMemberTranslator
        {
            public CharASCIIStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset)
            {
                byte[] bytes = memory.ReadBytes(this.iOffset + recordOffset, this.mStructElement.Length);
                if (this.mStructElement.Length == 1)
                {
                    return (char)bytes[0];
                }
                else
                {
                    return Encoding.ASCII.GetString(bytes);
                }
            }

            public override void PutValue(IByteArray memory, object obj)
            {
                string convStr = null;
                if (obj is char[] cArray)
                {
                    convStr = new string(cArray);
                }
                else if (obj is string s)
                {
                    convStr = s;
                }
                else if (obj is char c)
                {
                    convStr = new string(new char[] { c });
                }
                else
                {
                    this.ThrowConversionError(obj.GetType().FullName);
                }

                memory.WriteAscii(convStr, this.iOffset);
            }
        }

        /// <summary>
        /// WYDE structure element data translator.
        /// </summary>
        internal class WydeStructureElementTranslator : StructMemberTranslator
        {
            public WydeStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset)
            {
                string ca = memory.ReadUnicode(this.iOffset + recordOffset, this.mStructElement.Length * 2);
                if (this.mStructElement.Length == 1)
                {
                    return ca[0];
                }
                else
                {
                    return ca;
                }
            }

            public override void PutValue(IByteArray memory, object obj)
            {
                string convStr = null;
                if (obj is char[] cArray)
                {
                    convStr = new string(cArray);
                }
                else if (obj is string s)
                {
                    convStr = s;
                }
                else if (obj is char c)
                {
                    convStr = new string(new char[] { c });
                }
                else
                {
                    this.ThrowConversionError(obj.GetType().FullName);
                }

                memory.WriteUnicode(convStr, this.iOffset);
            }
        }

        /// <summary>
        /// SMALLINT structure element data translator.
        /// </summary>
        internal class ShortStructureElementTranslator : StructMemberTranslator
        {
            public ShortStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => memory.ReadInt16(this.iOffset + recordOffset);

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte || obj is short || obj is int || obj is long
                    || obj is ushort || obj is uint || obj is ulong
                    || (obj is float f && f <= short.MaxValue && f >= short.MinValue)
                    || (obj is double d && d <= short.MaxValue && d >= short.MinValue)
                    || (obj is decimal dc && dc <= short.MaxValue && dc >= short.MinValue))
                {
                    memory.WriteInt16((short)obj, this.iOffset);
                }
                else
                {
                    if ((obj is float fl && (fl > short.MaxValue || fl < short.MinValue))
                        || (obj is double dbl && (dbl > short.MaxValue || dbl < short.MinValue))
                        || (obj is decimal dec && (dec > short.MaxValue || dec < short.MinValue)))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTOVERFLOW, this.mStructElement.SqlTypeName, obj.ToString()));
                    }
                    else
                    {
                        this.ThrowConversionError(obj.GetType().FullName);
                    }
                }
            }
        }

        /// <summary>
        /// INTEGER structure element data translator.
        /// </summary>
        internal class IntStructureElementTranslator : StructMemberTranslator
        {
            public IntStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => memory.ReadInt32(this.iOffset + recordOffset);

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte || obj is short || obj is int || obj is long
                    || obj is ushort || obj is uint || obj is ulong
                    || (obj is float f && f <= int.MaxValue && f >= int.MinValue)
                    || (obj is double d && d <= int.MaxValue && d >= int.MinValue)
                    || (obj is decimal dc && dc <= int.MaxValue && dc >= int.MinValue))
                {
                    memory.WriteInt32((int)obj, this.iOffset);
                }
                else
                {
                    if ((obj is float fl && (fl > int.MaxValue || fl < int.MinValue))
                        || (obj is double dbl && (dbl > int.MaxValue || dbl < int.MinValue))
                        || (obj is decimal dec && (dec > int.MaxValue || dec < int.MinValue)))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTOVERFLOW, this.mStructElement.SqlTypeName, obj.ToString()));
                    }
                    else
                    {
                        this.ThrowConversionError(obj.GetType().FullName);
                    }
                }
            }
        }

        /// <summary>
        /// LONG structure element data translator.
        /// </summary>
        internal class LongStructureElementTranslator : StructMemberTranslator
        {
            public LongStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => memory.ReadInt64(this.iOffset + recordOffset);

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte || obj is short || obj is int || obj is long
                    || obj is ushort || obj is uint || obj is ulong
                    || (obj is float f && f <= long.MaxValue && f >= long.MinValue)
                    || (obj is double d && d <= long.MaxValue && d >= long.MinValue)
                    || (obj is decimal dc && dc <= long.MaxValue && dc >= long.MinValue))
                {
                    memory.WriteInt64((long)obj, this.iOffset);
                }
                else
                {
                    if ((obj is float fl && (fl > long.MaxValue || fl < long.MinValue))
                        || (obj is double dbl && (dbl > long.MaxValue || dbl < long.MinValue))
                        || (obj is decimal dec && (dec > long.MaxValue || dec < long.MinValue)))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTOVERFLOW, this.mStructElement.SqlTypeName, obj.ToString()));
                    }
                    else
                    {
                        this.ThrowConversionError(obj.GetType().FullName);
                    }
                }
            }
        }

        /// <summary>
        /// FLOAT structure element data translator.
        /// </summary>
        internal class FloatStructureElementTranslator : StructMemberTranslator
        {
            public FloatStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => BitConverter.ToSingle(memory.ReadBytes(this.iOffset, 4), 0);

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte || obj is short || obj is int || obj is long
                    || obj is ushort || obj is uint || obj is ulong || obj is float
                    || (obj is double dbl && dbl <= long.MaxValue && dbl >= float.MinValue)
                    || (obj is decimal dec && dec <= long.MaxValue))
                {
                    memory.WriteBytes(BitConverter.GetBytes((float)obj), this.iOffset);
                }
                else
                {
                    if ((obj is double d && (d > long.MaxValue || d < float.MinValue))
                        || (obj is decimal de && de > long.MaxValue))
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STRUCTELEMENTOVERFLOW, this.mStructElement.SqlTypeName, obj.ToString()));
                    }
                    else
                    {
                        this.ThrowConversionError(obj.GetType().FullName);
                    }
                }
            }
        }

        /// <summary>
        /// DOUBLE structure element data translator.
        /// </summary>
        internal class DoubleStructureElementTranslator : StructMemberTranslator
        {
            public DoubleStructureElementTranslator(IStructureElement structElement, bool unicode)
                : base(structElement, unicode)
            {
            }

            public override object GetValue(IByteArray memory, int recordOffset) => BitConverter.ToDouble(memory.ReadBytes(this.iOffset, 8), 0);

            public override void PutValue(IByteArray memory, object obj)
            {
                if (obj is byte || obj is short || obj is int || obj is long
                    || obj is ushort || obj is uint || obj is ulong || obj is float || obj is double
                    || (obj is decimal dec && dec <= long.MaxValue && dec >= long.MinValue))
                {
                    memory.WriteBytes(BitConverter.GetBytes((double)obj), this.iOffset);
                }
                else
                {
                    this.ThrowConversionError(obj.GetType().FullName);
                }
            }
        }

        /// <summary>
        /// Translator for LONG arguments of DB Procedures.
        /// </summary>
        internal class ProcedureStreamTranslator : DBTechTranslator
        {
            public ProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override int ColumnDisplaySize => int.MaxValue;

            public override object TransBinaryStreamForInput(Stream stream, int length)
            {
                if (this.IsBinary)
                {
                    if (stream == null)
                    {
                        return null;
                    }

                    return new BinaryProcedurePutValue(this, stream, length);
                }
                else
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONBYTESTREAM));
                }
            }

            public override object TransStringForInput(string val)
            {
                if (val == null)
                {
                    return null;
                }

                if (this.IsASCII)
                {
                    return new ASCIIProcedurePutValue(this, Encoding.ASCII.GetBytes(val));
                }
                else
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSTRINGSTREAM));
                }
            }

            public override object TransCharacterStreamForInput(Stream stream, int length)
            {
                if (this.IsASCII)
                {
                    return stream == null ? null : new ASCIIProcedurePutValue(this, stream, length);
                }
                else
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSTRINGSTREAM));
                }
            }

            public override object TransBytesForInput(byte[] val) => val != null ? this.TransBinaryStreamForInput(new MemoryStream(val), -1) : this.TransBinaryStreamForInput(null, -1);

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                var putval = (AbstractProcedurePutValue)data;
                putval.PutDescriptor(dataPart);
            }

            protected override object TransSpecificForInput(object obj) =>
                obj is Stream stream ? this.TransASCIIStreamForInput(stream, -1) : null;

            public virtual object TransASCIIStreamForInput(Stream stream, int length)
            {
                if (this.IsASCII)
                {
                    if (stream == null)
                    {
                        return null;
                    }

                    return new ASCIIProcedurePutValue(this, stream, length);
                }
                else
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONSTRINGSTREAM));
                }
            }

            private bool IsASCII => this.byDataType == DataType.STRA || this.byDataType == DataType.LONGA;

            private bool IsBinary => this.byDataType == DataType.STRB || this.byDataType == DataType.LONGB;

            private Stream GetStream(ISqlParameterController controller, IByteArray mem, IByteArray longdata)
            {
                Stream result = null;
                if (!this.IsDBNull(mem))
                {
                    var descriptor = mem.ReadBytes(this.iBufferPosition, this.iLogicalLength);

                    // return also NULL if the LONG hasn't been touched.
                    if (IsDescriptorNull(descriptor))
                    {
                        return null;
                    }

                    var getval = new GetLOBValue(controller.Connection, descriptor, longdata);
                    result = getval.ASCIIStream;
                }

                return result;
            }

            public override Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => this.GetStream(controller, mem, longData);

            public override Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                if (this.IsBinary)
                {
                    return this.GetStream(controller, mem, longData);
                }
                else
                {
                    throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYREADFROMLONG));
                }
            }

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? this.GetBytes(controller, mem)[0] : (byte)0;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem)
            {
                Stream blobStream;

                blobStream = this.GetBinaryStream(controller, mem, controller.ReplyData);
                if (blobStream == null)
                {
                    return null;
                }

                try
                {
                    const int bufSize = 4096;
                    byte[] buf = new byte[bufSize];
                    int readLen;

                    using (var tmpStream = new MemoryStream())
                    {
                        readLen = blobStream.Read(buf, 0, buf.Length);
                        while (readLen > 0)
                        {
                            tmpStream.Write(buf, 0, readLen);
                            if (readLen < bufSize)
                            {
                                break;
                            }

                            readLen = blobStream.Read(buf, 0, buf.Length);
                        }

                        return tmpStream.ToArray();
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
            }

            public virtual TextReader GetCharacterStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                var byteStream = this.GetASCIIStream(controller, mem, longData);
                if (byteStream == null)
                {
                    return null;
                }

                return new RawByteReader(byteStream);
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                const int bufSize = 4096;
                var result = new StringBuilder();

                var reader = this.GetCharacterStream(controller, mem, controller.ReplyData);
                if (reader == null)
                {
                    return null;
                }

                try
                {
                    char[] buf = new char[bufSize];
                    int charsRead;

                    while ((charsRead = reader.Read(buf, 0, bufSize)) > 0)
                    {
                        result.Append(new string(buf, 0, charsRead));
                        if (charsRead < bufSize)
                        {
                            break;
                        }
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

            public override long GetChars(ISqlParameterController controller, IByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
            {
                const int bufSize = 4096;
                int alreadyRead = 0;

                var reader = this.GetCharacterStream(controller, mem, controller.ReplyData);
                if (reader == null)
                {
                    return 0;
                }

                try
                {
                    char[] buf = new char[bufSize];
                    int charsRead;

                    while ((charsRead = reader.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0)
                    {
                        alreadyRead += charsRead;
                        if (charsRead < bufSize)
                        {
                            break;
                        }
                    }

                    alreadyRead = 0;
                    while ((charsRead = reader.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0)
                    {
                        Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, charsRead);
                        alreadyRead += charsRead;
                        if (charsRead < bufSize)
                        {
                            break;
                        }
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

            public override long GetBytes(ISqlParameterController controller, IByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
            {
                const int bufSize = 4096;
                int alreadyRead = 0;

                var stream = this.GetBinaryStream(controller, mem, controller.ReplyData);
                if (stream == null)
                {
                    return 0;
                }

                try
                {
                    byte[] buf = new byte[bufSize];
                    int bytesRead;

                    while ((bytesRead = stream.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0)
                    {
                        alreadyRead += bytesRead;
                        if (bytesRead < bufSize)
                        {
                            break;
                        }
                    }

                    alreadyRead = 0;
                    while ((bytesRead = stream.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0)
                    {
                        Buffer.BlockCopy(buf, 0, buffer, bufferoffset + alreadyRead, bytesRead);
                        alreadyRead += bytesRead;
                        if (bytesRead < bufSize)
                        {
                            break;
                        }
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

            protected static bool IsDescriptorNull(byte[] descriptor) => descriptor[LongDesc.State] == LongDesc.StateStream;
        }

        /// <summary>
        /// Translator for LONG UNICODE arguments of DB Procedures.
        /// </summary>
        internal class UnicodeProcedureStreamTranslator : ProcedureStreamTranslator
        {
            public UnicodeProcedureStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override int ColumnDisplaySize => 1073741824;

            public override object TransStringForInput(string val) => val != null ? new UnicodeProcedurePutValue(this, val.ToCharArray()) : null;

            public object TransCharacterStreamForInput(TextReader reader, int length) => reader != null ? new UnicodeProcedurePutValue(this, reader, length) : null;

            public override Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                var reader = this.GetCharacterStream(controller, mem, longData);
                return reader != null ? new ReaderStream(reader, false) : null;
            }

            public override TextReader GetCharacterStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                TextReader result = null;
                AbstractGetValue getval;

                if (!this.IsDBNull(mem))
                {
                    var descriptor = mem.ReadBytes(this.iBufferPosition, this.iLogicalLength);
                    if (IsDescriptorNull(descriptor))
                    {
                        return null;
                    }

                    getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData);
                    result = getval.CharacterStream;
                }

                return result;
            }

            public override object TransASCIIStreamForInput(Stream stream, int length)
            {
                if (stream == null)
                {
                    return null;
                }

                return this.TransCharacterStreamForInput(new StreamReader(stream), length);
            }
        }

        /// <summary>
        /// Abstract stream translator.
        /// </summary>
        internal abstract class StreamTranslator : BinaryDataTranslator
        {
            protected StreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override int ColumnDisplaySize
            {
                get
                {
                    switch (this.byDataType)
                    {
                        case DataType.STRUNI:
                        case DataType.LONGUNI:
                            return 1073741824 - 4096;
                        default:
                            return 2147483647 - 8192;
                    }
                }
            }

            public override int Precision => int.MaxValue;

            public override Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) =>
                throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIREADFROMLONG));

            public override Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) =>
                this.GetStream(controller, mem, longData);

            public virtual TextReader GetCharacterStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                var byteStream = this.GetASCIIStream(controller, mem, longData);

                return byteStream != null ? new RawByteReader(byteStream) : null;
            }

            public Stream GetStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                Stream result = null;

                if (!this.IsDBNull(mem))
                {
                    var descriptor = mem.ReadBytes(this.iBufferPosition, this.iLogicalLength);
                    var getval = new GetLOBValue(controller.Connection, descriptor, longData);

                    result = getval.ASCIIStream;
                }

                return result;
            }

            public override string GetString(ISqlParameterController controller, IByteArray mem)
            {
                const int bufSize = 4096;
                var result = new StringBuilder();

                var reader = this.GetCharacterStream(controller, mem, controller.ReplyData);
                if (reader == null)
                {
                    return null;
                }

                try
                {
                    char[] buf = new char[bufSize];
                    int charsRead;

                    while ((charsRead = reader.Read(buf, 0, bufSize)) > 0)
                    {
                        result.Append(new string(buf, 0, charsRead));
                        if (charsRead < bufSize)
                        {
                            break;
                        }
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

            public override long GetChars(ISqlParameterController controller, IByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length)
            {
                const int bufSize = 4096;
                int alreadyRead = 0;

                var reader = this.GetCharacterStream(controller, mem, controller.ReplyData);
                if (reader == null)
                {
                    return 0;
                }

                try
                {
                    char[] buf = new char[bufSize];
                    int charsRead;

                    while ((charsRead = reader.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0)
                    {
                        alreadyRead += charsRead;
                        if (charsRead < bufSize)
                        {
                            break;
                        }
                    }

                    alreadyRead = 0;
                    while ((charsRead = reader.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0)
                    {
                        Array.Copy(buf, 0, buffer, bufferoffset + alreadyRead, charsRead);
                        alreadyRead += charsRead;
                        if (charsRead < bufSize)
                        {
                            break;
                        }
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

            public override long GetBytes(ISqlParameterController controller, IByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length)
            {
                const int bufSize = 4096;
                int alreadyRead = 0;

                var stream = this.GetBinaryStream(controller, mem, controller.ReplyData);
                if (stream == null)
                {
                    return 0;
                }

                try
                {
                    byte[] buf = new byte[bufSize];
                    int bytesRead;

                    while ((bytesRead = stream.Read(buf, 0, (int)(alreadyRead + bufSize < fldOffset ? bufSize : fldOffset - alreadyRead))) > 0)
                    {
                        alreadyRead += bytesRead;
                        if (bytesRead < bufSize)
                        {
                            break;
                        }
                    }

                    alreadyRead = 0;
                    while ((bytesRead = stream.Read(buf, 0, (int)(length - alreadyRead < bufSize ? length - alreadyRead : bufSize))) > 0)
                    {
                        Buffer.BlockCopy(buf, 0, buffer, bufferoffset + alreadyRead, bytesRead);
                        alreadyRead += bytesRead;
                        if (bytesRead < bufSize)
                        {
                            break;
                        }
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

            public override bool IsCaseSensitive => true;

            protected override void PutSpecific(IMaxDBDataPart dataPart, object data)
            {
                var putval = (PutValue)data;
                putval.PutDescriptor(dataPart, this.iBufferPosition - 1);
            }

            public virtual object TransASCIIStreamForInput(Stream stream, int length) =>
                throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIPUTTOLONG));

            public override object TransBinaryStreamForInput(Stream stream, int length) =>
                throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYPUTTOLONG));

            /// <summary>
            /// Translates a byte array. This is done only in derived classes
            /// that accept byte arrays(this one may be a BLOB or a CLOB,
            /// and so does not decide about it).
            /// </summary>
            /// <param name="val">The byte array to bind.</param>
            /// <returns>The Putval instance created for this one.</returns>
            public override object TransBytesForInput(byte[] val) => throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.BINARYPUTTOLONG));

            protected override object TransSpecificForInput(object obj) =>
                obj is Stream stream ? this.TransASCIIStreamForInput(stream, -1) : null;

            public object TransStreamForInput(Stream stream, int length) => stream != null ? new PutValue(stream, length, this.iBufferPosition) : null;

            public override object TransStringForInput(string val) => throw new MaxDBConversionException(MaxDBMessages.Extract(MaxDBError.ASCIIPUTTOLONG));
        }

        /// <summary>
        /// ASCII stream translator.
        /// </summary>
        internal class ASCIIStreamTranslator : StreamTranslator
        {
            private Encoding asciiEncoding;

            public ASCIIStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public Encoding AsciiEncoding
            {
                get
                {
                    return this.asciiEncoding ?? Encoding.GetEncoding(1252);
                }

                set
                {
                    this.asciiEncoding = value;
                }
            }

            public override Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => this.GetStream(controller, mem, longData);

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetString(controller, mem);

            public override object TransASCIIStreamForInput(Stream stream, int length) => this.TransStreamForInput(stream, length);

            public override object TransStringForInput(string val) => val != null ? new PutValue(this.AsciiEncoding.GetBytes(val), this.iBufferPosition) : null;
        }

        /// <summary>
        /// Binary stream translator.
        /// </summary>
        internal class BinaryStreamTranslator : StreamTranslator
        {
            public BinaryStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData) => this.GetStream(controller, mem, longData);

            public override byte GetByte(ISqlParameterController controller, IByteArray mem) => !this.IsDBNull(mem) ? this.GetBytes(controller, mem)[0] : (byte)0;

            public override byte[] GetBytes(ISqlParameterController controller, IByteArray mem)
            {
                MemoryStream tmpStream;

                var blobStream = this.GetBinaryStream(controller, mem, controller.ReplyData);
                if (blobStream == null)
                {
                    return null;
                }

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
                        {
                            break;
                        }

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

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetBytes(controller, mem);

            public override object TransByteForInput(byte val) => this.TransBytesForInput(new[] { val });

            public override object TransBytesForInput(byte[] val) => val != null ? new PutValue(val, this.iBufferPosition) : null;

            public override object TransBinaryStreamForInput(Stream stream, int length) => this.TransStreamForInput(stream, length);

            protected override object TransSpecificForInput(object obj) => obj is Stream stream ? this.TransBinaryStreamForInput(stream, -1) : null;
        }

        /// <summary>
        /// UNICODE stream translator.
        /// </summary>
        internal class UnicodeStreamTranslator : StreamTranslator
        {
            public UnicodeStreamTranslator(int mode, int ioType, int dataType, int len, int ioLen, int bufpos)
                : base(mode, ioType, dataType, len, ioLen, bufpos)
            {
            }

            public override Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                var reader = this.GetCharacterStream(controller, mem, longData);
                return reader != null ? new ReaderStream(reader, false) : null;
            }

            public override object GetValue(ISqlParameterController controller, IByteArray mem) => this.GetString(controller, mem);

            public override TextReader GetCharacterStream(ISqlParameterController controller, IByteArray mem, IByteArray longData)
            {
                TextReader result = null;

                if (!this.IsDBNull(mem))
                {
                    var descriptor = mem.ReadBytes(this.iBufferPosition, this.iLogicalLength);
                    var getval = new GetUnicodeLOBValue(controller.Connection, descriptor, longData);
                    result = getval.CharacterStream;
                }

                return result;
            }

            public override object TransASCIIStreamForInput(Stream stream, int length) => stream != null ? this.TransCharacterStreamForInput(new StreamReader(stream), length) : null;

            public object TransCharacterStreamForInput(TextReader reader, int length) => reader != null ? new PutUnicodeValue(reader, length, this.iBufferPosition) : null;

            public override object TransStringForInput(string val) => val != null ? new PutUnicodeValue(val.ToCharArray(), this.iBufferPosition) : null;
        }
    }
}