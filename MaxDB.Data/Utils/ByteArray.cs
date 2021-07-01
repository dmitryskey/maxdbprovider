//-----------------------------------------------------------------------------------------------
// <copyright file="ByteArray.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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

namespace MaxDB.Data.Utils
{
    using MaxDB.Data.Interfaces.Utils;
    using MaxDB.Data.MaxDBProtocol;
    using System;
    using System.Text;

    /// <summary>
    /// Byte Array class.
    /// </summary>
    internal class ByteArray : IByteArray
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ByteArray"/> class.
        /// </summary>
        /// <param name="data">Data byte array.</param>
        /// <param name="offset">Data offset.</param>
        /// <param name="swapMode">Data swap mode (LE vs BE).</param>
        public ByteArray(byte[] data, int offset = 0, bool? swapMode = null)
        {
            this.Data = data;
            this.Offset = offset;
            this.Swapped = swapMode ?? Consts.IsLittleEndian;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteArray"/> class.
        /// </summary>
        /// <param name="size">Byte array size.</param>
        public ByteArray(int size) => this.Data = new byte[size];

        /// <summary>
        /// Gets a byte array length.
        /// </summary>
        public int Length => this.Data.Length;

        /// <summary>
        /// Gets a value indicating whether the data array is a little-endian or big-endian.
        /// </summary>
        public bool Swapped
        {
            get;
        }

        /// <summary>
        /// Gets or sets data buffer offset.
        /// </summary>
        public int Offset
        {
            get; set;
        }

        /// <summary>
        /// Gets data buffer.
        /// </summary>
        protected byte[] Data { get; }

        /// <summary>
        /// Creates a shallow copy of the <see cref="ByteArray"/>.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <param name="swapMode">Swap mode.</param>
        /// <returns>A shallow copy of the <see cref="ByteArray"/>.</returns>
        public IByteArray Clone(int offset = 0, bool? swapMode = null) =>
            new ByteArray(this.Data, this.Offset + offset, swapMode ?? this.Swapped);

        /// <summary>
        /// Gets byte array data.
        /// </summary>
        /// <returns>A <see cref="byte[]"/> array.</returns>
        public byte[] GetArrayData() => this.Data;

        /// <summary>
        /// Reads the specified number of bytes from the current byte array.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <param name="len">Number of bytes to read.</param>
        /// <returns>A byte array containing data read from the underlying byte array.</returns>
        public byte[] ReadBytes(int offset, int len)
        {
            offset += this.Offset;

            byte[] res = new byte[len];
            Buffer.BlockCopy(this.Data, offset, res, 0, len);
            return res;
        }

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteBytes(byte[] values, int offset)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(values)));
            }

            offset += this.Offset;
            values.CopyTo(this.Data, offset);
        }

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Number of bytes to write.</param>
        public void WriteBytes(byte[] values, int offset, int len)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(values)));
            }

            offset += this.Offset;
            Buffer.BlockCopy(values, 0, this.Data, offset, len);
        }

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Number of bytes to write.</param>
        /// <param name="filler">Bytes to fill out array.</param>
        public void WriteBytes(byte[] values, int offset, int len, byte[] filler)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(values)));
            }

            offset += this.Offset;

            int copyLen = values.Length;
            int fillLen = 0;

            if (copyLen > len)
            {
                copyLen = len;
            }
            else if (copyLen < len)
            {
                fillLen = len - copyLen;
            }

            Buffer.BlockCopy(values, 0, this.Data, offset, copyLen);

            if (fillLen > 0)
            {
                int chunkLen;
                offset += copyLen;

                while (fillLen > 0)
                {
                    chunkLen = Math.Min(fillLen, Consts.FillBufSize);
                    Buffer.BlockCopy(filler, 0, this.Data, offset, chunkLen);
                    fillLen -= chunkLen;
                    offset += chunkLen;
                }
            }

            return;
        }

        /// <summary>
        /// Read a byte from the array.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <returns>The byte value.</returns>
        public byte ReadByte(int offset)
        {
            offset += this.Offset;
            return this.Data[offset];
        }

        /// <summary>
        /// Write a byte to the byte array.
        /// </summary>
        /// <param name="value">Byte value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteByte(byte value, int offset)
        {
            offset += this.Offset;
            this.Data[offset] = value;
        }

        /// <summary>
        /// Read <see cref="ushort"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned short.</returns>
        public ushort ReadUInt16(int offset)
        {
            offset += this.Offset;
            return this.Swapped == BitConverter.IsLittleEndian
                ? BitConverter.ToUInt16(this.Data, offset)
                : this.Swapped ? (ushort)((this.Data[offset + 1] * 0x100) + this.Data[offset]) : (ushort)((this.Data[offset] * 0x100) + this.Data[offset + 1]);
        }

        /// <summary>
        /// Write <see cref="ushort"/>.
        /// </summary>
        /// <param name="value">Unsigned short value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteUInt16(ushort value, int offset) => this.WriteValue(value, offset, 2);

        /// <summary>
        /// Read <see cref="short"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed short.</returns>
        public short ReadInt16(int offset) => this.Swapped == BitConverter.IsLittleEndian ? BitConverter.ToInt16(this.Data, offset + this.Offset) : (short)this.ReadUInt16(offset);

        /// <summary>
        /// Write <see cref="short"/>.
        /// </summary>
        /// <param name="value">Signed short value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteInt16(short value, int offset) => this.WriteValue(value, offset, 2);

        /// <summary>
        /// Read <see cref="uint"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned int.</returns>
        public uint ReadUInt32(int offset) => this.Swapped == BitConverter.IsLittleEndian ? BitConverter.ToUInt32(this.Data, offset + this.Offset) :
                this.Swapped ? (uint)((this.ReadUInt16(offset + 2) * 0x10000) + this.ReadUInt16(offset)) : (uint)((this.ReadUInt16(offset) * 0x10000) + this.ReadUInt16(offset + 2));

        /// <summary>
        /// Write <see cref="uint"/>.
        /// </summary>
        /// <param name="value">Unsigned int value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteUInt32(uint value, int offset) => this.WriteValue(value, offset, 4);

        /// <summary>
        /// Read <see cref="int"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed int.</returns>
        public int ReadInt32(int offset) => this.Swapped == BitConverter.IsLittleEndian ? BitConverter.ToInt32(this.Data, offset + this.Offset) : (int)this.ReadUInt32(offset);

        /// <summary>
        /// Write <see cref="int"/>.
        /// </summary>
        /// <param name="value">Signed int value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteInt32(int value, int offset) => this.WriteValue(value, offset, 4);

        /// <summary>
        /// Read <see cref="ulong"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned long.</returns>
        public ulong ReadUInt64(int offset) => this.Swapped == BitConverter.IsLittleEndian ? BitConverter.ToUInt64(this.Data, offset + this.Offset) :
                this.Swapped ? (ulong)((this.ReadUInt32(offset + 4) * 0x100000000) + this.ReadUInt32(offset)) : (ulong)((this.ReadUInt32(offset) * 0x100000000) + this.ReadUInt32(offset + 4));

        /// <summary>
        /// Read <see cref="long"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed long.</returns>
        public long ReadInt64(int offset) => this.Swapped == BitConverter.IsLittleEndian ? BitConverter.ToInt64(this.Data, offset + this.Offset) : (long)this.ReadUInt64(offset);

        /// <summary>
        /// Write <see cref="long"/>.
        /// </summary>
        /// <param name="value">Signed long value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteInt64(long value, int offset) => this.WriteValue(value, offset, 8);

        /// <summary>
        /// Read ASCII string.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String lenght.</param>
        /// <returns>ASCII string.</returns>
        public string ReadAscii(int offset, int len) => this.ReadEncoding(Encoding.ASCII, offset, len);

        /// <summary>
        /// Read string.
        /// </summary>
        /// <param name="encoding">String encoding.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String length.</param>
        /// <returns>String in the given encoding.</returns>
        public string ReadEncoding(Encoding encoding, int offset, int len)
        {
            offset += this.Offset;

            return encoding.GetString(this.Data, offset, len);
        }

        /// <summary>
        /// Write ASCII string.
        /// </summary>
        /// <param name="value">ASCII string.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteAscii(string value, int offset) => this.WriteEncoding(Encoding.ASCII, value, offset);

        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="encoding">String encoding.</param>
        /// <param name="value">Strign value.</param>
        /// <param name="offset">Array offset.</param>
        public void WriteEncoding(Encoding encoding, string value, int offset) => this.WriteBytes(encoding.GetBytes(value), offset);

        /// <summary>
        /// Read unicode string.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String length to read.</param>
        /// <returns>Unicode stirng.</returns>
        public string ReadUnicode(int offset, int len)
        {
            offset += this.Offset;

            return this.Swapped ? Encoding.Unicode.GetString(this.Data, offset, len) : Encoding.BigEndianUnicode.GetString(this.Data, offset, len);
        }

        /// <summary>
        /// Write unicode string.
        /// </summary>
        /// <param name="value">Unicode string.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Stirng length.</param>
        public void WriteUnicode(string value, int offset, int len = -1)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(value)));
            }

            if (len < 0)
            {
                len = value.Length;
            }

            if (this.Swapped)
            {
                this.WriteBytes(Encoding.Unicode.GetBytes(value), offset, len * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
            else
            {
                this.WriteBytes(Encoding.BigEndianUnicode.GetBytes(value), offset, len * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
        }

        private void WriteValue(long value, int offset, int bytes)
        {
            offset += this.Offset;

            for (int i = 0; i < bytes; i++)
            {
                this.Data[(this.Swapped ? i : bytes - i - 1) + offset] = (byte)(value & 0xFF);

                value >>= 8;
            }
        }
    }
}
