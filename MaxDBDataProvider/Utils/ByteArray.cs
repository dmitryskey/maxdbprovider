// Copyright © 2005-2018 Dmitry S. Kataev
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

namespace MaxDB.Data.Utilities
{
    using System;
    using System.Text;
    using MaxDB.Data.MaxDBProtocol;

    /// <summary>
    /// Summary description for ByteArray.
    /// </summary>
    internal class ByteArray
    {
        protected byte[] byData; // data buffer
        protected bool bSwapMode = Consts.IsLittleEndian; // is data array little-endian or big-endian

        protected int iOffset; // data buffer offset

        public ByteArray(byte[] data) => this.byData = data;

        public ByteArray(byte[] data, int offset)
        {
            this.byData = data;
            this.iOffset = offset;
        }

        public ByteArray(byte[] data, int offset, bool swapMode)
        {
            this.byData = data;
            this.iOffset = offset;
            this.bSwapMode = swapMode;
        }

        public ByteArray(int size) => this.byData = new byte[size];

        public ByteArray Clone(int offset = 0) => new ByteArray(this.byData, this.iOffset + offset, this.bSwapMode);

        public ByteArray Clone(int offset, bool swapMode) => new ByteArray(this.byData, this.iOffset + offset, swapMode);

        public int Length => this.byData.Length;

        public bool Swapped => this.bSwapMode;

        public int Offset
        {
            get => this.iOffset;

            set => this.iOffset = value;
        }

        public byte[] GetArrayData() => this.byData;

        public byte[] ReadBytes(int offset, int len)
        {
            offset += this.iOffset;

            byte[] res = new byte[len];
            Buffer.BlockCopy(this.byData, offset, res, 0, len);
            return res;
        }

        public void WriteBytes(byte[] values, int offset)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));
            }

            offset += this.iOffset;
            values.CopyTo(this.byData, offset);
        }

        public void WriteBytes(byte[] values, int offset, int len)
        {
            offset += this.iOffset;
            Buffer.BlockCopy(values, 0, this.byData, offset, len);
        }

        public void WriteBytes(byte[] values, int offset, int len, byte[] filler)
        {
            if (values == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));
            }

            offset += this.iOffset;

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

            Buffer.BlockCopy(values, 0, this.byData, offset, copyLen);

            if (fillLen > 0)
            {
                int chunkLen;
                offset += copyLen;

                while (fillLen > 0)
                {
                    chunkLen = Math.Min(fillLen, Consts.FillBufSize);
                    Buffer.BlockCopy(filler, 0, this.byData, offset, chunkLen);
                    fillLen -= chunkLen;
                    offset += chunkLen;
                }
            }

            return;
        }

        public byte ReadByte(int offset)
        {
            offset += this.iOffset;
            return this.byData[offset];
        }

        public void WriteByte(byte value, int offset)
        {
            offset += this.iOffset;
            this.byData[offset] = value;
        }

        public ushort ReadUInt16(int offset)
        {
            offset += this.iOffset;
            return BitConverter.IsLittleEndian == this.bSwapMode
                ? BitConverter.ToUInt16(this.byData, offset)
                : this.bSwapMode ? (ushort)((this.byData[offset + 1] * 0x100) + this.byData[offset]) : (ushort)((this.byData[offset] * 0x100) + this.byData[offset + 1]);
        }

        public void WriteUInt16(ushort value, int offset) => this.WriteValue(value, offset, 2);

        public short ReadInt16(int offset) => BitConverter.IsLittleEndian == this.bSwapMode ? BitConverter.ToInt16(this.byData, offset + this.iOffset) : (short)this.ReadUInt16(offset);

        public void WriteInt16(short value, int offset) => this.WriteValue(value, offset, 2);

        public uint ReadUInt32(int offset) => BitConverter.IsLittleEndian == this.bSwapMode ? BitConverter.ToUInt32(this.byData, offset + this.iOffset) :
                this.bSwapMode ? (uint)((this.ReadUInt16(offset + 2) * 0x10000) + this.ReadUInt16(offset)) : (uint)((this.ReadUInt16(offset) * 0x10000) + this.ReadUInt16(offset + 2));

        public void WriteUInt32(uint value, int offset) => this.WriteValue(value, offset, 4);

        public int ReadInt32(int offset) => BitConverter.IsLittleEndian == this.bSwapMode ? BitConverter.ToInt32(this.byData, offset + this.iOffset) : (int)this.ReadUInt32(offset);

        public void WriteInt32(int value, int offset) => this.WriteValue(value, offset, 4);

        public ulong ReadUInt64(int offset) => BitConverter.IsLittleEndian == this.bSwapMode ? BitConverter.ToUInt64(this.byData, offset + this.iOffset) :
                this.bSwapMode ? (ulong)((this.ReadUInt32(offset + 4) * 0x100000000) + this.ReadUInt32(offset)) : (ulong)((this.ReadUInt32(offset) * 0x100000000) + this.ReadUInt32(offset + 4));

        public long ReadInt64(int offset) => BitConverter.IsLittleEndian == this.bSwapMode ? BitConverter.ToInt64(this.byData, offset + this.iOffset) : (long)this.ReadUInt64(offset);

        public void WriteInt64(long value, int offset) => this.WriteValue(value, offset, 8);

        public string ReadAscii(int offset, int len) => this.ReadEncoding(Encoding.ASCII, offset, len);

        public string ReadEncoding(Encoding encoding, int offset, int len)
        {
            offset += this.iOffset;

            return encoding.GetString(this.byData, offset, len);
        }

        public void WriteAscii(string value, int offset) => this.WriteEncoding(Encoding.ASCII, value, offset);

        public void WriteEncoding(Encoding encoding, string value, int offset) => this.WriteBytes(encoding.GetBytes(value), offset);

        public string ReadUnicode(int offset, int len)
        {
            offset += this.iOffset;

            return this.bSwapMode ? Encoding.Unicode.GetString(this.byData, offset, len) : Encoding.BigEndianUnicode.GetString(this.byData, offset, len);
        }

        public void WriteUnicode(string value, int offset)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            this.WriteUnicode(value, offset, value.Length);
        }

        public void WriteUnicode(string value, int offset, int len)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            if (this.bSwapMode)
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
            offset += this.iOffset;

            for (int i = 0; i < bytes; i++)
            {
                if (this.bSwapMode)
                {
                    this.byData[i + offset] = (byte)(value & 0xFF);
                }
                else
                {
                    this.byData[bytes - i - 1 + offset] = (byte)(value & 0xFF);
                }

                value >>= 8;
            }
        }
    }
}
