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
using System.Text;
using MaxDB.Data.MaxDBProtocol;

namespace MaxDB.Data.Utilities
{
	/// <summary>
	/// Summary description for ByteArray.
	/// </summary>
	internal class ByteArray
	{
		protected byte[] byData; //data buffer
		protected bool bSwapMode = Consts.IsLittleEndian;
 
#if SAFE
        protected int iOffset; //data buffer offset

        public ByteArray(byte[] data)
		{
			byData = data;
		}

        public ByteArray(byte[] data, int offset)
		{
			byData = data;
			iOffset = offset; 
		}

        public ByteArray(byte[] data, int offset, bool swapMode)
		{
			byData = data;
			iOffset = offset; 
			bSwapMode = swapMode;
		}

        public ByteArray(int size)
		{
			byData = new byte[size];
		}

#else
        public ByteArray(int size, bool swapMode)
        {
            byData = new byte[size];
            bSwapMode = swapMode;
        }
#endif // !SAFE

#if SAFE
#if NET20
        public ByteArray Clone()
        {
            return new ByteArray(byData, iOffset, bSwapMode); 
        }
#endif // NET20

		public ByteArray Clone(int offset)
		{
			return new ByteArray(byData, iOffset + offset, bSwapMode); 
		}

		public ByteArray Clone(int offset, bool swapMode)
		{
			return new ByteArray(byData, iOffset + offset, swapMode); 
		}

        public int Length
		{
			get
			{
				return byData.Length;
			}
		}

        public bool Swapped
		{
			get
			{
				return bSwapMode;
			}
        }

        public int Offset
		{
			get
			{
				return iOffset;
			}
			set
			{
				iOffset = value;
			}
		}
#endif // SAFE

        public byte[] GetArrayData()
		{
			return byData;
		}

		public byte[] ReadBytes(int offset, int len)
		{
#if SAFE
			offset += iOffset;
#endif // SAFE
			byte[] res = new byte[len];
			Array.Copy(byData, offset, res, 0, len);
			return res;
		}

		public void WriteBytes(byte[] values, int offset)
		{
            if (values == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));

#if SAFE
			offset += iOffset;
#endif // SAFE
            values.CopyTo(byData, offset);
		}

#if SAFE
		public void WriteBytes(byte[] values, int offset, int len)
        {
			offset += iOffset;
            Array.Copy(values, 0, byData, offset, len);
		}
#endif // SAFE

		public void WriteBytes(byte[] values, int offset, int len, byte[] filler)
		{
            if (values == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "values"));

#if SAFE
			offset += iOffset;
#endif // SAFE

            int copyLen = values.Length;
			int fillLen = 0;

			if (copyLen > len) 
				copyLen = len;
			else if (copyLen < len) 
				fillLen = len - copyLen;
			Array.Copy(values, 0, byData, offset, copyLen);
			
			if (fillLen > 0)
			{
				int chunkLen;
				offset += copyLen;

				while(fillLen > 0) 
				{
					chunkLen = Math.Min(fillLen, Consts.FillBufSize);
					Array.Copy(filler, 0, byData, offset, chunkLen);
					fillLen -= chunkLen;
					offset += chunkLen;
				}
			}
			
			return;
		}

#if SAFE
		public byte ReadByte(int offset)
        {

			offset += iOffset;
            return byData[offset];
		}

		public void WriteByte(byte value, int offset)
        {
			offset += iOffset;
            byData[offset] = value;
		}

		public ushort ReadUInt16(int offset)
        {
			offset += iOffset;
            if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToUInt16(byData, offset);
			else
			{
				if (bSwapMode)
					return (ushort)(byData[offset + 1] * 0x100 + byData[offset]);
				else
					return (ushort)(byData[offset] * 0x100 + byData[offset + 1]);
			}
		}


		public void WriteUInt16(ushort value, int offset)
		{
			WriteValue(value, offset, 2);
		}

		public short ReadInt16(int offset)
        {

			offset += iOffset;
            if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToInt16(byData, offset);
			else
				return (short)ReadUInt16(offset);
		}

		public void WriteInt16(short value, int offset)
		{
			WriteValue(value, offset, 2);
		}

		public uint ReadUInt32(int offset)
        {
			offset += iOffset;
            if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToUInt32(byData, offset);
			else
			{
				if (bSwapMode)
					return (uint)(ReadUInt16(offset + 2) * 0x10000 + ReadUInt16(offset));
				else
					return (uint)(ReadUInt16(offset) * 0x10000 + ReadUInt16(offset + 2));
			}
		}

		public void WriteUInt32(uint value, int offset)
		{
			WriteValue(value, offset, 4);
		}

		public int ReadInt32(int offset)
        {

			offset += iOffset;

            if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToInt32(byData, offset);
			else
				return (int)ReadUInt32(offset);
		}

		public void WriteInt32(int value, int offset)
		{
			WriteValue(value, offset, 4);
        }

		public ulong ReadUInt64(int offset)
		{
			if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToUInt64(byData, offset);
			else
			{
				if (bSwapMode)
					return (ulong)(ReadUInt32(offset + 4) * 0x100000000 + ReadUInt32(offset));
				else
					return (ulong)(ReadUInt32(offset) * 0x100000000 + ReadUInt32(offset + 4));
			}
		}

		public long ReadInt64(int offset)
		{
			offset += iOffset;
			if (BitConverter.IsLittleEndian == bSwapMode)
				return BitConverter.ToInt64(byData, offset);
			else
				return (long)ReadUInt64(offset);
		}

        public void WriteInt64(long value, int offset)
		{
			WriteValue(value, offset, 8);
		}

		public string ReadAscii(int offset, int len)
        {

			offset += iOffset;
            return Encoding.ASCII.GetString(byData, offset, len);
		}

#endif // SAFE

		public void WriteAscii(string value, int offset)
		{
			WriteBytes(Encoding.ASCII.GetBytes(value), offset);
		}

		public string ReadUnicode(int offset, int len)
        {
#if SAFE
			offset += iOffset;
#endif // SAFE
            if (bSwapMode)
				return Encoding.Unicode.GetString(byData, offset, len);
			else
				return Encoding.BigEndianUnicode.GetString(byData, offset, len);
		}

		public void WriteUnicode(string value, int offset)
		{
            if (value == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

			WriteUnicode(value, offset, value.Length);
		}

		public void WriteUnicode(string value, int offset, int len)
		{
            if (value == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

			if (bSwapMode)
				WriteBytes(Encoding.Unicode.GetBytes(value), offset, len * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
			else
                WriteBytes(Encoding.BigEndianUnicode.GetBytes(value), offset, len * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
		}

#if !SAFE
        public double ReadDouble(int offset)
        {
            return BitConverter.ToDouble(byData, offset);
        }

        public void WriteDouble(double val, int offset)
        {
            WriteBytes(BitConverter.GetBytes(val), offset);
        }
#endif // !SAFE

#if SAFE
		private void WriteValue(long value, int offset, int bytes)
        {
			offset += iOffset;

            for (int i = 0; i < bytes; i++)
			{
				if (bSwapMode)
					byData[i + offset] = (byte)(value & 0xFF);
				else
					byData[bytes - i - 1 + offset] = (byte)(value & 0xFF);
				value >>= 8;
			}
		}
#endif // SAFE
	}
}
