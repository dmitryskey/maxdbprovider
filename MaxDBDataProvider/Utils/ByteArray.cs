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
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider.Utils
{
	/// <summary>
	/// Summary description for ByteArray.
	/// </summary>
	public class ByteArray
	{
		protected byte[] m_data; //data buffer
		protected int m_offset = 0; //data buffer offset
		protected bool m_swapMode = Consts.IsLittleEndian;

		public ByteArray(byte[] data)
		{
			m_data = data;
		}

		public ByteArray(byte[] data, int offset)
		{
			m_data = data;
			m_offset = offset; 
		}

		public ByteArray(byte[] data, int offset, bool swapMode)
		{
			m_data = data;
			m_offset = offset; 
			m_swapMode = swapMode;
		}

		public ByteArray(int size)
		{
			m_data = new byte[size];
		}

		public ByteArray(int size, bool swapMode)
		{
			m_data = new byte[size];
			m_swapMode = swapMode;
		}

		public ByteArray(int size, bool swapMode, int offset)
		{
			m_data = new byte[size];
			m_swapMode = swapMode;
			m_offset = offset;
		}

		public ByteArray Clone(int offset)
		{
			return new ByteArray(m_data, m_offset + offset, m_swapMode); 
		}

		public ByteArray Clone(int offset, bool swapMode)
		{
			return new ByteArray(m_data, m_offset + offset, swapMode); 
		}

		public byte[] arrayData
		{
			get
			{
				return m_data;
			}
		}

		public int Length
		{
			get
			{
				return m_data.Length;
			}
		}

		public int Offset
		{
			get
			{
				return m_offset;
			}
			set
			{
				m_offset = value;
			}
		}

		public bool Swapped
		{
			get
			{
				return m_swapMode;
			}
		}

		public byte[] ReadBytes(int offset, int len)
		{
			offset += m_offset;
			byte[] res = new byte[len];
			Array.Copy(m_data, offset, res, 0, len);
			return res;
		}

		public void WriteBytes(byte[] values, int offset)
		{
			offset += m_offset;
			values.CopyTo(m_data, offset);
		}

		public void WriteBytes(byte[] values, int offset, int len)
		{
			offset += m_offset;
			Array.Copy(values, 0, m_data, offset, len);
		}

		public void WriteBytes(byte[] values, int offset, int len, byte[] filler)
		{
			offset += m_offset;

			int copyLen = values.Length;
			int fillLen = 0;

			if (copyLen > len) 
				copyLen = len;
			else if (copyLen < len) 
				fillLen = len - copyLen;
			Array.Copy(values, 0, m_data, offset, copyLen);
			
			if (fillLen > 0)
			{
				int chunkLen;
				offset += copyLen;

				while(fillLen > 0) 
				{
					chunkLen = Math.Min(fillLen, Consts.FillBufSize);
					Array.Copy(filler, 0, m_data, offset, chunkLen);
					fillLen -= chunkLen;
					offset += chunkLen;
				}
			}
			
			return;
		}

		public byte ReadByte(int offset)
		{
			offset += m_offset;
			return m_data[offset];
		}

		public void WriteByte(byte val, int offset)
		{
			offset += m_offset;
			m_data[offset] = val;
		}

		public ushort ReadUInt16(int offset)
		{
			offset += m_offset;
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToUInt16(m_data, offset);
			else
			{
				if (m_swapMode)
					return (ushort)(m_data[offset + 1] * 0x100 + m_data[offset]);
				else
					return (ushort)(m_data[offset] * 0x100 + m_data[offset + 1]);
			}
		}

		public void WriteUInt16(ushort val, int offset)
		{
			WriteValue(val, offset, 2);
		}

		public short ReadInt16(int offset)
		{
			offset += m_offset;
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToInt16(m_data, offset);
			else
				return (short)ReadUInt16(offset);
		}

		public void WriteInt16(short val, int offset)
		{
			WriteValue(val, offset, 2);
		}

		public uint ReadUInt32(int offset)
		{
			offset += m_offset;
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToUInt32(m_data, offset);
			else
			{
				if (m_swapMode)
					return (uint)(ReadUInt16(offset + 2) * 0x10000 + ReadUInt16(offset));
				else
					return (uint)(ReadUInt16(offset) * 0x10000 + ReadUInt16(offset + 2));
			}
		}

		public void WriteUInt32(uint val, int offset)
		{
			WriteValue(val, offset, 4);
		}

		public int ReadInt32(int offset)
		{
			offset += m_offset;
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToInt32(m_data, offset);
			else
				return (int)ReadUInt32(offset);
		}

		public void WriteInt32(int val, int offset)
		{
			WriteValue(val, offset, 4);
		}

		public ulong ReadUInt64(int offset)
		{
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToUInt64(m_data, offset);
			else
			{
				if (m_swapMode)
					return (ulong)(ReadUInt32(offset + 4) * 0x100000000 + ReadUInt32(offset));
				else
					return (ulong)(ReadUInt32(offset) * 0x100000000 + ReadUInt32(offset + 4));
			}
		}

		public void WriteUInt64(ulong val, int offset)
		{
			WriteValue(val, offset, 8);
		}

		public long ReadInt64(int offset)
		{
			offset += m_offset;
			if (BitConverter.IsLittleEndian == m_swapMode)
				return BitConverter.ToInt64(m_data, offset);
			else
				return (long)ReadUInt64(offset);
		}

		public void WriteInt64(long val, int offset)
		{
			WriteValue(val, offset, 8);
		}

		public float ReadFloat(int offset)
		{
			offset += m_offset;
			return BitConverter.ToSingle(m_data, offset);
		}

		public void WriteFloat(float val, int offset)
		{
			WriteBytes(BitConverter.GetBytes(val), offset);
		}

		public double ReadDouble(int offset)
		{
			offset += m_offset;
			return BitConverter.ToDouble(m_data, offset);
		}

		public void WriteDouble(double val, int offset)
		{
			WriteBytes(BitConverter.GetBytes(val), offset);
		}

		public string ReadASCII(int offset, int len)
		{
			offset += m_offset;
			return Encoding.ASCII.GetString(m_data, offset, len);
		}

		public void WriteASCII(string val, int offset)
		{
			WriteBytes(Encoding.ASCII.GetBytes(val), offset);
		}

		public void WriteASCII(string val, int offset, int len)
		{
			WriteBytes(Encoding.ASCII.GetBytes(val), offset, len, Consts.BlankBytes);
		}

		public string ReadUnicode(int offset, int len)
		{
			offset += m_offset;
			if (m_swapMode)
				return Encoding.Unicode.GetString(m_data, offset, len);
			else
				return Encoding.BigEndianUnicode.GetString(m_data, offset, len);
		}

		public void WriteUnicode(string val, int offset)
		{
			WriteUnicode(val, offset, val.Length);
		}

		public void WriteUnicode(string val, int offset, int len)
		{
			if (m_swapMode)
				WriteBytes(Encoding.Unicode.GetBytes(val), offset, len, Consts.BlankUnicodeBytes);
			else
				WriteBytes(Encoding.BigEndianUnicode.GetBytes(val), offset, len, Consts.BlankBigEndianUnicodeBytes);
		}

		protected void WriteValue(ulong val, int offset, int bytes)
		{
			offset += m_offset;
			for(int i = 0; i < bytes; i++)
			{
				if (m_swapMode)
					m_data[i + offset] = (byte)(val & 0xFF);
				else
					m_data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}

		protected void WriteValue(long val, int offset, int bytes)
		{
			offset += m_offset;
			for(int i = 0; i < bytes; i++)
			{
				if (m_swapMode)
					m_data[i + offset] = (byte)(val & 0xFF);
				else
					m_data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}
	}
}
