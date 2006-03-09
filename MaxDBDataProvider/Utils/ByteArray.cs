using System;
using System.Text;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
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
			else if (copyLen <  len) 
				fillLen = len - copyLen;
			Array.Copy(values, 0, m_data, offset, copyLen);
			
			if (fillLen > 0)
			{
				int chunkLen;
				offset += copyLen;

				while (fillLen > 0) 
				{
					chunkLen = Math.Min(fillLen, MaxDBProtocol.Consts.fillBufSize);
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
			if (m_swapMode)
				return (ushort)(m_data[offset + 1] * 0x100 + m_data[offset]);
			else
				return (ushort)(m_data[offset] * 0x100 + m_data[offset + 1]);
		}

		public void writeUInt16(ushort val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public short readInt16(int offset)
		{
			offset += m_offset;
			return (short)ReadUInt16(offset);
		}

		public void writeInt16(short val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public uint readUInt32(int offset)
		{
			offset += m_offset;
			if (m_swapMode)
				return (uint)(ReadUInt16(offset + 2) * 0x10000 + ReadUInt16(offset));
			else
				return (uint)(ReadUInt16(offset) * 0x10000 + ReadUInt16(offset + 2));
		}

		public void writeUInt32(uint val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public int ReadInt32(int offset)
		{
			offset += m_offset;
			return (int)readUInt32(offset);
		}

		public void WriteInt32(int val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public ulong readUInt64(int offset)
		{
			if (m_swapMode)
				return (ulong)(readUInt32(offset + 4) * 0x100000000 + readUInt32(offset));
			else
				return (ulong)(readUInt32(offset) * 0x100000000 + readUInt32(offset + 4));
		}

		public void writeUInt64(ulong val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public long readInt64(int offset)
		{
			offset += m_offset;
			return (long)readUInt64(offset);
		}

		public void writeInt64(long val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public float readFloat(int offset)
		{
			offset += m_offset;
			return BitConverter.ToSingle(m_data, offset);
		}

		public void writeFloat(float val, int offset)
		{
			WriteBytes(BitConverter.GetBytes(val), offset);
		}

		public double readDouble(int offset)
		{
			offset += m_offset;
			return BitConverter.ToDouble(m_data, offset);
		}

		public void writeDouble(double val, int offset)
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
			WriteBytes(Encoding.ASCII.GetBytes(val), offset, len, Consts.blankBytes);
		}

		public string readUnicode(int offset, int len)
		{
			offset += m_offset;
			if (m_swapMode)
				return Encoding.Unicode.GetString(m_data, offset, len);
			else
				return Encoding.BigEndianUnicode.GetString(m_data, offset, len);
		}

		public void writeUnicode(string val, int offset)
		{
			if (m_swapMode)
				WriteBytes(Encoding.Unicode.GetBytes(val), offset);
			else
				WriteBytes(Encoding.BigEndianUnicode.GetBytes(val), offset);
		}

		public void writeUnicode(string val, int offset, int len)
		{
			if (m_swapMode)
				WriteBytes(Encoding.Unicode.GetBytes(val), offset, len, Consts.blankUnicodeBytes);
			else
				WriteBytes(Encoding.BigEndianUnicode.GetBytes(val), offset, len, Consts.blankBigEndianUnicodeBytes);
		}

		protected void writeValue(ulong val, int offset, int bytes)
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

		protected void writeValue(long val, int offset, int bytes)
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
