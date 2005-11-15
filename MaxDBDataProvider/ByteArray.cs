using System;
using System.Text;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for ByteArray.
	/// </summary>
	public class ByteArray
	{
		protected byte[] data; //data buffer
		protected bool IsLittleEndian;

		public ByteArray(byte[] data)
		{
			this.data = data;
			IsLittleEndian = BitConverter.IsLittleEndian;
		}

		public ByteArray(byte[] data, bool swapMode)
		{
			this.data = data;
			IsLittleEndian = swapMode;
		}

		public ByteArray(int size)
		{
			this.data = new byte[size];
			IsLittleEndian = BitConverter.IsLittleEndian;
		}

		public ByteArray(int size, bool swapMode)
		{
			this.data = new byte[size];
			IsLittleEndian = swapMode;
		}

		public byte[] arrayData
		{
			get
			{
				return data;
			}
		}

		public byte[] readBytes(int offset, int len)
		{
			byte[] res = new byte[len];
			data.CopyTo(res, offset);
			return res;
		}

		public void writeBytes(byte[] values, int offset)
		{
			values.CopyTo(data, offset);
		}

		public byte readByte(int offset)
		{
			return data[offset];
		}

		public void writeByte(byte val, int offset)
		{
			data[offset] = val;
		}

		public ushort readUInt16(int offset)
		{
			if (IsLittleEndian)
				return (ushort)(data[offset + 1] * 0x100 + data[offset]);
			else
				return (ushort)(data[offset] * 0x100 + data[offset + 1]);
		}

		public void writeUInt16(ushort val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public short readInt16(int offset)
		{
			return (short)readUInt16(offset);
		}

		public void writeInt16(short val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public uint readUInt32(int offset)
		{
			if (IsLittleEndian)
				return (uint)(readUInt16(offset + 2) * 0x10000 + readUInt16(offset));
			else
				return (uint)(readUInt16(offset) * 0x10000 + readUInt16(offset + 2));
		}

		public void writeUInt32(uint val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public int readInt32(int offset)
		{
			return (int)readUInt32(offset);
		}

		public void writeInt32(int val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public ulong readUInt64(int offset)
		{
			if (IsLittleEndian)
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
			return (long)readUInt64(offset);
		}

		public void writeInt64(long val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public float readFloat(int offset)
		{
			return BitConverter.ToSingle(data, offset);
		}

		public void writeFloat(float val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public double readDouble(int offset)
		{
			return BitConverter.ToDouble(data, offset);
		}

		public void writeDouble(double val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public string readASCII(int offset, int len)
		{
			return Encoding.ASCII.GetString(data, offset, len);
		}

		public void writeASCII(string val, int offset)
		{
			Encoding.ASCII.GetBytes(val).CopyTo(data, offset);
		}

		public string readUnicode(int offset, int len)
		{
			if (IsLittleEndian)
				return Encoding.Unicode.GetString(data, offset, len);
			else
				return Encoding.BigEndianUnicode.GetString(data, offset, len);
		}

		public void writeUnicode(string val, int offset)
		{
			if (IsLittleEndian)
				Encoding.Unicode.GetBytes(val).CopyTo(data, offset);
			else
				Encoding.BigEndianUnicode.GetBytes(val).CopyTo(data, offset);
		}

		protected void writeValue(ulong val, int offset, int bytes)
		{
			for(int i = 0; i < bytes; i++)
			{
				if (IsLittleEndian)
					data[i + offset] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}

		protected void writeValue(long val, int offset, int bytes)
		{
			for(int i = 0; i < bytes; i++)
			{
				if (IsLittleEndian)
					data[i + offset] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}
	}
}
