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
		protected int offset = 0; //data buffer offset
		protected bool swapMode;

		public ByteArray(byte[] data)
		{
			this.data = data;
			this.swapMode = BitConverter.IsLittleEndian;
		}

		public ByteArray(byte[] data, bool swapMode)
		{
			this.data = data;
			this.swapMode = swapMode;
		}

		public ByteArray(byte[] data, bool swapMode, int offset)
		{
			this.data = data;
			this.swapMode = swapMode;
			this.offset = offset; 
		}

		public ByteArray(int size)
		{
			this.data = new byte[size];
			this.swapMode = BitConverter.IsLittleEndian;
		}

		public ByteArray(int size, bool swapMode)
		{
			this.data = new byte[size];
			this.swapMode = swapMode;
		}

		public ByteArray(int size, bool swapMode, int offset)
		{
			this.data = new byte[size];
			this.swapMode = swapMode;
			this.offset = offset;
		}

		public ByteArray Clone(int offset)
		{
			return new ByteArray(this.data, this.swapMode, this.offset + offset); 
		}

		public byte[] arrayData
		{
			get
			{
				return data;
			}
		}

		public int Length
		{
			get
			{
				return data.Length;
			}
		}

		public int Offset
		{
			get
			{
				return offset;
			}
		}

		public byte[] readBytes(int offset, int len)
		{
			offset += this.offset;
			byte[] res = new byte[len];
			data.CopyTo(res, offset);
			return res;
		}

		public void writeBytes(byte[] values, int offset)
		{
			offset += this.offset;
			values.CopyTo(data, offset);
		}

		public void writeBytes(byte[] values, int offset, int len)
		{
			offset += this.offset;
			Array.Copy(values, 0, data, offset, len);
		}

		public byte readByte(int offset)
		{
			offset += this.offset;
			return data[offset];
		}

		public void writeByte(byte val, int offset)
		{
			offset += this.offset;
			data[offset] = val;
		}

		public ushort readUInt16(int offset)
		{
			offset += this.offset;
			if (swapMode)
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
			offset += this.offset;
			return (short)readUInt16(offset);
		}

		public void writeInt16(short val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public uint readUInt32(int offset)
		{
			offset += this.offset;
			if (swapMode)
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
			offset += this.offset;
			return (int)readUInt32(offset);
		}

		public void writeInt32(int val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public ulong readUInt64(int offset)
		{
			if (swapMode)
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
			offset += this.offset;
			return (long)readUInt64(offset);
		}

		public void writeInt64(long val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public float readFloat(int offset)
		{
			offset += this.offset;
			return BitConverter.ToSingle(data, offset);
		}

		public void writeFloat(float val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public double readDouble(int offset)
		{
			offset += this.offset;
			return BitConverter.ToDouble(data, offset);
		}

		public void writeDouble(double val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public string readASCII(int offset, int len)
		{
			offset += this.offset;
			return Encoding.ASCII.GetString(data, offset, len);
		}

		public void writeASCII(string val, int offset)
		{
			offset += this.offset;
			Encoding.ASCII.GetBytes(val).CopyTo(data, offset);
		}

		public string readUnicode(int offset, int len)
		{
			offset += this.offset;
			if (swapMode)
				return Encoding.Unicode.GetString(data, offset, len);
			else
				return Encoding.BigEndianUnicode.GetString(data, offset, len);
		}

		public void writeUnicode(string val, int offset)
		{
			offset += this.offset;
			if (swapMode)
				Encoding.Unicode.GetBytes(val).CopyTo(data, offset);
			else
				Encoding.BigEndianUnicode.GetBytes(val).CopyTo(data, offset);
		}

		protected void writeValue(ulong val, int offset, int bytes)
		{
			offset += this.offset;
			for(int i = 0; i < bytes; i++)
			{
				if (swapMode)
					data[i + offset] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}

		protected void writeValue(long val, int offset, int bytes)
		{
			offset += this.offset;
			for(int i = 0; i < bytes; i++)
			{
				if (swapMode)
					data[i + offset] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1 + offset] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}
	}
}
