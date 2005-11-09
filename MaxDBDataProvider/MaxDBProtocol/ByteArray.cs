using System;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for ByteArray.
	/// </summary>
	internal class ByteArray
	{
		protected byte[] data; //data buffer
		protected int offset; //working area offset in the buffer
		protected int size; //size of the working area

		public ByteArray(byte[] data)
		{
			this.data = data;
			this.offset = 0;
			this.size = data.Length;
		}

		public ByteArray(byte[] data, int offset)
		{
			this.data = data;
			this.offset = offset;
			this.size = data.Length - offset;
		}

		public ByteArray(int size)
		{
			this.data = new byte[size];
			this.offset = 0;
			this.size = data.Length;
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
			data.CopyTo(res, this.offset + offset);
			return res;
		}

		public void writeBytes(byte[] values, int offset)
		{
			values.CopyTo(data, this.offset + offset);
		}

		public byte readByte(int offset)
		{
			return data[this.offset + offset];
		}

		public void writeByte(byte val, int offset)
		{
			data[this.offset + offset] = val;
		}

		public ushort readUInt16(int offset)
		{
			return BitConverter.ToUInt16(data, this.offset + offset);
		}

		public void writeUInt16(ushort val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public short readInt16(int offset)
		{
			return BitConverter.ToInt16(data, this.offset + offset);
		}

		public void writeInt16(short val, int offset)
		{
			writeValue(val, offset, 2);
		}

		public uint readUInt32(int offset)
		{
			return BitConverter.ToUInt32(data, this.offset + offset);
		}

		public void writeUInt32(uint val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public int readInt32(int offset)
		{
			return BitConverter.ToInt32(data, this.offset + offset);
		}

		public void writeInt32(int val, int offset)
		{
			writeValue(val, offset, 4);
		}

		public ulong readUInt64(int offset)
		{
			return BitConverter.ToUInt64(data, this.offset + offset);
		}

		public void writeUInt64(ulong val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public long readInt64(int offset)
		{
			return BitConverter.ToInt64(data, this.offset + offset);
		}

		public void writeInt64(long val, int offset)
		{
			writeValue(val, offset, 8);
		}

		public float readFloat(int offset)
		{
			return BitConverter.ToSingle(data, this.offset + offset);
		}

		public void writeFloat(float val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public double readDouble(int offset)
		{
			return BitConverter.ToDouble(data, this.offset + offset);
		}

		public void writeDouble(double val, int offset)
		{
			writeBytes(BitConverter.GetBytes(val), offset);
		}

		public string readASCII(int offset, int len)
		{
			return Encoding.ASCII.GetString(data, this.offset + offset, len);
		}

		public void writeASCII(string val, int offset)
		{
			Encoding.ASCII.GetBytes(val).CopyTo(data, this.offset + offset);
		}

		public string readUnicode(int offset, int len)
		{
			return Encoding.Unicode.GetString(data, this.offset + offset, len);
		}

		public void writeUnicode(string val, int offset)
		{
			Encoding.Unicode.GetBytes(val).CopyTo(data, this.offset + offset);
		}

		protected void writeValue(ulong val, int offset, int bytes)
		{
			for(int i = 0; i < bytes; i++)
			{
				if (BitConverter.IsLittleEndian)
					data[i] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}

		protected void writeValue(long val, int offset, int bytes)
		{
			for(int i = 0; i < bytes; i++)
			{
				if (BitConverter.IsLittleEndian)
					data[i] = (byte)(val & 0xFF);
				else
					data[bytes - i - 1] = (byte)(val & 0xFF);
				val >>= 8;
			}
		}
	}
}
