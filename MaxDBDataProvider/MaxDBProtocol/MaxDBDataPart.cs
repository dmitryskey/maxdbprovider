using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "DataPart class"

	public abstract class DataPart
	{
		virtual protected internal int MaxDataSize
		{
			get
			{
				return origData.Length - this.extent - 8;
			}
		}

		virtual public int Extent
		{
			get
			{
				return extent;
			}
		}

		public int Length
		{
			get
			{
				if (origData != null)
					return origData.Length;
				else
					return 0;
			}
		}

		private static readonly int maxArgCount = System.Int16.MaxValue;
		protected internal short argCount = 0;
		protected internal int extent = 0;
		protected internal int massExtent = 0;
		internal ByteArray origData;
		
		internal MaxDBRequestPacket reqPacket;
		
		internal DataPart(ByteArray data, MaxDBRequestPacket packet)
		{
			origData = data;
			reqPacket = packet;
		}
		
		public abstract void  addArg(int pos, int len);
		
		public virtual void Close()
		{
			int argCountOffs = - PartHeaderOffset.Data + PartHeaderOffset.ArgCount;
			origData.writeInt16(argCount, argCountOffs);
			reqPacket.closePart(massExtent + extent, argCount);
		}
		
		public virtual void closeArrayPart(short rows)
		{
			int argCountOffs = - PartHeaderOffset.Data + PartHeaderOffset.ArgCount;
			origData.writeInt16(rows, argCountOffs);
			reqPacket.closePart(massExtent + extent * rows, rows);
		}
		
		public virtual bool hasRoomFor(int recordSize, int reserve)
		{
			return (argCount < maxArgCount && (Length - extent) > (recordSize + reserve));
		}
		
		public virtual bool hasRoomFor(int recordSize)
		{
			return (this.argCount < maxArgCount && (Length - extent) > recordSize);
		}
		
		public virtual void setFirstPart()
		{
			reqPacket.addPartAttr(PartAttributes.FirstPacket);
		}
		
		public virtual void setLastPart()
		{
			reqPacket.addPartAttr(PartAttributes.LastPacket);
		}
		
		public abstract void writeDefineByte(int value_Renamed, int offset);
		
		public virtual void markEmptyStream(ByteArray descMark)
		{
			descMark.writeByte(LongDesc.LastData, LongDesc.Valmode);
			descMark.writeInt32(massExtent + extent + 1, LongDesc.Valpos);
			descMark.writeInt32(0, LongDesc.Vallen);
		}
		
		public abstract bool fillWithOMSReader(StreamReader stream, int rowSize);
		
		public abstract bool fillWithProcedureReader(StreamReader reader, short rowCount);
		
		public abstract void addRow(short fieldCount);
		
		public abstract void writeNull(int pos, int len);
		
		public abstract ByteArray writeDescriptor(int pos, byte[] descriptor);
		
		public abstract void  fillWithOMSReturnCode(int returncode);
		
		public abstract bool fillWithOMSStream(Stream stream, bool asciiForUnicode);
		
		public abstract bool fillWithProcedureStream(Stream stream, short rowCount);
		
		public virtual bool fillWithStream(Stream stream, ByteArray descMark, PutValue putval)
		{
			// not exact, but enough to prevent an overflow - adding this
			// part to the packet may at most eat up 8 bytes more, if
			// the size is weird.
			int maxDataSize = MaxDataSize;
			
			if (maxDataSize <= 1)
			{
				descMark.writeByte(LongDesc.NoData, LongDesc.Valmode);
				return false;
			}
			int dataStart = this.extent;
			int bytesRead;
			byte[] readBuf = new byte[4096];
			bool streamExhausted = false;
			try
			{
				while (!streamExhausted && maxDataSize > 0)
				{
					bytesRead = stream.Read(readBuf, 0, Math.Min(maxDataSize, 4096));
					if (bytesRead > 0)
					{
						origData.writeBytes(readBuf, this.extent);
						extent += bytesRead;
						maxDataSize -= bytesRead;
					}
					else
						streamExhausted = true;
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException("Reading from a stream resulted in an IOException", ex);
			}
			/*
			* patch pos, length and kind
			*/
			if (streamExhausted)
				descMark.writeByte(LongDesc.LastData, LongDesc.Valmode);
			else
				descMark.writeByte(LongDesc.DataPart, LongDesc.Valmode);

			descMark.writeInt32(massExtent + dataStart + 1, LongDesc.Valpos);
			descMark.writeInt32(extent - dataStart, LongDesc.Vallen);
			putval.markRequestedChunk(origData.Clone(dataStart), extent - dataStart);
			return streamExhausted;
		}
		
		public virtual bool fillWithReader(StreamReader reader, ByteArray descMark, PutValue putval)
		{
			const int unicodeWidthC = 2;
			// not exact, but enough to prevent an overflow - adding this
			// part to the packet may at most eat up 8 bytes more, if
			// the size is weird.
			int maxDataSize = (origData.Length - extent - 8) / unicodeWidthC;
			if (maxDataSize <= 1)
			{
				descMark.writeByte(LongDesc.NoData, LongDesc.Valmode);
				return false;
			}
			
			int dataStart = extent;
			int charsRead;
			char[] readBuf = new char[4096];
			bool streamExhausted = false;
			try
			{
				while (!streamExhausted && maxDataSize > 0)
				{
					charsRead = reader.Read(readBuf, 0, Math.Min(maxDataSize, 4096));
					if (charsRead > 0)
					{
						origData.writeUnicode(new string(readBuf), extent);
						extent += charsRead * unicodeWidthC;
						maxDataSize -= charsRead;
					}
					else
						streamExhausted = true;
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException("Reading from a stream resulted in an IOException", ex);
			}
			/*
			* patch pos, length and kind
			*/
			if (streamExhausted)
				descMark.writeByte(LongDesc.LastData, LongDesc.Valmode);
			else
				descMark.writeByte(LongDesc.DataPart, LongDesc.Valmode);

			descMark.writeInt32(massExtent + dataStart + 1, LongDesc.Valpos);
			descMark.writeInt32(extent - dataStart, LongDesc.Vallen);
			putval.markRequestedChunk(origData.Clone(dataStart), extent - dataStart);
			return streamExhausted;
		}
	}

	#endregion

	#region "DataPartVariable class"

	public class DataPartVariable : DataPart 
	{
		private int fieldCount = 0;

		private int currentArgCount = 0;

		private int currentFieldCount = 0;

		private int currentFieldLen = 0;

		public DataPartVariable(ByteArray data, MaxDBRequestPacket reqPacket) : base(data, reqPacket) 
		{
		}

		public DataPartVariable(ByteArray data, short argCount) : base(data, null) 
		{
			this.argCount = argCount;
		}

		public bool nextRow() 
		{
			if (currentArgCount >= argCount) 
				return false;

			currentArgCount++;
			fieldCount = origData.readInt16(extent);
			extent += 2;
			currentFieldCount = 0;
			currentFieldLen = 0;
			return true;
		}

		public bool nextField() 
		{
			if (currentFieldCount >= fieldCount) 
				return false;

			currentFieldCount++;
			extent += currentFieldLen;
			currentFieldLen = readFieldLength(extent);
			extent += (currentFieldLen > 250) ? 3 : 1;
			return true;
		}

		public int CurrentFieldLen
		{
			get
			{
				return currentFieldLen;
			}
		}

		public int CurrentOffset
		{
			get
			{
				return extent;
			}
		}

		public override void addArg(int pos, int len) 
		{
			argCount++;
		}

		public override void addRow(short fieldCount) 
		{
			origData.writeInt16(fieldCount, extent);
			extent += 2;
		}

		public int readFieldLength(int offset) 
		{
			int erg = origData.readByte(offset);
			if (erg <= 250) 
				return erg;
			else 
				return origData.readInt16(offset + 1);
		}

		public int writeFieldLength(int val, int offset) 
		{
			if (val <= 250) 
			{
				origData.writeByte((byte)val, offset);
				return 1;
			} 
			else 
			{
				origData.writeByte(255, offset);
				origData.writeInt16((short)val, offset + 1);
				return 3;
			}
		}

		public override void writeDefineByte(int val, int offset) 
		{
			//vardata part has no define byte
			return;
		}

		public void writeUnicode(string val) 
		{
			int vallen = val.Length * 2;
			extent += writeFieldLength(vallen, extent);
			origData.writeUnicode(val, extent);
			extent += vallen;
		}

		public void writeASCII(string val) 
		{
			int vallen = val.Length;
			extent += writeFieldLength(vallen, extent);
			origData.writeASCII(val, extent);
			extent += vallen;
		}

		public void writeBytes(byte[] val, int offset, int len) 
		{
			extent += writeFieldLength(len, extent);
			origData.writeBytes(val, extent, len);
			extent += len;
		}

		public void writeBytes(byte[] val, int offset) 
		{
			writeBytes(val, offset, val.Length);
		}

		public void writeByte(byte val, int offset) 
		{
			int len = 1;
			extent += writeFieldLength(len, extent);
			origData.writeByte(val, extent);
			extent += len;
		}

		public void writeInt16(short val, int offset) 
		{
			int len = 2;
			extent += writeFieldLength(len, extent);
			origData.writeInt16(val, extent);
			extent += len;
		}

		public void writeInt32(int val, int offset) 
		{
			int len = 4;
			extent += writeFieldLength(len, extent);
			origData.writeInt32(val, extent);
			extent += len;
		}

		public void writeInt64(long val, int offset) 
		{
			int len = 8;
			extent += writeFieldLength(len, extent);
			origData.writeInt64(val, extent);
			extent += len;
		}
    
		public override void writeNull(int pos, int len) 
		{
			origData.writeByte(Packet.NullValue, extent);
			extent++; 
			addArg(pos, len);
		}

		public override ByteArray writeDescriptor (int pos, byte[] descriptor)
		{
			int offset = extent + 1;
			origData.writeBytes(descriptor, extent);
			return origData.Clone(offset);
		}

		public byte[] readBytes(int offset, int len)
		{
			return origData.readBytes(offset, len);
		}

		public byte readByte(int offset)
		{
			return origData.readByte(offset);
		}

		public short readInt16(int offset)
		{
			return origData.readInt16(offset);
		}

		public int readInt32(int offset)
		{
			return origData.readInt32(offset);
		}

		public long readInt64(int offset)
		{
			return origData.readInt64(offset);
		}

		public string readASCII(int offset, int len)
		{
			return origData.readASCII(offset, len);
		}

		public string readUnicode(int offset, int len)
		{
			return origData.readUnicode(offset, len);
		}

		public override bool fillWithOMSReader(StreamReader stream, int rowSize)
		{
			throw new NotImplementedException();
		}
		
		public override bool fillWithProcedureReader(StreamReader reader, short rowCount)
		{
			throw new NotImplementedException();
		}

		public override void  fillWithOMSReturnCode(int returncode)
		{
			throw new NotImplementedException();
		}
		
		public override bool fillWithOMSStream(Stream stream, bool asciiForUnicode)
		{
			throw new NotImplementedException();
		}
		
		public override bool fillWithProcedureStream(Stream stream, short rowCount)
		{
			throw new NotImplementedException();
		}
	}

	#endregion
}
