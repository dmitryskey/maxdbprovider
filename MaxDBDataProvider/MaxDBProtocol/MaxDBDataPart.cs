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
				return origData.Length - extent - 8;
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

		private static readonly int maxArgCount = Int16.MaxValue;
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
		
		public abstract void AddArg(int pos, int len);
		
		public virtual void Close()
		{
			int argCountOffs = - PartHeaderOffset.Data + PartHeaderOffset.ArgCount;
			origData.writeInt16(argCount, argCountOffs);
			reqPacket.ClosePart(massExtent + extent, argCount);
		}
		
		public virtual void CloseArrayPart(short rows)
		{
			int argCountOffs = - PartHeaderOffset.Data + PartHeaderOffset.ArgCount;
			origData.writeInt16(rows, argCountOffs);
			reqPacket.ClosePart(massExtent + extent * rows, rows);
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
		
		public virtual void SetLastPart()
		{
			reqPacket.addPartAttr(PartAttributes.LastPacket);
		}
		
		public abstract void writeDefineByte(byte val, int offset);

		public virtual void writeByte(byte val, int offset) 
		{
			origData.writeByte(val, offset);
		}

		public virtual void writeBytes(byte[] val, int offset, int len)
		{
			origData.writeBytes(val, offset, len, Consts.zeroBytes);
		}

		public virtual void writeBytes(byte[] val, int offset)
		{
			origData.writeBytes(val, offset);
		}

		public void writeASCIIBytes(byte[] val, int offset, int len)
		{
			origData.writeBytes(val, offset, len, Consts.blankBytes);
		}

		public void writeUnicodeBytes(byte[] val, int offset, int len)
		{
			origData.writeBytes(val, offset, len, Consts.blankUnicodeBytes);
		}

		public virtual void writeInt16(short val, int offset) 
		{
			origData.writeInt16(val, offset);
		}

		public virtual void writeInt32(int val, int offset) 
		{
			origData.writeInt32(val, offset);
		}

		public virtual void writeInt64(long val, int offset) 
		{
			origData.writeInt64(val, offset);
		}

		public void writeUnicode(string val, int offset, int len)
		{
			origData.writeUnicode(val, offset, len);
		}

		public void writeASCII(string val, int offset, int len)
		{
			origData.writeASCII(val, offset, len);
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
		
		public virtual void markEmptyStream(ByteArray descMark)
		{
			descMark.writeByte(LongDesc.LastData, LongDesc.Valmode);
			descMark.writeInt32(massExtent + extent + 1, LongDesc.Valpos);
			descMark.writeInt32(0, LongDesc.Vallen);
		}
		
		public abstract bool FillWithOMSReader(TextReader reader, int rowSize);
		
		public abstract bool FillWithProcedureReader(TextReader reader, short rowCount);
		
		public abstract void AddRow(short fieldCount);
		
		public abstract void WriteNull(int pos, int len);
		
		public abstract ByteArray writeDescriptor(int pos, byte[] descriptor);
		
		public abstract void  FillWithOMSReturnCode(int returncode);
		
		public abstract bool FillWithOMSStream(Stream stream, bool asciiForUnicode);
		
		public abstract bool FillWithProcedureStream(Stream stream, short rowCount);
		
		public virtual bool FillWithStream(Stream stream, ByteArray descMark, PutValue putval)
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
		
		public virtual bool FillWithReader(TextReader reader, ByteArray descMark, PutValue putval)
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

	#region "Variable Data Part class"

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

		public override void AddArg(int pos, int len) 
		{
			argCount++;
		}

		public override void AddRow(short fieldCount) 
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

		public override void writeDefineByte(byte val, int offset) 
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

		public override void writeBytes(byte[] val, int offset, int len) 
		{
			extent += writeFieldLength(len, extent);
			origData.writeBytes(val, extent, len);
			extent += len;
		}

		public override void writeBytes(byte[] val, int offset) 
		{
			writeBytes(val, offset, val.Length);
		}

		public override void writeByte(byte val, int offset) 
		{
			int len = 1;
			extent += writeFieldLength(len, extent);
			origData.writeByte(val, extent);
			extent += len;
		}

		public override void writeInt16(short val, int offset) 
		{
			int len = 2;
			extent += writeFieldLength(len, extent);
			origData.writeInt16(val, extent);
			extent += len;
		}

		public override void writeInt32(int val, int offset) 
		{
			int len = 4;
			extent += writeFieldLength(len, extent);
			origData.writeInt32(val, extent);
			extent += len;
		}

		public override void writeInt64(long val, int offset) 
		{
			int len = 8;
			extent += writeFieldLength(len, extent);
			origData.writeInt64(val, extent);
			extent += len;
		}
    
		public override void WriteNull(int pos, int len) 
		{
			origData.writeByte(Packet.NullValue, extent);
			extent++; 
			AddArg(pos, len);
		}

		public override ByteArray writeDescriptor(int pos, byte[] descriptor)
		{
			int offset = extent + 1;
			origData.writeBytes(descriptor, extent);
			return origData.Clone(offset);
		}

		public override bool FillWithOMSReader(TextReader reader, int rowSize)
		{
			throw new NotImplementedException();
		}
		
		public override bool FillWithProcedureReader(TextReader reader, short rowCount)
		{
			throw new NotImplementedException();
		}

		public override void  FillWithOMSReturnCode(int returncode)
		{
			throw new NotImplementedException();
		}
		
		public override bool FillWithOMSStream(Stream stream, bool asciiForUnicode)
		{
			throw new NotImplementedException();
		}
		
		public override bool FillWithProcedureStream(Stream stream, short rowCount)
		{
			throw new NotImplementedException();
		}
	}

	#endregion

	#region "Fixed Data Part class"

	public class DataPartFixed : DataPart 
	{
		public DataPartFixed(ByteArray rawMem, MaxDBRequestPacket requestPacket) : base(rawMem, requestPacket)
		{
    }

    public override void writeDefineByte(byte val, int offset) 
	{
        writeByte(val, offset);
    }

    public override void AddRow(short fieldCount) 
	{
        // nothing to do with fixed Datapart
    }

    public override void AddArg(int pos, int len) 
	{
        argCount++;
        extent = Math.Max(extent, pos + len);
    }

    public override void WriteNull(int pos, int len) 
	{
        writeByte((byte)255, pos - 1);
        writeBytes(new byte[len], pos);
        AddArg(pos, len);
    }

    public void writeDefault(int pos, int len) 
	{
        writeByte(253, pos - 1);
        writeBytes(new byte[len], pos);
        AddArg(pos, len);
    }

    public override ByteArray writeDescriptor(int pos, byte[] descriptor) 
	{
        writeDefineByte(0, pos++);
        writeBytes(descriptor, pos);
        return origData.Clone(pos);
    }

    public override bool FillWithOMSReader(TextReader reader, int rowSize)
	{
        // We have to:
        // - read and write only multiples of 'rowSize' (which should be 2)
        // - but up to maxReadLength

        bool streamExhausted = false;
        int maxDataSize = (MaxDataSize / rowSize) * rowSize;

        int readBufSize = (4096 / rowSize) * rowSize;
        if (readBufSize == 0) 
            readBufSize = rowSize;
        
        char[] readBuf = new char[readBufSize];
        int charsRead = 0;
        int bytesWritten = 0;
        while (!streamExhausted && maxDataSize > 0) 
		{
            charsRead = 0;
            int startPos = 0;
            int charsToRead = Math.Min(maxDataSize / 2, readBufSize);
            int currCharsRead = 0;
            while (charsToRead != 0) 
			{
                try 
				{
                    currCharsRead = reader.Read(readBuf, startPos, charsToRead);
                } 
				catch (IOException ioex) 
				{
                    throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ioex.Message));
                }
                // if the stream is exhausted, we have to look whether it is wholly written.
                if (currCharsRead == -1) 
				{
                    if ((charsRead * 2) % rowSize != 0) 
                        throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_ODDSIZE));
                    else 
					{
                        charsToRead = 0;
                        streamExhausted = true;
                    }
                } 
				else 
				{
                    charsRead += currCharsRead;
                    // does it fit, then it is ok.
                    if (charsRead > 0 && (charsRead * 2) % rowSize == 0) 
                        charsToRead = 0;
                    else 
					{
                        // else advance in the buffer
                        charsToRead -= currCharsRead;
                        startPos += currCharsRead;
                    }
                }
            }

            writeUnicode(new string(readBuf), extent, charsRead * 2);
            extent += charsRead * 2;
            maxDataSize -= charsRead * 2;
            bytesWritten += charsRead * 2;
        }
        // The number of arguments is the number of rows
        argCount = (short)(bytesWritten / rowSize);
        // the data must be marked as 'last part' in case it is a last part.
        if (streamExhausted) 
            SetLastPart();
        
        return streamExhausted;
    }

    public override bool FillWithProcedureReader(TextReader reader, short rowCount)
    {
        bool streamExhausted = false;
        int maxDataSize = (MaxDataSize / 2) * 2;
    
        int readBufSize = (4096 / 2) * 2;
        if (readBufSize == 0) 
            readBufSize = 2;
        
        char[] readBuf = new char[readBufSize];
        int charsRead = 0;
        int bytesWritten = 0;
        while (!streamExhausted && maxDataSize > 0) 
		{
            charsRead = 0;
            int startPos = 0;
            int charsToRead = Math.Min(maxDataSize / 2, readBufSize);
            int currCharsRead = 0;
            while (charsToRead != 0) 
			{
                try 
				{
                    currCharsRead = reader.Read(readBuf, startPos, charsToRead);
                } 
				catch (IOException ioex) 
				{
                    throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ioex.Message));
                }
                // if the stream is exhausted, we have to look whether it is wholly written.
                if (currCharsRead == -1) 
				{
                    charsToRead = 0;
                    streamExhausted = true;
                } 
				else 
				{
                    charsRead += currCharsRead;
                    // does it fit, then it is ok.
                    if (charsRead > 0) 
                        charsToRead = 0;
                    else 
					{
                        // else advance in the buffer
                        charsToRead -= currCharsRead;
                        startPos += currCharsRead;
                    }
                }
            }
    
            writeUnicode(new string(readBuf), extent, charsRead * 2);
            this.extent += charsRead * 2;
            maxDataSize -= charsRead * 2;
            bytesWritten += charsRead * 2;
        }
        // The number of arguments is the number of rows
        argCount = (short)(bytesWritten / 2);
        // the data must be marked as 'last part' in case it is a last part.
        if (streamExhausted) 
            SetLastPart();
        
        return streamExhausted;
    }

    public override void FillWithOMSReturnCode(int returncode)
	{
        writeInt32(returncode, extent);
        extent += 4;
        argCount++;
    }

    public override bool FillWithOMSStream(Stream stream, bool asciiForUnicode)
    {
        // We have to:
        // - read and write only multiples of 'rowSize'
        // - but up to maxReadLength
    
        bool streamExhausted = false;
        int maxDataSize = MaxDataSize;
        int readBufSize = 4096;
        byte[] readBuf = new byte[readBufSize];
        byte[] expandbuf = null;
        if (asciiForUnicode) 
            expandbuf = new byte[readBufSize * 2];
        
        int bytesRead = 0;
        int bytesWritten = 0;
        while (!streamExhausted && maxDataSize > (asciiForUnicode ? 1 : 0)) 
		{
            bytesRead = 0;
            int startPos = 0;
            int bytesToRead = Math.Min(maxDataSize / (asciiForUnicode ? 2 : 1), readBufSize);
            int currBytesRead = 0;
            while (bytesToRead != 0) 
			{
                try 
				{
                    currBytesRead = stream.Read(readBuf, startPos, bytesToRead);
                } 
				catch (IOException ioex) 
				{
                    throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ioex.Message));
                }
                // if the stream is exhausted, we have to look
                // whether it is wholly written.
                if (currBytesRead == -1) 
				{
                    bytesToRead = 0;
                    streamExhausted = true;
                } 
				else 
				{
                    bytesRead += currBytesRead;
                    bytesToRead = 0;
                }
            }
            if (asciiForUnicode) 
			{
                for (int i = 0; i < bytesRead; ++i) 
				{
                    expandbuf[i * 2] = 0;
                    expandbuf[i * 2 + 1] = readBuf[i];
                }
                writeBytes(expandbuf, extent, bytesRead * 2);
                extent += bytesRead * 2;
                maxDataSize -= bytesRead * 2;
                bytesWritten += bytesRead * 2;
            } 
			else 
			{
                writeBytes(readBuf, extent, bytesRead);
                extent += bytesRead;
                maxDataSize -= bytesRead;
                bytesWritten += bytesRead;
            }
        }
        // The number of arguments is the number of rows
        argCount = (short)(bytesWritten / (asciiForUnicode ? 2 : 1));
        // the data must be marked as 'last part' in case it is a last part.
        if (streamExhausted) 
            SetLastPart();
        
        return streamExhausted;
    }

    public override bool FillWithProcedureStream(Stream stream, short rowCount)
    {
        bool streamExhausted = false;
        int maxDataSize = MaxDataSize;
        if (maxDataSize > short.MaxValue) 
            maxDataSize = short.MaxValue;
        
        int rowsize = 1;
        int bytesRead = 0;
        int bytesWritten = 0;
        int readBufferSize = 4096;
        byte[] readBuffer = new byte[4096];
        while (!streamExhausted & maxDataSize > rowsize) 
		{
            bytesRead = 0;
            int startPos = 0;
            int currBytesRead = 0;
            int bytesToRead = Math.Min(maxDataSize / rowsize, readBufferSize);
            while (bytesToRead != 0) 
			{
                try 
				{
                    currBytesRead = stream.Read(readBuffer, startPos, bytesToRead);
                } 
				catch (IOException ioEx) 
				{
                    throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ioEx.Message));
                }
                if (currBytesRead == -1) 
				{
                    streamExhausted = true;
                    bytesToRead = 0;
                } 
				else 
				{
                    bytesRead += currBytesRead;
                    bytesToRead = 0;
                }
            }
            writeBytes(readBuffer, extent, bytesRead);
            extent += bytesRead;
            maxDataSize -= bytesRead;
            bytesWritten += bytesRead;
        }
    
        this.argCount = (short)(bytesWritten / rowsize);
    
        if (streamExhausted) 
            SetLastPart();
    
        return streamExhausted;
    }  
}

#endregion
}
