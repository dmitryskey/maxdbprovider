using System;
using System.IO;
using System.Data;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Put Value class"

	public class PutValue
	{
		private byte[] desc;
		//
		// Set if the Putval instance was created from a byte array (to that byte array).
		//
		private byte[] sourceBytes;
    
		protected ByteArray descMark;
		private Stream stream;
		//
		// the following is used to reread data to recover from a timeout
		//
		protected ByteArray reqData = null;
		protected int reqLength;
		private int bufpos;

		public PutValue(Stream stream, int length, int bufpos)
		{
			if (length >= 0)
				stream = new StreamFilter(stream, length);
			else
				this.stream = stream;
			this.bufpos = bufpos;
		}
    
		public PutValue(byte[] bytes, int bufpos)
		{
			stream = new MemoryStream(bytes);
			sourceBytes = bytes;
			bufpos = bufpos;
		}
    
		protected PutValue(int bufpos)
		{
			bufpos = bufpos;
			stream = null;
		}

		public int BufPos
		{
			get
			{
				return bufpos;
			}
		}

		public virtual bool atEnd
		{
			get
			{
				return stream == null;
			}
		}

		public void writeDescriptor(DataPart mem, int pos)
		{
			if (desc == null) 
				desc = new byte[LongDesc.Size];
        
			descMark = mem.writeDescriptor(pos, desc);
		}

		private byte[] newDescriptor
		{
			get
			{
				return new byte [LongDesc.Size];
			}
		}

		public void putDescriptor(DataPart mem, int pos)
		{
			if (desc == null) 
				desc = newDescriptor;
			
			descMark = mem.writeDescriptor(pos, desc);
		}

		public void setDescriptor(byte[] desc)
		{
			this.desc = desc;
		}

		public virtual void transferStream(DataPart dataPart)
		{
			if (atEnd)
				dataPart.markEmptyStream (descMark);
			else 
				if (dataPart.FillWithStream(stream, descMark, this)) 
			{
				try 
				{
					stream.Close();
				}
				catch 
				{
					// ignore
				}
				stream = null;
			}
		}

		public void transferStream(DataPart dataPart, short streamIndex)
		{
			transferStream(dataPart);
			descMark.writeInt16(streamIndex, LongDesc.Valind);
		}

		public void markAsLast(DataPart dataPart)
		{
			// avoid putting it in if this would break the aligned boundary.
			if(dataPart.Length - dataPart.Extent - 8 - LongDesc.Size - 1 < 0) 
				throw new IndexOutOfRangeException();
        
			int descriptorPos = dataPart.Extent;
			writeDescriptor(dataPart, descriptorPos);
			dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
			descMark.WriteByte(LongDesc.LastPutval, LongDesc.Valmode);
		}

		public void markRequestedChunk(ByteArray reqData, int reqLength)
		{
			this.reqData = reqData;
			this.reqLength = reqLength;
		}

		public virtual void Reset()
		{
			if (reqData != null) 			
			{
				byte[] data = reqData.ReadBytes(0, reqLength);
				Stream firstChunk = new MemoryStream(data);
				if (stream == null) 
					stream = firstChunk;
				else 
					stream = new JoinStream (new Stream[]{firstChunk, stream});
			}
			reqData = null;
		}
	}

	#endregion

	#region "Put Unicode Value class"

	public class PutUnicodeValue : PutValue
	{
		private TextReader reader;
		private char[] sourceChars;
    
		public PutUnicodeValue(TextReader readerp, int length, int bufpos) : base(bufpos)
		{
			if (length >= 0)
				reader = new TextReaderFilter(readerp, length);
			else
				reader = readerp;
		}
    
		public PutUnicodeValue(char[] source, int bufpos) : base(bufpos)
		{
			reader = new StringReader(new string(source));
			sourceChars = source;	
		}

		public override bool atEnd
		{
			get
			{
				return reader == null;
			}
		}

		public override void transferStream(DataPart dataPart)
		{
			if (!atEnd && dataPart.FillWithReader(reader, descMark, this))
			{
				try 
				{
					reader.Close();
				}
				catch 
				{
					// ignore
				}
				reader = null;
			}
		}

		public override void Reset()
		{
			if(reqData != null) 
			{
				StringReader firstChunk = new StringReader(reqData.readUnicode(0, reqLength));
				if(reader == null) 
					reader = firstChunk;
				else
					reader = new JoinTextReader(new TextReader[] {firstChunk, reader });
      
				reqData = null;
			}
		}
	}

	#endregion

	#region "Abstract Procedure Put Value class"

	public abstract class AbstractProcedurePutValue 
	{
		private DBTechTranslator translator; 
		private ByteArray  descriptor;
		private ByteArray    descriptorMark;
	
		public AbstractProcedurePutValue(DBTechTranslator translator)
		{	    
			this.translator = translator;
			this.descriptor = new ByteArray(LongDesc.Size);
			this.descriptor.WriteByte(LongDesc.StateStream, LongDesc.State);		
		}

		public void updateIndex(int index)
		{
			this.descriptorMark.writeInt16((short)index, LongDesc.Valind);        
		}

		public void putDescriptor(DataPart memory)
		{
			memory.writeDefineByte (0, translator.BufPos - 1);
			memory.WriteBytes(descriptor.arrayData, translator.BufPos);
			descriptorMark = memory.origData.Clone(translator.BufPos);       
		}

		public abstract void TransferStream(DataPart dataPart, short rowCount);
    
		public abstract void CloseStream();
	}

	#endregion

	#region "Basic Procedure Put Value class"

	public abstract class BasicProcedurePutValue : AbstractProcedurePutValue 
	{
		protected Stream stream;
		protected int      length;
 
		public BasicProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator)
		{
			if (length == -1) 
			{
				this.stream = stream;
				this.length = int.MaxValue; 
			} 
			else 
			{
				this.stream = new StreamFilter(stream, length);
				this.length = length;
			}
		}

		public override void TransferStream(DataPart dataPart, short rowCount)
		{
			dataPart.FillWithProcedureStream(stream, rowCount);
		}

		public override void CloseStream()
		{
			try 
			{
				stream.Close();
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ex.Message));                   
			}
		}
	}

	#endregion

	#region "ASCII Procedure Put Value class"

	public class ASCIIProcedurePutValue : BasicProcedurePutValue
	{ 
		public ASCIIProcedurePutValue(DBTechTranslator translator, byte[] bytes): this(translator, new MemoryStream(bytes), -1)
		{
		}
    
		public ASCIIProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator, stream, length)
		{
		}
	}
	#endregion

	#region "Binary Procedure Put Value class"

	public class BinaryProcedurePutValue : BasicProcedurePutValue
	{
	
		public BinaryProcedurePutValue(DBTechTranslator translator, byte[] bytes):this(translator, new MemoryStream(bytes), -1)
		{
		}
	
		public BinaryProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator, stream, length)
		{
		}
	}

	#endregion

	#region "Unicode Procedure Put Value class"

	public class UnicodeProcedurePutValue : AbstractProcedurePutValue 
	{
		protected TextReader reader;
		protected int length;

		public UnicodeProcedurePutValue(DBTechTranslator translator, char[] buffer) :
			this(translator, new StringReader(new string(buffer)), -1)
		{
		}
 
		public UnicodeProcedurePutValue(DBTechTranslator translator, TextReader reader, int length) : base(translator)
		{
			if (length == -1) 
			{
				this.reader = reader;
				this.length = int.MaxValue; 
			} 
			else 
			{
				this.reader = new TextReaderFilter(reader, length);
				this.length = length;
			}
		}

		public override void TransferStream(DataPart dataPart, short rowCount)
		{
			dataPart.FillWithProcedureReader(reader, rowCount);
		}

		public override void CloseStream()
		{
			try 
			{
				reader.Close();
			} 
			catch(Exception ex) 
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_STREAM_IOEXCEPTION, ex.Message));                   
			}
		}
	}

	#endregion

	#region "Abstract Get Value class"

	public abstract class AbstractGetValue
	{
		protected MaxDBConnection connection;
		protected byte[] descriptor;
		internal ByteArray streamBuffer;
		internal int itemsInBuffer;
		protected int itemSize;
		internal bool atEnd;
		protected bool firstChunk = true;
		internal long longPosition = 0;
		protected long longSize = -1;

		public AbstractGetValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, int itemSize)
		{
			this.connection = connection;
			this.atEnd = false;
			this.itemSize = itemSize;
			this.SetupStreamBuffer(descriptor, dataPart);
		}

		internal bool NextChunk()
		{
			try 
			{
				int valMode = descriptor[LongDesc.Valmode];
				if (valMode == LongDesc.LastData || valMode == LongDesc.AllData) 
				{
					atEnd = true;
					return false;
				}
            
				firstChunk = false;
				MaxDBRequestPacket requestPacket = connection.CreateRequestPacket();
				MaxDBReplyPacket replyPacket;
				DataPart longpart = requestPacket.initGetValue(connection.AutoCommit);
				longpart.WriteByte(0, 0);
				longpart.WriteBytes(descriptor, 1);
				int maxval = int.MaxValue - 1;
				longpart.WriteInt32(maxval, 1 + LongDesc.Vallen);
				longpart.AddArg(1, LongDesc.Size);
				longpart.Close();
				try 
				{
					replyPacket = connection.Exec(requestPacket, this, GCMode.GC_DELAYED);
				}
				catch(MaxDBSQLException sqlEx) 
				{
					throw new IOException(sqlEx.Message, sqlEx);
				}

				replyPacket.findPart(PartKind.Longdata);
				int dataPos = replyPacket.PartDataPos;
				descriptor = replyPacket.GetDataBytes(dataPos, LongDesc.Size + 1);
				if(descriptor[LongDesc.Valmode] == LongDesc.StartposInvalid) 
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_INVALID_STARTPOSITION));
            
				SetupStreamBuffer(descriptor, replyPacket.Clone(dataPos));
				return true;
			}
			catch(PartNotFound) 
			{
				throw new IOException(MessageTranslator.Translate(MessageKey.ERROR_LONGDATAEXPECTED));
			}
			catch(DataException ex) 
			{
				throw new StreamIOException(ex);
			}
		}

		protected MaxDBReplyPacket ExecGetValue(byte[] descriptor)
		{
			MaxDBRequestPacket requestPacket = connection.CreateRequestPacket();
			DataPart longpart = requestPacket.initGetValue(connection.AutoCommit);
			longpart.WriteByte(0, 0);
			longpart.WriteBytes(descriptor, 1);
			longpart.WriteInt32(int.MaxValue - 1, 1 + LongDesc.Vallen);
			longpart.AddArg(1, LongDesc.Size);
			longpart.Close();
			return connection.Exec(requestPacket, this, GCMode.GC_DELAYED);
		}

		private void SetupStreamBuffer(byte[] descriptor, ByteArray dataPart)
		{
			ByteArray desc = new ByteArray(descriptor);//??? swapMode? 
			int dataStart;

			dataStart = desc.ReadInt32(LongDesc.Valpos) - 1;
			itemsInBuffer = desc.ReadInt32(LongDesc.Vallen) / itemSize;
			streamBuffer = dataPart.Clone(dataStart);
			this.descriptor = descriptor;
			if(descriptor[LongDesc.InternPos] == 0 && descriptor[LongDesc.InternPos + 1] == 0 &&
				descriptor[LongDesc.InternPos + 2] == 0 && descriptor[LongDesc.InternPos + 3] == 0) 
				descriptor[LongDesc.InternPos + 3] = 1;
		}

		public long LengthInBytes
		{
			get
			{
				if (longSize > -1) 
					return longSize;

				ByteArray desc = new ByteArray(descriptor);//??? swapMode
				longSize = desc.ReadInt32(LongDesc.MaxLen);
				if (longSize > 0)
					return longSize;

				longSize = LongSizeRequest;
				return longSize;
			}
		}

		// Send a request to get the length of a LONG value.
		// This is done by sending a getval where the valmode is set to DataTrunc
		protected long LongSizeRequest
		{
			get
			{
				byte[] requestDescriptor = new byte[descriptor.Length];
				byte[] resultDescriptor;
				MaxDBReplyPacket replyPacket;

				// copy descriptor
				Array.Copy(descriptor, 0, requestDescriptor, 0, descriptor.Length);
				requestDescriptor[LongDesc.Valmode] = LongDesc.DataTrunc;
				replyPacket = ExecGetValue(requestDescriptor);
				// get descriptor and read intern_pos
				try 
				{
					replyPacket.findPart(PartKind.Longdata);
				}
				catch(PartNotFound) 
				{
					throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_LONGDATAEXPECTED));
				}

				int dataPos = replyPacket.PartDataPos;
				resultDescriptor = replyPacket.GetDataBytes(dataPos, LongDesc.Size + 1);
				ByteArray descBytes = new ByteArray(resultDescriptor);

				// The result is the Pascal index of the append position, so 1 has to be subtracted
				return descBytes.ReadInt32(LongDesc.MaxLen);
			}
		}

		public abstract Stream ASCIIStream{get;}

		public abstract Stream BinaryStream{get;}

		public abstract TextReader CharacterStream{get;}

		public long Position(string searchstr, long start)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_POSITION_NOTIMPLEMENTED));
		}

		public int SetBytes(long pos, byte[] bytes)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETBYTES_NOTIMPLEMENTED));
		}

		public int SetBytes(long pos, byte[] bytes, int offset, int len)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETBYTES_NOTIMPLEMENTED));
		}

		public Stream SetBinaryStream(long pos)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETBINARYSTREAM_NOTIMPLEMENTED));
		}

		public void Truncate(long len)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_TRUNCATE_NOTIMPLEMENTED));
		}

		public int SetString(long pos, string str)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETSTRING_NOTIMPLEMENTED));
		}

		public int SetString(long pos, string str, int offset, int len)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETSTRING_NOTIMPLEMENTED));
		}

		public Stream SetASCIIStream(long pos)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETASCIISTREAM_NOTIMPLEMENTED));
		}

		public TextWriter SetCharacterStream(long pos)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_SETCHARACTERSTREAM_NOTIMPLEMENTED));
		}

		public long Position(char[] clob, long start)
		{
			throw new NotImplementedException(MessageTranslator.Translate(MessageKey.ERROR_POSITION_NOTIMPLEMENTED));
		}
	}

	#endregion

	#region "Get Value class"

	class GetValue : AbstractGetValue
	{
		bool isBinary = false;
		int asciiColumnAsUnicodeMultiplier = 1;
    
		public GetValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, int dataKind) : base(connection, descriptor, dataPart, 1)
		{
			isBinary = (dataKind == DataType.STRB) || (dataKind == DataType.LONGB);
			if ((dataKind == DataType.STRA || dataKind == DataType.LONGA) && connection.DatabaseEncoding == Encoding.Unicode) 
				asciiColumnAsUnicodeMultiplier = 2; 
		}

		public long Length
		{
			get
			{
				return (LengthInBytes * asciiColumnAsUnicodeMultiplier / itemSize);
			}
		}

		public override Stream ASCIIStream
		{
			get
			{
				return new GetValueStream(this);
			}
		}

		public override Stream BinaryStream
		{
			get
			{
				return new GetValueStream(this);
			}
		}

		public override TextReader CharacterStream
		{
			get
			{
				return new RawByteReader(ASCIIStream);
			}
		}

		#region "Get Value Stream class"

		class GetValueStream : Stream
		{
			GetValue m_value;

			public GetValueStream(GetValue val)
			{
				m_value = val;
			}

			public override bool CanRead
			{
				get
				{
					return true;
				}
			}

			public override bool CanSeek
			{
				get
				{
					return true;
				}
			}

			public override bool CanWrite
			{
				get
				{
					return false;
				}
			}

			public override void Flush()
			{
				throw new NotSupportedException();
			}


			public override long Length
			{
				get
				{
					return m_value.streamBuffer.Length;
				}
			}

			public override long Position
			{
				get
				{
					return m_value.streamBuffer.Offset;
				}
				set
				{
					m_value.streamBuffer.Offset = (int)value;
				}
			}

			public override int ReadByte()
			{
				int result;

				if (m_value.itemsInBuffer <= 0)
					m_value.NextChunk();
            
				if (m_value.atEnd) 
					return -1;
            
				result = m_value.streamBuffer.ReadByte(0);
				m_value.streamBuffer.Offset++;
				m_value.itemsInBuffer--;
				m_value.longPosition++;
				return result;
			}

			public override int Read(byte[] b, int off, int len)
			{
				int bytesCopied = 0;
				int chunkSize;
				byte[] chunk;

				while ((len > 0) && !m_value.atEnd) 
				{
					if (m_value.itemsInBuffer <= 0) 
						m_value.NextChunk();
                
					if (!m_value.atEnd ) 
					{
						// copy bytes in buffer
						chunkSize = Math.Min(len, m_value.itemsInBuffer);
						chunk = m_value.streamBuffer.ReadBytes(0, chunkSize);
						Array.Copy(chunk, 0, b, off, chunkSize);
						len -= chunkSize;
						off += chunkSize;
						m_value.itemsInBuffer -= chunkSize;
						m_value.streamBuffer.Offset += chunkSize;
						bytesCopied += chunkSize;
					}
				}
				if ((bytesCopied == 0) && m_value.atEnd) 
					bytesCopied = -1;
				else 
					m_value.longPosition += bytesCopied;
            
				return bytesCopied;
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				switch(origin)
				{
					case SeekOrigin.Begin:
						m_value.streamBuffer.Offset = (int)offset;
						break;
					case SeekOrigin.Current:
						m_value.streamBuffer.Offset += (int)offset;
						break;
					case SeekOrigin.End:
						m_value.streamBuffer.Offset = m_value.streamBuffer.Length - (int)offset - 1;
						break;
				}
				return m_value.streamBuffer.Offset;
			}

			public override void SetLength(long value)
			{
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException();
			}
		}


		#endregion
	}

	#endregion

	#region "Get LOB Value class"

	class GetLOBValue : GetValue
	{
		public GetLOBValue(MaxDBConnection connection, byte [] descriptor, ByteArray dataPart, int dataKind) : 
			base(connection, descriptor, dataPart, dataKind)
		{
		}

		public byte[] GetBytes(long pos, int length)
		{
			byte [] result;
			pos = pos - 1; // Lobs start at 1
			try 
			{
				if (pos < longPosition) 
					throw new NotSupportedException(MessageTranslator.Translate(MessageKey.ERROR_MOVEBACKWARDINBLOB));
            
				Stream stream = BinaryStream;
				if (pos > longPosition) 
					stream.Seek(pos - longPosition, SeekOrigin.Begin);
            
				result = new byte[length];
				int bytesRead = stream.Read (result, 0, length);
				if (bytesRead < length) 
				{
					byte[] tmp = new byte[bytesRead];
					Array.Copy(result, 0, tmp, 0, bytesRead);
					result = tmp;
				}
			}
			catch (StreamIOException sioExc) 
			{
				throw sioExc.SqlException;
			}
			catch (IOException ioExc) 
			{
				throw new DataException(ioExc.Message);
			}
			return result;
		}

		public string GetSubString(long pos, int length)
		{
			return Encoding.ASCII.GetString(GetBytes(pos, length));
		}
	}

	#endregion

	#region "Get Unicode Value class"

	public class GetUnicodeValue : AbstractGetValue
	{
		private bool isUnicodeColumn;
			
		public GetUnicodeValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, bool isUnicodeColumn) :
			base(connection, descriptor, dataPart, 2)
		{
			this.isUnicodeColumn = isUnicodeColumn;
		}

		public long Length
		{
			get
			{
				return (LengthInBytes / (isUnicodeColumn ? 2 : 1));
			}
		}

		public override Stream ASCIIStream
		{
			get
			{
				return new ReaderStream(CharacterStream, false);
			}
		}

		public override Stream BinaryStream
		{
			get
			{
				return new ReaderStream(CharacterStream, false);
			}
		}

		public override TextReader CharacterStream
		{
			get
			{
				return new GetUnicodeValueReader(this);
			}
		}

		#region "Get Unicode Value Reader class"

		public class GetUnicodeValueReader : TextReader
		{
			private GetUnicodeValue m_value;

			public GetUnicodeValueReader(GetUnicodeValue val)
			{
				m_value = val;
			}

			public override int Read(char[] b, int offset, int count)
			{
				int originalOffset = offset;
				int charsCopied = 0;
				int chunkChars;
				int chunkBytes;
				char[] chunk;

				while ((count > 0) && !m_value.atEnd) 
				{
					if (m_value.itemsInBuffer <= 0) 
						m_value.NextChunk();

					if (!m_value.atEnd) 
					{
						chunkChars = Math.Min(count, m_value.itemsInBuffer);
						chunkBytes = chunkChars * Consts.unicodeWidth;
						chunk = m_value.streamBuffer.readUnicode(0, chunkBytes).ToCharArray();
						Array.Copy(chunk, 0, b, offset, chunkChars);
						count -= chunkChars;
						offset += chunkChars;
						m_value.itemsInBuffer -= chunkChars;
						m_value.streamBuffer = m_value.streamBuffer.Clone(chunkBytes);
						charsCopied += chunkChars;
					}
				}
				if ((charsCopied == 0) && m_value.atEnd) 
				{
					charsCopied = -1;
				}
				else 
				{
					m_value.longPosition += charsCopied;
				}
				return charsCopied;
			}
		}

		#endregion
		
	}
	#endregion

	#region "Get Unicode LOB Value class"

	public class GetUnicodeLOBValue : GetUnicodeValue
	{
		public GetUnicodeLOBValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, bool isUnicodeColumn) :
			base(connection, descriptor, dataPart, isUnicodeColumn)
		{
		}

		public string GetSubString(long pos, int length)
		{
			char[] result;
			int charsRead = length;
			pos = pos - 1; // Lobs start at 1
			try 
			{
				if (pos < longPosition) 
					throw new NotSupportedException(MessageTranslator.Translate(MessageKey.ERROR_MOVEBACKWARDINBLOB));

				TextReader reader = CharacterStream;
				if (pos > longPosition)
				{
					char[] buffer = new char[pos - longPosition];
					reader.Read(buffer, 0, (int)(pos - longPosition));
				}
				
				result = new char[length];
				charsRead = reader.Read(result, 0, length);
			}
			catch (StreamIOException sioExc) 
			{
				throw sioExc.SqlException;
			}
			catch (IOException ioExc) 
			{
				throw new DataException (ioExc.Message);
			}
			return new string(result, 0, charsRead);
		}
	}

	#endregion
}
