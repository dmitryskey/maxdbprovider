using System;
using System.IO;
using System.Data;
using System.Text;
using System.Collections;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE
	#region "Put Value class"

	public class PutValue
	{
		private byte[] m_desc;
		protected ByteArray m_descMark;
		private Stream m_stream;
		//
		// the following is used to reread data to recover from a timeout
		//
		protected ByteArray m_reqData = null;
		protected int m_reqLength;
		private int m_bufpos;

		public PutValue(Stream stream, int length, int bufpos)
		{
			if (length >= 0)
				m_stream = new StreamFilter(stream, length);
			else
				m_stream = stream;
			m_bufpos = bufpos;
		}
    
		public PutValue(byte[] bytes, int bufpos)
		{
			m_stream = new MemoryStream(bytes);
			m_bufpos = bufpos;
		}
    
		protected PutValue(int bufpos)
		{
			m_bufpos = bufpos;
			m_stream = null;
		}

		public int BufPos
		{
			get
			{
				return m_bufpos;
			}
		}

		public virtual bool AtEnd
		{
			get
			{
				return m_stream == null;
			}
		}

		public void writeDescriptor(DataPart mem, int pos)
		{
			if (m_desc == null) 
				m_desc = new byte[LongDesc.Size];
        
			m_descMark = mem.WriteDescriptor(pos, m_desc);
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
			if (m_desc == null) 
				m_desc = newDescriptor;
			
			m_descMark = mem.WriteDescriptor(pos, m_desc);
		}

		public void setDescriptor(byte[] desc)
		{
			m_desc = desc;
		}

		public virtual void TransferStream(DataPart dataPart)
		{
			if (AtEnd)
				dataPart.MarkEmptyStream(m_descMark);
			else 
				if (dataPart.FillWithStream(m_stream, m_descMark, this)) 
			{
				try 
				{
					m_stream.Close();
				}
				catch 
				{
					// ignore
				}
				m_stream = null;
			}
		}

		public void TransferStream(DataPart dataPart, short streamIndex)
		{
			TransferStream(dataPart);
			m_descMark.WriteInt16(streamIndex, LongDesc.ValInd);
		}

		public void MarkAsLast(DataPart dataPart)
		{
			// avoid putting it in if this would break the aligned boundary.
			if(dataPart.Length - dataPart.Extent - 8 - LongDesc.Size - 1 < 0) 
				throw new IndexOutOfRangeException();
        
			int descriptorPos = dataPart.Extent;
			writeDescriptor(dataPart, descriptorPos);
			dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
			m_descMark.WriteByte(LongDesc.LastPutval, LongDesc.ValMode);
		}

		public void MarkRequestedChunk(ByteArray reqData, int reqLength)
		{
			m_reqData = reqData;
			m_reqLength = reqLength;
		}

		public void MarkErrorStream() 
		{
			m_descMark.WriteByte(LongDesc.Error, LongDesc.ValMode);
			m_descMark.WriteInt32(0, LongDesc.ValPos);
			m_descMark.WriteInt32(0, LongDesc.ValLen);
			try 
			{
				m_stream.Close();
			}
			catch (IOException) 
			{
				// ignore
			}
			m_stream = null;
		}

		public virtual void Reset()
		{
			if (m_reqData != null) 			
			{
				byte[] data = m_reqData.ReadBytes(0, m_reqLength);
				Stream firstChunk = new MemoryStream(data);
				if (m_stream == null) 
					m_stream = firstChunk;
				else 
					m_stream = new JoinStream (new Stream[]{firstChunk, m_stream});
			}
			m_reqData = null;
		}
	}

	#endregion

	#region "Put Unicode Value class"

	public class PutUnicodeValue : PutValue
	{
		private TextReader reader;
    
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
		}

		public override bool AtEnd
		{
			get
			{
				return reader == null;
			}
		}

		public override void TransferStream(DataPart dataPart)
		{
			if (!AtEnd && dataPart.FillWithReader(reader, m_descMark, this))
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
			if(m_reqData != null) 
			{
				StringReader firstChunk = new StringReader(m_reqData.ReadUnicode(0, m_reqLength));
				if(reader == null) 
					reader = firstChunk;
				else
					reader = new JoinTextReader(new TextReader[] {firstChunk, reader });
      
				m_reqData = null;
			}
		}
	}

	#endregion

	#region "Abstract Procedure Put Value class"

	internal abstract class AbstractProcedurePutValue 
	{
		private DBTechTranslator translator; 
		private ByteArray  descriptor;
		private ByteArray  descriptorMark;
	
		public AbstractProcedurePutValue(DBTechTranslator translator)
		{	    
			this.translator = translator;
			this.descriptor = new ByteArray(LongDesc.Size);
			this.descriptor.WriteByte(LongDesc.StateStream, LongDesc.State);		
		}

		public void UpdateIndex(int index)
		{
			this.descriptorMark.WriteInt16((short)index, LongDesc.ValInd);        
		}

		public void putDescriptor(DataPart memory)
		{
			memory.WriteDefineByte (0, translator.BufPos - 1);
			memory.WriteBytes(descriptor.arrayData, translator.BufPos);
			descriptorMark = memory.m_origData.Clone(translator.BufPos);       
		}

		public abstract void TransferStream(DataPart dataPart, short rowCount);
    
		public abstract void CloseStream();
	}

	#endregion

	#region "Basic Procedure Put Value class"

	internal abstract class BasicProcedurePutValue : AbstractProcedurePutValue 
	{
		protected Stream stream;
		protected int      length;
 
		internal BasicProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator)
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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ex.Message));                   
			}
		}
	}

	#endregion

	#region "ASCII Procedure Put Value class"

	internal class ASCIIProcedurePutValue : BasicProcedurePutValue
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

	internal class BinaryProcedurePutValue : BasicProcedurePutValue
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

	internal class UnicodeProcedurePutValue : AbstractProcedurePutValue 
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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ex.Message));                   
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
				int valMode = descriptor[LongDesc.ValMode];
				if (valMode == LongDesc.LastData || valMode == LongDesc.AllData) 
				{
					atEnd = true;
					return false;
				}
            
				firstChunk = false;
				MaxDBRequestPacket requestPacket = connection.GetRequestPacket();
				MaxDBReplyPacket replyPacket;
				DataPart longpart = requestPacket.InitGetValue(connection.AutoCommit);
				longpart.WriteByte(0, 0);
				longpart.WriteBytes(descriptor, 1);
				int maxval = int.MaxValue - 1;
				longpart.WriteInt32(maxval, 1 + LongDesc.ValLen);
				longpart.AddArg(1, LongDesc.Size);
				longpart.Close();
				try 
				{
					replyPacket = connection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				}
				catch(MaxDBSQLException sqlEx) 
				{
					throw new IOException(sqlEx.Message, sqlEx);
				}

				replyPacket.FindPart(PartKind.LongData);
				int dataPos = replyPacket.PartDataPos;
				descriptor = replyPacket.ReadDataBytes(dataPos, LongDesc.Size + 1);
				if(descriptor[LongDesc.ValMode] == LongDesc.StartposInvalid) 
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INVALID_STARTPOSITION));
            
				SetupStreamBuffer(descriptor, replyPacket.Clone(dataPos));
				return true;
			}
			catch(PartNotFound) 
			{
				throw new IOException(MaxDBMessages.Extract(MaxDBMessages.ERROR_LONGDATAEXPECTED));
			}
			catch(DataException ex) 
			{
				throw new StreamIOException(ex);
			}
		}

		internal MaxDBReplyPacket ExecGetValue(byte[] descriptor)
		{
			MaxDBRequestPacket requestPacket = connection.GetRequestPacket();
			DataPart longpart = requestPacket.InitGetValue(connection.AutoCommit);
			longpart.WriteByte(0, 0);
			longpart.WriteBytes(descriptor, 1);
			longpart.WriteInt32(int.MaxValue - 1, 1 + LongDesc.ValLen);
			longpart.AddArg(1, LongDesc.Size);
			longpart.Close();
			return connection.Execute(requestPacket, this, GCMode.GC_DELAYED);
		}

		private void SetupStreamBuffer(byte[] descriptor, ByteArray dataPart)
		{
			ByteArray desc = new ByteArray(descriptor);//??? swapMode? 
			int dataStart;

			dataStart = desc.ReadInt32(LongDesc.ValPos) - 1;
			itemsInBuffer = desc.ReadInt32(LongDesc.ValLen) / itemSize;
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
				requestDescriptor[LongDesc.ValMode] = LongDesc.DataTrunc;
				replyPacket = ExecGetValue(requestDescriptor);
				// get descriptor and read intern_pos
				try 
				{
					replyPacket.FindPart(PartKind.LongData);
				}
				catch(PartNotFound) 
				{
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_LONGDATAEXPECTED));
				}

				int dataPos = replyPacket.PartDataPos;
				resultDescriptor = replyPacket.ReadDataBytes(dataPos, LongDesc.Size + 1);
				ByteArray descBytes = new ByteArray(resultDescriptor);

				// The result is the Pascal index of the append position, so 1 has to be subtracted
				return descBytes.ReadInt32(LongDesc.MaxLen);
			}
		}

		public abstract Stream ASCIIStream{get;}

		public abstract Stream BinaryStream{get;}

		public abstract TextReader CharacterStream{get;}
	}

	#endregion

	#region "Get Value class"

	class GetValue : AbstractGetValue
	{
		int asciiColumnAsUnicodeMultiplier = 1;
    
		public GetValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, int dataKind) : base(connection, descriptor, dataPart, 1)
		{
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
					throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_MOVEBACKWARDINBLOB));
            
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
						chunkBytes = chunkChars * Consts.UnicodeWidth;
						chunk = m_value.streamBuffer.ReadUnicode(0, chunkBytes).ToCharArray();
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
					throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_MOVEBACKWARDINBLOB));

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

	#region "Fetch chunk class"

	/*
		The outcome of a particular fetch operation.  A fetch operation
		results in one (when the fetch size is 1) or more (when the fetch
		size is >1) data rows returned from the database server. Depending on
		the kind of the fetch, the positioning in the result at the database
		server and the start and end index computation does differ.
	*/
	class FetchChunk
	{
		private MaxDBReplyPacket m_replyPacket;	// The data packet from the fetch operation.
		private ByteArray m_replyData;	// The data part of replyPacket.
		private ByteArray m_currentRecord;	// The current record inside the data part (replyData).
		private FetchType m_type;	// type of fetch chunk
		private int m_startIndex;	// The index of the first row in this chunk.
		private int m_endIndex;	// The index of the last row in this chunk.
		private int m_currentOffset;	//The current index within this chunk, starting with 0.
		private bool m_first;	// A flag indicating that this chunk is the first chunk of the result set.
		private bool m_last;	// A flag indicating that this chunk is the last chunk of the result set.
		private int m_recordSize;	// The number of bytes in a row.
		private int m_chunkSize;	// The number of elements in this chunk.
		private int m_rowsInResultSet;	// The number of rows in the complete result set, or -1 if this is not known.

		public FetchChunk(FetchType type, int absoluteStartRow, MaxDBReplyPacket replyPacket, int recordSize, int maxRows, int rowsInResultSet)
		{
			m_replyPacket = replyPacket;
			m_type = type;
			m_recordSize = recordSize;
			m_rowsInResultSet = rowsInResultSet;
			try 
			{
				replyPacket.FirstSegment();
				replyPacket.FindPart(PartKind.Data);
			} 
			catch(PartNotFound) 
			{
				throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_FETCH_NODATAPART));
			}
			m_chunkSize = replyPacket.PartArgs;
			int dataPos=replyPacket.PartDataPos;
			m_replyData = replyPacket.Clone(dataPos);
			m_currentOffset=0;
			m_currentRecord = m_replyData.Clone(m_currentOffset * m_recordSize);
			if (absoluteStartRow > 0) 
			{
				m_startIndex = absoluteStartRow;
				m_endIndex = absoluteStartRow + m_chunkSize - 1;
			} 
			else 
			{
				if(rowsInResultSet != -1) 
				{
					if(absoluteStartRow < 0) 
						m_startIndex = rowsInResultSet + absoluteStartRow + 1; // - 1 is last
					else 
						m_startIndex = rowsInResultSet - absoluteStartRow + m_chunkSize ;

					m_endIndex = m_startIndex + m_chunkSize -1;
				} 
				else 
				{
					m_startIndex = absoluteStartRow;
					m_endIndex = absoluteStartRow + m_chunkSize -1;
				}
			}
			DetermineFlags(maxRows);
		}

		/*
			Determines whether this chunk is the first and/or last of
			a result set. This is done by checking the index boundaries,
			and also the LAST PART information of the reply packet.
			A forward chunk is also the last if it contains the record at
			the maxRows row, as the user decided to make
			the limit here.
		*/

		private void DetermineFlags(int maxRows)
		{
			if(m_replyPacket.WasLastPart) 
			{
				switch(m_type) 
				{
					case FetchType.FIRST:
					case FetchType.LAST:
					case FetchType.RELATIVE_DOWN:
						m_first = true;
						m_last = true;
						break;
					case FetchType.ABSOLUTE_UP:
					case FetchType.ABSOLUTE_DOWN:
					case FetchType.RELATIVE_UP:
						m_last = true;
						break;
				}
			}

			if(m_startIndex == 1) 
				m_first = true;

			if(m_endIndex == -1) 
				m_last = true;

			// one special last for maxRows set
			if(maxRows != 0 && IsForward && m_endIndex >= maxRows) 
			{
				// if we have fetched too much, we have to cut here ...
				m_endIndex = maxRows;
				m_chunkSize = maxRows + 1 - m_startIndex;
				m_last = true;
			}
		}
    
		// Gets the reply packet.
		public MaxDBReplyPacket ReplyPacket
		{
			get
			{
				return m_replyPacket;
			}
		}

		// Gets the current record.
		public ByteArray CurrentRecord
		{
			get
			{
				return m_currentRecord;
			}
		}

		/*
			Returns whether the given row is truly inside the chunk.
			@param row the row to check. Rows <0 count from the end of the result.
			@return true if the row is inside, false if it's not
			or the condition could not be determined due to an unknown end of result set.
		*/
		public bool ContainsRow(int row)
		{
			if(m_startIndex <= row && m_endIndex >= row) 
				return true;

			// some tricks depending on whether we are on last/first chunk
			if(IsForward && m_last && row < 0) 
				return row >= m_startIndex - m_endIndex - 1;

			if(!IsForward && m_first && row > 0) 
				return row <= m_endIndex - m_startIndex + 1;

			// if we know the number of rows, we can compute this anyway by inverting the row
			if(m_rowsInResultSet != -1 && ((m_startIndex<0 && row > 0) || (m_startIndex > 0 && row < 0))) 
			{
				int inverted_row = (row > 0) ? (row - m_rowsInResultSet - 1) : (row + m_rowsInResultSet + 1);
				return m_startIndex <= inverted_row && m_endIndex >= inverted_row;
			}

			return false;
		}

		//Moves the position inside the chunk by a relative offset.
		public bool Move(int relativepos)
		{
			if(m_currentOffset + relativepos < 0 || m_currentOffset + relativepos >= m_chunkSize )  
				return false;
			else 
			{
				UnsafeMove(relativepos);
				return true;
			}
		}

		//	Moves the position inside the chunk by a relative offset, but unchecked.
		private void UnsafeMove(int relativepos)
		{
			m_currentOffset += relativepos;
			m_currentRecord = m_currentRecord.Clone(relativepos * m_recordSize);
		}

		// Sets the current record to the supplied absolute position.
		public bool setRow(int row)
		{
			if(m_startIndex <= row && m_endIndex >= row) 
			{
				UnsafeMove(row - m_startIndex - m_currentOffset);
				return true;
			}
			// some tricks depending on whether we are on last/first chunk
			if(IsForward && m_last && row < 0 && row >= m_startIndex - m_endIndex - 1 ) 
			{
				// move backward to the row from the end index, but
				// honor the row number start at 1, make this
				// relative to chunk by subtracting start index
				// and relative for the move by subtracting the
				// current offset
				UnsafeMove(m_endIndex + row + 1 - m_startIndex - m_currentOffset);
				return true;
			}
			if(!IsForward && m_first && row > 0 && row <= m_endIndex - m_startIndex + 1) 
			{
				// simple. row is > 0. m_startIndex if positive were 1 ...
				UnsafeMove(row - 1 - m_currentOffset);
			}
			// if we know the number of rows, we can compute this anyway by inverting the row
			if(m_rowsInResultSet != -1 && ((m_startIndex < 0 && row > 0) || (m_startIndex > 0 && row < 0))) 
			{
				int inverted_row = (row > 0) ? (row - m_rowsInResultSet - 1) : (row + m_rowsInResultSet + 1);
				return setRow(inverted_row);
			}

			return false;
		}

		// Get the reply data.
		public ByteArray ReplyData
		{
			get
			{
				return m_replyData;
			}
		}

		/*
			 Returns whether this chunk is the first one or sets the first flag.
			 Take care, that this information may not be reliable.
			 @return true if this is the first, and false if this
			 is not first or the information is not known.
		*/
		public bool IsFirst
		{
			get
			{
				return m_first;
			}
			set
			{
				m_first = value;
			}
		}

		/*
			Returns whether this chunk is the last one or sets the last flag.
			Take care, that this information may not be reliable.
			@return true if this is the last, and false if this
			is not first or the information is not known.
		*/
		public bool IsLast
		{
			get
			{
				return m_last;
			}
			set
			{
				m_last = value;
			}
		}

		//	Gets the size of this chunk.
		public int Size
		{
			get
			{
				return m_chunkSize;
			}
		}

		//	Gets whether the current position is the first in the result set.
		public bool PositionedAtFirst
		{
			get
			{
				return m_first && m_currentOffset == 0;
			}
		}

		//	Gets whether the current position is the last in the result set.
		public bool PositionedAtLast
		{
			get
			{
				return m_last && m_currentOffset == m_chunkSize-1;
			}
		}

		//	Get the current position within the result set.
		public int LogicalPos
		{
			get
			{
				return m_startIndex + m_currentOffset;
			}
		}

		//	Gets the current offset in this chunk.
		public int Pos
		{
			get
			{
				return m_currentOffset;
			}
		}

		//	Retrieves the position where the internal position is after the fetch if this chunk is the current chunk.
		public int KernelPos
		{
			get
			{
				switch(m_type) 
				{
					case FetchType.ABSOLUTE_DOWN:
					case FetchType.RELATIVE_UP:
					case FetchType.LAST:
						return m_startIndex;
					case FetchType.FIRST:
					case FetchType.ABSOLUTE_UP:
					case FetchType.RELATIVE_DOWN:
					default:
						return m_endIndex;
				}
			}
		}

		public bool IsForward
		{
			get
			{
				return (m_type == FetchType.FIRST || m_type == FetchType.ABSOLUTE_UP || m_type == FetchType.RELATIVE_UP);
			}
		}

		//	Get the number of rows in the result set.
		public int RowsInResultSet
		{
			get
			{
				return m_rowsInResultSet;
			}
			set
			{
				m_rowsInResultSet = value;
			}
		}

		//	Gets the start index of the fetch chunk.
		public int Start
		{
			get
			{
				return m_startIndex;
			}
		}

		//	Gets the end index of the fetch chunk.
		public int End
		{
			get
			{
				return m_endIndex;
			}
		}
	}

	#endregion

	#region "Put Value class comparator"

	public class PutValueComparator : IComparer
	{
		int IComparer.Compare(object x, object y)
		{
			PutValue p1 = (PutValue)x;
			PutValue p2 = (PutValue)y;

			int p1_bufpos = p1.BufPos;
			int p2_bufpos = p2.BufPos;

			return p1_bufpos - p2_bufpos;		
		}
	}

	#endregion
#endif
}
