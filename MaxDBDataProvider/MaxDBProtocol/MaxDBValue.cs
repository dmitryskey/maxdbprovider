//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.IO;
using System.Data;
using System.Text;
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif // NET20
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE

	#region "Put Value class"

    internal class PutValue : IDisposable
	{
		private byte[] byDescription;
		protected ByteArray baDescriptionMark;
		private Stream mStream;
		//
		// the following is used to reread data to recover from a timeout
		//
		protected ByteArray baRequestData;
		protected int iRequestLength;
		private int iBufferPosition;

		public PutValue(Stream stream, int length, int position)
		{
			if (length >= 0)
				mStream = new FilteredStream(stream, length);
			else
				mStream = stream;
			iBufferPosition = position;
		}
    
		public PutValue(byte[] bytes, int bufferPosition)
		{
			mStream = new MemoryStream(bytes);
			iBufferPosition = bufferPosition;
		}

        protected PutValue(int bufferPosition)
        {
            iBufferPosition = bufferPosition;
        }

		public int BufferPosition
		{
			get
			{
				return iBufferPosition;
			}
		}

		public virtual bool AtEnd
		{
			get
			{
				return mStream == null;
			}
		}

		public void WriteDescriptor(DataPart memory, int pos)
		{
            if (memory == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "memory"));

			if (byDescription == null) 
				byDescription = new byte[LongDesc.Size];
        
			baDescriptionMark = memory.WriteDescriptor(pos, byDescription);
		}

        private static byte[] NewDescriptor
		{
			get
			{
				return new byte[LongDesc.Size];
			}
		}

		public void PutDescriptor(DataPart memory, int pos)
		{
            if (memory == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "memory"));

			if (byDescription == null) 
				byDescription = NewDescriptor;
			
			baDescriptionMark = memory.WriteDescriptor(pos, byDescription);
		}

		public void SetDescriptor(byte[] description)
		{
			byDescription = description;
		}

		public virtual void TransferStream(DataPart dataPart)
		{
            if (dataPart == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "dataPart"));

			if (AtEnd)
				dataPart.MarkEmptyStream(baDescriptionMark);
			else 
				if (dataPart.FillWithStream(mStream, baDescriptionMark, this)) 
				{
                    try
                    {
                        mStream.Close();
                    }
                    catch (ObjectDisposedException)
                    {
                        // ignore
                    }
                    finally
                    {
                        mStream = null;
                    }
				}
		}

		public void TransferStream(DataPart dataPart, short streamIndex)
		{
            if (dataPart == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "dataPart"));

			TransferStream(dataPart);
			baDescriptionMark.WriteInt16(streamIndex, LongDesc.ValInd);
		}

		public void MarkAsLast(DataPart dataPart)
		{
            if (dataPart == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "dataPart"));

			// avoid putting it in if this would break the aligned boundary.
			if(dataPart.Length - dataPart.Extent - 8 - LongDesc.Size - 1 < 0)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INDEX_OUTOFRANGE, dataPart.Length)); ;
        
			int descriptorPos = dataPart.Extent;
			WriteDescriptor(dataPart, descriptorPos);
			dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
			baDescriptionMark.WriteByte(LongDesc.LastPutval, LongDesc.ValMode);
		}

		public void MarkRequestedChunk(ByteArray data, int length)
		{
			baRequestData = data;
			iRequestLength = length;
		}

		public void MarkErrorStream() 
		{
			baDescriptionMark.WriteByte(LongDesc.Error, LongDesc.ValMode);
			baDescriptionMark.WriteInt32(0, LongDesc.ValPos);
			baDescriptionMark.WriteInt32(0, LongDesc.ValLen);
			try 
			{
				mStream.Close();
			}
			catch (IOException) 
			{
				// ignore
			}
			mStream = null;
		}

		public virtual void Reset()
		{
			if (baRequestData != null) 			
			{
				byte[] data = baRequestData.ReadBytes(0, iRequestLength);
				Stream firstChunk = new MemoryStream(data);
				if (mStream == null) 
					mStream = firstChunk;
				else 
					mStream = new JoinStream(new Stream[]{firstChunk, mStream});
			}
			baRequestData = null;
		}

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing && mStream != null)
				((IDisposable)mStream).Dispose();
        }

        #endregion
    }

	#endregion

	#region "Put Unicode Value class"

    internal class PutUnicodeValue : PutValue
	{
		private TextReader mReader;
    
		public PutUnicodeValue(TextReader readerp, int length, int bufpos) : base(bufpos)
		{
			if (length >= 0)
				mReader = new TextReaderFilter(readerp, length);
			else
				mReader = readerp;
		}
    
		public PutUnicodeValue(char[] source, int bufpos) : base(bufpos)
		{
			mReader = new StringReader(new string(source));
		}

		public override bool AtEnd
		{
			get
			{
				return mReader == null;
			}
		}

		public override void TransferStream(DataPart dataPart)
		{
			if (!AtEnd && dataPart.FillWithReader(mReader, baDescriptionMark, this))
			{
                try
                {
                    mReader.Close();
                }
                catch(ObjectDisposedException)
                {
                    // ignore
                }
                finally
                {
                    mReader = null;
                }
			}
		}

		public override void Reset()
		{
			if(baRequestData != null) 
			{
				StringReader firstChunk = new StringReader(baRequestData.ReadUnicode(0, iRequestLength));
				if(mReader == null) 
					mReader = firstChunk;
				else
					mReader = new JoinTextReader(new TextReader[] {firstChunk, mReader });
      
				baRequestData = null;
			}
		}
	}

	#endregion

	#region "Abstract Procedure Put Value class"

	internal abstract class AbstractProcedurePutValue 
	{
		private DBTechTranslator m_translator; 
		private ByteArray m_descriptor;
		private ByteArray m_descriptorMark;
	
		public AbstractProcedurePutValue(DBTechTranslator translator)
		{	    
			m_translator = translator;
			m_descriptor = new ByteArray(LongDesc.Size);
			m_descriptor.WriteByte(LongDesc.StateStream, LongDesc.State);		
		}

		public void UpdateIndex(int index)
		{
			m_descriptorMark.WriteInt16((short)index, LongDesc.ValInd);        
		}

		public void putDescriptor(DataPart memory)
		{
			memory.WriteDefineByte (0, m_translator.BufPos - 1);
			memory.WriteBytes(m_descriptor.GetArrayData(), m_translator.BufPos);
			m_descriptorMark = memory.baOrigData.Clone(m_translator.BufPos);       
		}

		public abstract void TransferStream(DataPart dataPart, short rowCount);
    
		public abstract void CloseStream();
	}

	#endregion

	#region "Basic Procedure Put Value class"

	internal abstract class BasicProcedurePutValue : AbstractProcedurePutValue 
	{
		protected Stream mStream;
 
		internal BasicProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator)
		{
			if (length == -1) 
				mStream = stream;
			else 
				mStream = new FilteredStream(stream, length);
		}

		public override void TransferStream(DataPart dataPart, short rowCount)
		{
			dataPart.FillWithProcedureStream(mStream, rowCount);
		}

		public override void CloseStream()
		{
			try 
			{
				mStream.Close();
			} 
			catch(Exception ex) 
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ex.Message));                   
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
		public BinaryProcedurePutValue(DBTechTranslator translator, Stream stream, int length) : base(translator, stream, length)
		{
		}
	}

	#endregion

	#region "Unicode Procedure Put Value class"

	internal class UnicodeProcedurePutValue : AbstractProcedurePutValue 
	{
		protected TextReader mReader;

		public UnicodeProcedurePutValue(DBTechTranslator translator, char[] buffer) :
			this(translator, new StringReader(new string(buffer)), -1)
		{
		}
 
		public UnicodeProcedurePutValue(DBTechTranslator translator, TextReader reader, int length) : base(translator)
		{
			if (length == -1) 
                mReader = reader;
			else 
                mReader = new TextReaderFilter(reader, length);
		}

		public override void TransferStream(DataPart dataPart, short rowCount)
		{
			dataPart.FillWithProcedureReader(mReader, rowCount);
		}

		public override void CloseStream()
		{
			try 
			{
				mReader.Close();
			} 
			catch(Exception ex) 
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ex.Message));                   
			}
		}
	}

	#endregion

	#region "Abstract Get Value class"

    internal abstract class AbstractGetValue
	{
		protected MaxDBConnection dbConnection;
		protected byte[] byDescriptor;
		internal ByteArray bsStreamBuffer;
		internal int iItemsInBuffer;
		protected int iItemSize;
		internal bool bAtEnd;
		internal long lLongPosition;

		public AbstractGetValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart, int itemSize)
		{
			dbConnection = connection;
			iItemSize = itemSize;
			SetupStreamBuffer(descriptor, dataPart);
		}

		internal bool NextChunk()
		{
			try 
			{
				int valMode = byDescriptor[LongDesc.ValMode];
				if (valMode == LongDesc.LastData || valMode == LongDesc.AllData) 
				{
					bAtEnd = true;
					return false;
				}
            
				MaxDBRequestPacket requestPacket = dbConnection.GetRequestPacket();
				MaxDBReplyPacket replyPacket;
				DataPart longpart = requestPacket.InitGetValue(dbConnection.AutoCommit);
				longpart.WriteByte(0, 0);
				longpart.WriteBytes(byDescriptor, 1);
				int maxval = int.MaxValue - 1;
				longpart.WriteInt32(maxval, 1 + LongDesc.ValLen);
				longpart.AddArg(1, LongDesc.Size);
				longpart.Close();
				try 
				{
					replyPacket = dbConnection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				}
				catch(MaxDBException ex) 
				{
					throw new IOException(ex.Message, ex);
				}

				replyPacket.FindPart(PartKind.LongData);
				int dataPos = replyPacket.PartDataPos;
				byDescriptor = replyPacket.ReadDataBytes(dataPos, LongDesc.Size + 1);
				if(byDescriptor[LongDesc.ValMode] == LongDesc.StartposInvalid) 
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALID_STARTPOSITION));
            
				SetupStreamBuffer(byDescriptor, replyPacket.Clone(dataPos));
				return true;
			}
			catch(PartNotFoundException) 
			{
				throw new IOException(MaxDBMessages.Extract(MaxDBError.LONGDATAEXPECTED));
			}
			catch(DataException ex) 
			{
				throw new StreamIOException(ex);
			}
		}

		private void SetupStreamBuffer(byte[] descriptor, ByteArray dataPart)
		{
			ByteArray desc = new ByteArray(descriptor);//??? swapMode? 
			int dataStart;

			dataStart = desc.ReadInt32(LongDesc.ValPos) - 1;
			iItemsInBuffer = desc.ReadInt32(LongDesc.ValLen) / iItemSize;
			bsStreamBuffer = dataPart.Clone(dataStart);
			this.byDescriptor = descriptor;
			if(descriptor[LongDesc.InternPos] == 0 && descriptor[LongDesc.InternPos + 1] == 0 &&
				descriptor[LongDesc.InternPos + 2] == 0 && descriptor[LongDesc.InternPos + 3] == 0) 
				descriptor[LongDesc.InternPos + 3] = 1;
		}

		public abstract Stream ASCIIStream{get;}

		public abstract Stream BinaryStream{get;}

		public abstract TextReader CharacterStream{get;}
	}

	#endregion

	#region "Get Value class"

	internal class GetValue : AbstractGetValue
	{
		public GetValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart) : base(connection, descriptor, dataPart, 1)
		{
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
					return m_value.bsStreamBuffer.Length;
				}
			}

			public override long Position
			{
				get
				{
					return m_value.bsStreamBuffer.Offset;
				}
				set
				{
					m_value.bsStreamBuffer.Offset = (int)value;
				}
			}

			public override int ReadByte()
			{
				int result;

				if (m_value.iItemsInBuffer <= 0)
					m_value.NextChunk();
            
				if (m_value.bAtEnd) 
					return -1;
            
				result = m_value.bsStreamBuffer.ReadByte(0);
				m_value.bsStreamBuffer.Offset++;
				m_value.iItemsInBuffer--;
				m_value.lLongPosition++;
				return result;
			}

			public override int Read(byte[] b, int off, int len)
			{
				int bytesCopied = 0;
				int chunkSize;
				byte[] chunk;

				while ((len > 0) && !m_value.bAtEnd) 
				{
					if (m_value.iItemsInBuffer <= 0) 
						m_value.NextChunk();
                
					if (!m_value.bAtEnd ) 
					{
						// copy bytes in buffer
						chunkSize = Math.Min(len, m_value.iItemsInBuffer);
						chunk = m_value.bsStreamBuffer.ReadBytes(0, chunkSize);
						Array.Copy(chunk, 0, b, off, chunkSize);
						len -= chunkSize;
						off += chunkSize;
						m_value.iItemsInBuffer -= chunkSize;
						m_value.bsStreamBuffer.Offset += chunkSize;
						bytesCopied += chunkSize;
					}
				}
				if ((bytesCopied == 0) && m_value.bAtEnd) 
					bytesCopied = -1;
				else 
					m_value.lLongPosition += bytesCopied;
            
				return bytesCopied;
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				switch(origin)
				{
					case SeekOrigin.Begin:
						m_value.bsStreamBuffer.Offset = (int)offset;
						break;
					case SeekOrigin.Current:
						m_value.bsStreamBuffer.Offset += (int)offset;
						break;
					case SeekOrigin.End:
						m_value.bsStreamBuffer.Offset = m_value.bsStreamBuffer.Length - (int)offset - 1;
						break;
				}
				return m_value.bsStreamBuffer.Offset;
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

	internal class GetLOBValue : GetValue
	{
		public GetLOBValue(MaxDBConnection connection, byte [] descriptor, ByteArray dataPart) : 
			base(connection, descriptor, dataPart)
		{
    	}
	}

	#endregion

	#region "Get Unicode Value class"

	internal class GetUnicodeValue : AbstractGetValue
	{
		public GetUnicodeValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart) :
			base(connection, descriptor, dataPart, 2)
		{
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

				while ((count > 0) && !m_value.bAtEnd) 
				{
					if (m_value.iItemsInBuffer <= 0) 
						m_value.NextChunk();

					if (!m_value.bAtEnd) 
					{
						chunkChars = Math.Min(count, m_value.iItemsInBuffer);
						chunkBytes = chunkChars * Consts.UnicodeWidth;
						chunk = m_value.bsStreamBuffer.ReadUnicode(0, chunkBytes).ToCharArray();
						Array.Copy(chunk, 0, b, offset, chunkChars);
						count -= chunkChars;
						offset += chunkChars;
						m_value.iItemsInBuffer -= chunkChars;
						m_value.bsStreamBuffer = m_value.bsStreamBuffer.Clone(chunkBytes);
						charsCopied += chunkChars;
					}
				}
				if ((charsCopied == 0) && m_value.bAtEnd) 
				{
					charsCopied = -1;
				}
				else 
				{
					m_value.lLongPosition += charsCopied;
				}
				return charsCopied;
			}
		}

		#endregion
		
	}
	#endregion

	#region "Get Unicode LOB Value class"

	internal class GetUnicodeLOBValue : GetUnicodeValue
	{
		public GetUnicodeLOBValue(MaxDBConnection connection, byte[] descriptor, ByteArray dataPart) :
			base(connection, descriptor, dataPart)
		{
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
	internal class FetchChunk
	{
		private MaxDBConnection dbConnection; //database connection
		private MaxDBReplyPacket mReplyPacket;	// The data packet from the fetch operation.
		private ByteArray baReplyData;	// The data part of replyPacket.
		private ByteArray baCurrentRecord;	// The current record inside the data part (replyData).
		private FetchType mType;	// type of fetch chunk
		private int iStartIndex;	// The index of the first row in this chunk.
		private int iEndIndex;	// The index of the last row in this chunk.
		private int iCurrentOffset;	//The current index within this chunk, starting with 0.
		private bool bFirst;	// A flag indicating that this chunk is the first chunk of the result set.
		private bool bLast;	// A flag indicating that this chunk is the last chunk of the result set.
		private int iRecordSize;	// The number of bytes in a row.
		private int iChunkSize;	// The number of elements in this chunk.
		private int iRowsInResultSet;	// The number of rows in the complete result set, or -1 if this is not known.

		public FetchChunk(MaxDBConnection conn, FetchType type, int absoluteStartRow, MaxDBReplyPacket replyPacket, int recordSize, int maxRows, int rowsInResultSet)
		{
			dbConnection = conn;
			mReplyPacket = replyPacket;
			mType = type;
			iRecordSize = recordSize;
			iRowsInResultSet = rowsInResultSet;
			try 
			{
				replyPacket.FirstSegment();
				replyPacket.FindPart(PartKind.Data);
			} 
			catch(PartNotFoundException) 
			{
				throw new DataException(MaxDBMessages.Extract(MaxDBError.FETCH_NODATAPART));
			}
			iChunkSize = replyPacket.PartArgsCount;
			int dataPos = replyPacket.PartDataPos;
			baReplyData = replyPacket.Clone(dataPos);
			baCurrentRecord = baReplyData.Clone(iCurrentOffset * iRecordSize);
			if (absoluteStartRow > 0) 
			{
				iStartIndex = absoluteStartRow;
				iEndIndex = absoluteStartRow + iChunkSize - 1;
			} 
			else 
			{
				if(rowsInResultSet != -1) 
				{
					if(absoluteStartRow < 0) 
						iStartIndex = rowsInResultSet + absoluteStartRow + 1; // - 1 is last
					else 
						iStartIndex = rowsInResultSet - absoluteStartRow + iChunkSize ;

					iEndIndex = iStartIndex + iChunkSize -1;
				} 
				else 
				{
					iStartIndex = absoluteStartRow;
					iEndIndex = absoluteStartRow + iChunkSize -1;
				}
			}
			DateTime dt = DateTime.Now;

			//>>> SQL TRACE
            dbConnection.mLogger.SqlTrace(dt, "FETCH BUFFER START: " + iStartIndex.ToString(CultureInfo.InvariantCulture));
            dbConnection.mLogger.SqlTrace(dt, "FETCH BUFFER END  : " + iEndIndex.ToString(CultureInfo.InvariantCulture));
			//<<< SQL TRACE
			
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
			if(mReplyPacket.WasLastPart) 
			{
				switch(mType) 
				{
					case FetchType.FIRST:
					case FetchType.LAST:
					case FetchType.RELATIVE_DOWN:
						bFirst = true;
						bLast = true;
						break;
					case FetchType.ABSOLUTE_UP:
					case FetchType.ABSOLUTE_DOWN:
					case FetchType.RELATIVE_UP:
						bLast = true;
						break;
				}
			}

			if(iStartIndex == 1) 
				bFirst = true;

			if(iEndIndex == -1) 
				bLast = true;

			// one special last for maxRows set
			if(maxRows != 0 && IsForward && iEndIndex >= maxRows) 
			{
				// if we have fetched too much, we have to cut here ...
				iEndIndex = maxRows;
				iChunkSize = maxRows + 1 - iStartIndex;
				bLast = true;
			}
		}

		// Gets the current record.
		public ByteArray CurrentRecord
		{
			get
			{
				return baCurrentRecord;
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
			if(iStartIndex <= row && iEndIndex >= row) 
				return true;

			// some tricks depending on whether we are on last/first chunk
			if(IsForward && bLast && row < 0) 
				return row >= iStartIndex - iEndIndex - 1;

			if(!IsForward && bFirst && row > 0) 
				return row <= iEndIndex - iStartIndex + 1;

			// if we know the number of rows, we can compute this anyway by inverting the row
			if(iRowsInResultSet != -1 && ((iStartIndex<0 && row > 0) || (iStartIndex > 0 && row < 0))) 
			{
				int inverted_row = (row > 0) ? (row - iRowsInResultSet - 1) : (row + iRowsInResultSet + 1);
				return iStartIndex <= inverted_row && iEndIndex >= inverted_row;
			}

			return false;
		}

		//Moves the position inside the chunk by a relative offset.
		public bool Move(int relativepos)
		{
			if(iCurrentOffset + relativepos < 0 || iCurrentOffset + relativepos >= iChunkSize )  
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
			iCurrentOffset += relativepos;
			baCurrentRecord = baCurrentRecord.Clone(relativepos * iRecordSize);
		}

		// Sets the current record to the supplied absolute position.
		public bool setRow(int row)
		{
			if(iStartIndex <= row && iEndIndex >= row) 
			{
				UnsafeMove(row - iStartIndex - iCurrentOffset);
				return true;
			}
			// some tricks depending on whether we are on last/first chunk
			if(IsForward && bLast && row < 0 && row >= iStartIndex - iEndIndex - 1 ) 
			{
				// move backward to the row from the end index, but
				// honor the row number start at 1, make this
				// relative to chunk by subtracting start index
				// and relative for the move by subtracting the
				// current offset
				UnsafeMove(iEndIndex + row + 1 - iStartIndex - iCurrentOffset);
				return true;
			}
			if(!IsForward && bFirst && row > 0 && row <= iEndIndex - iStartIndex + 1) 
			{
				// simple. row is > 0. m_startIndex if positive were 1 ...
				UnsafeMove(row - 1 - iCurrentOffset);
			}
			// if we know the number of rows, we can compute this anyway by inverting the row
			if(iRowsInResultSet != -1 && ((iStartIndex < 0 && row > 0) || (iStartIndex > 0 && row < 0))) 
			{
				int inverted_row = (row > 0) ? (row - iRowsInResultSet - 1) : (row + iRowsInResultSet + 1);
				return setRow(inverted_row);
			}

			return false;
		}

		// Get the reply data.
		public ByteArray ReplyData
		{
			get
			{
				return baReplyData;
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
				return bFirst;
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
				return bLast;
			}
			set
			{
				bLast = value;
			}
		}

		//	Gets the size of this chunk.
		public int Size
		{
			get
			{
				return iChunkSize;
			}
		}

		//	Retrieves the position where the internal position is after the fetch if this chunk is the current chunk.
		public int KernelPos
		{
			get
			{
				switch(mType) 
				{
					case FetchType.ABSOLUTE_DOWN:
					case FetchType.RELATIVE_UP:
					case FetchType.LAST:
						return iStartIndex;
					case FetchType.FIRST:
					case FetchType.ABSOLUTE_UP:
					case FetchType.RELATIVE_DOWN:
					default:
						return iEndIndex;
				}
			}
		}

		public bool IsForward
		{
			get
			{
				return (mType == FetchType.FIRST || mType == FetchType.ABSOLUTE_UP || mType == FetchType.RELATIVE_UP);
			}
		}

		//	Get the number of rows in the result set.
		public int RowsInResultSet
		{
			set
			{
				iRowsInResultSet = value;
			}
		}

		//	Gets the start index of the fetch chunk.
		public int Start
		{
			get
			{
				return iStartIndex;
			}
		}

		//	Gets the end index of the fetch chunk.
		public int End
		{
			get
			{
				return iEndIndex;
			}
		}
	}

	#endregion

	#region "Put Value class comparator"

    internal class PutValueComparator : 
#if NET20
    IComparer<PutValue>
#else
    IComparer
#endif
    {
#if NET20
        public int Compare(PutValue x, PutValue y)
#else
		public int Compare(object x, object y)
#endif
        {
			PutValue p1 = (PutValue)x;
			PutValue p2 = (PutValue)y;

			return p1.BufferPosition - p2.BufferPosition;		
		}
	}

	#endregion

#endif // SAFE
}
