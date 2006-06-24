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
using System.Net.Sockets;
using System.Text;
using MaxDB.Data.Utils;

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE

	#region "DataPart class"

	public abstract class DataPart
	{
        private static readonly int m_maxArgCount = Int16.MaxValue;
        protected internal short m_argCount = 0;
        protected internal int m_extent = 0;
        protected internal int m_massExtent = 0;
        internal ByteArray m_data, m_origData;

        internal MaxDBRequestPacket reqPacket;

		virtual protected internal int MaxDataSize
		{
			get
			{
				return m_origData.Length - m_origData.Offset - m_extent - 8;
			}
		}

		virtual public int Extent
		{
			get
			{
				return m_extent;
			}
		}

		public int Length
		{
			get
			{
				if (m_origData != null)
					return m_origData.Length;
				else
					return 0;
			}
		}

		internal DataPart(ByteArray data, MaxDBRequestPacket packet)
		{
            m_data = data.Clone();
			m_origData = data;
			reqPacket = packet;
		}
		
		public abstract void AddArg(int pos, int len);
		
		public virtual void Close()
		{
			m_origData.WriteInt16(m_argCount, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
			reqPacket.ClosePart(m_massExtent + m_extent, m_argCount);
		}
		
		public virtual void CloseArrayPart(short rows)
		{
			m_data.WriteInt16(rows, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
			reqPacket.ClosePart(m_massExtent + m_extent * rows, rows);
		}
		
		public virtual bool HasRoomFor(int recordSize, int reserve)
		{
            return (m_argCount < m_maxArgCount && (m_origData.Length - m_origData.Offset - m_extent) > (recordSize + reserve));
		}
		
		public virtual bool HasRoomFor(int recordSize)
		{
            return (m_argCount < m_maxArgCount && (m_origData.Length - m_origData.Offset - m_extent) > recordSize);
		}
		
		public virtual void SetFirstPart()
		{
			reqPacket.AddPartAttr(PartAttributes.FirstPacket);
		}
		
		public virtual void SetLastPart()
		{
			reqPacket.AddPartAttr(PartAttributes.LastPacket_Ext);
		}

        public void MoveRecordBase()
        {
            m_origData.Offset += m_extent;
            m_massExtent += m_extent;
            m_extent = 0;
        }
		
		public abstract void WriteDefineByte(byte val, int offset);

		public virtual void WriteByte(byte val, int offset) 
		{
			m_origData.WriteByte(val, offset);
		}

		public virtual void WriteBytes(byte[] val, int offset, int len)
		{
			m_origData.WriteBytes(val, offset, len, Consts.ZeroBytes);
		}

		public virtual void WriteBytes(byte[] val, int offset)
		{
			m_origData.WriteBytes(val, offset);
		}

		public void WriteASCIIBytes(byte[] val, int offset, int len)
		{
			m_origData.WriteBytes(val, offset, len, Consts.BlankBytes);
		}

		public void WriteUnicodeBytes(byte[] val, int offset, int len)
		{
			m_origData.WriteBytes(val, offset, len, Consts.BlankUnicodeBytes);
		}

		public virtual void WriteInt16(short val, int offset) 
		{
			m_origData.WriteInt16(val, offset);
		}

		public virtual void WriteInt32(int val, int offset) 
		{
			m_origData.WriteInt32(val, offset);
		}

		public virtual void WriteInt64(long val, int offset) 
		{
			m_origData.WriteInt64(val, offset);
		}

		public void WriteUnicode(string val, int offset, int len)
		{
			m_origData.WriteUnicode(val, offset, len);
		}

		public void WriteASCII(string val, int offset, int len)
		{
			m_origData.WriteASCII(val, offset, len);
		}

		public byte[] ReadBytes(int offset, int len)
		{
			return m_origData.ReadBytes(offset, len);
		}

		public byte ReadByte(int offset)
		{
			return m_origData.ReadByte(offset);
		}

		public short ReadInt16(int offset)
		{
			return m_origData.ReadInt16(offset);
		}

		public int ReadInt32(int offset)
		{
			return m_origData.ReadInt32(offset);
		}

		public long ReadInt64(int offset)
		{
			return m_origData.ReadInt64(offset);
		}

		public string ReadASCII(int offset, int len)
		{
			return m_origData.ReadASCII(offset, len);
		}

		public string ReadUnicode(int offset, int len)
		{
			return m_origData.ReadUnicode(offset, len);
		}
		
		public virtual void MarkEmptyStream(ByteArray descMark)
		{
			descMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
			descMark.WriteInt32(m_massExtent + m_extent + 1, LongDesc.ValPos);
			descMark.WriteInt32(0, LongDesc.ValLen);
		}
		
		public abstract bool FillWithProcedureReader(TextReader reader, short rowCount);
		
		public abstract void AddRow(short fieldCount);
		
		public abstract void WriteNull(int pos, int len);
		
		public abstract ByteArray WriteDescriptor(int pos, byte[] descriptor);
		
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
				descMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
				return false;
			}
			int dataStart = m_extent;
			int bytesRead;
			byte[] readBuf = new byte[4096];
			bool streamExhausted = false;
			try
			{
				while (!streamExhausted && maxDataSize > 0)
				{
					bytesRead = stream.Read(readBuf, 0, Math.Min(maxDataSize, readBuf.Length));
					if (bytesRead > 0)
					{
						m_origData.WriteBytes(readBuf, m_extent, bytesRead);
						m_extent += bytesRead;
						maxDataSize -= bytesRead;
					}
					else
						streamExhausted = true;
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ex.Message), ex);
			}

			// patch pos, length and kind
			if (streamExhausted)
				descMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
			else
				descMark.WriteByte(LongDesc.DataPart, LongDesc.ValMode);

			descMark.WriteInt32(m_massExtent + dataStart + 1, LongDesc.ValPos);
			descMark.WriteInt32(m_extent - dataStart, LongDesc.ValLen);
			putval.MarkRequestedChunk(m_origData.Clone(dataStart), m_extent - dataStart);
			return streamExhausted;
		}
		
		public virtual bool FillWithReader(TextReader reader, ByteArray descMark, PutValue putval)
		{
			// not exact, but enough to prevent an overflow - adding this
			// part to the packet may at most eat up 8 bytes more, if
			// the size is weird.
            int maxDataSize = (m_origData.Length - m_extent - 8) / Consts.UnicodeWidth;
			if (maxDataSize <= 1)
			{
				descMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
				return false;
			}
			
			int dataStart = m_extent;
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
						m_origData.WriteUnicode(new string(readBuf), m_extent);
						m_extent += charsRead * Consts.UnicodeWidth;
						maxDataSize -= charsRead;
					}
					else
						streamExhausted = true;
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ex.Message), ex);
			}
			
			
			// patch pos, length and kind
			if (streamExhausted)
				descMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
			else
				descMark.WriteByte(LongDesc.DataPart, LongDesc.ValMode);

			descMark.WriteInt32(m_massExtent + dataStart + 1, LongDesc.ValPos);
			descMark.WriteInt32(m_extent - dataStart, LongDesc.ValLen);
			putval.MarkRequestedChunk(m_origData.Clone(dataStart), m_extent - dataStart);
			return streamExhausted;
		}
	}

	#endregion

	#region "Variable Data Part class"

	public class DataPartVariable : DataPart 
	{
		private int m_fieldCount = 0;
		private int m_currentArgCount = 0;
		private int m_currentFieldCount = 0;
		private int m_currentFieldLen = 0;

		public DataPartVariable(ByteArray data, MaxDBRequestPacket reqPacket) : base(data, reqPacket) 
		{
		}

		public DataPartVariable(ByteArray data, short argCount) : base(data, null) 
		{
			m_argCount = argCount;
		}

		public bool NextRow() 
		{
			if (m_currentArgCount >= m_argCount) 
				return false;

			m_currentArgCount++;
			m_fieldCount = m_origData.ReadInt16(m_extent);
			m_extent += 2;
			m_currentFieldCount = 0;
			m_currentFieldLen = 0;
			return true;
		}

		public bool NextField() 
		{
			if (m_currentFieldCount >= m_fieldCount) 
				return false;

			m_currentFieldCount++;
			m_extent += m_currentFieldLen;
			m_currentFieldLen = ReadFieldLength(m_extent);
			m_extent += (m_currentFieldLen > 250) ? 3 : 1;
			return true;
		}

		public int CurrentFieldLen
		{
			get
			{
				return m_currentFieldLen;
			}
		}

		public int CurrentOffset
		{
			get
			{
				return m_extent;
			}
		}

		public override void AddArg(int pos, int len) 
		{
			m_argCount++;
		}

		public override void AddRow(short fieldCount) 
		{
			m_origData.WriteInt16(fieldCount, m_extent);
			m_extent += 2;
		}

		public int ReadFieldLength(int offset) 
		{
			int erg = m_origData.ReadByte(offset);
			if (erg <= 250) 
				return erg;
			else 
				return m_origData.ReadInt16(offset + 1);
		}

		public int WriteFieldLength(int val, int offset) 
		{
			if (val <= 250) 
			{
				m_origData.WriteByte((byte)val, offset);
				return 1;
			} 
			else 
			{
				m_origData.WriteByte(255, offset);
				m_origData.WriteInt16((short)val, offset + 1);
				return 3;
			}
		}

		public override void WriteDefineByte(byte val, int offset) 
		{
			//vardata part has no define byte
			return;
		}

		public void WriteUnicode(string val) 
		{
			int vallen = val.Length * 2;
			m_extent += WriteFieldLength(vallen, m_extent);
			m_origData.WriteUnicode(val, m_extent);
			m_extent += vallen;
		}

		public void WriteASCII(string val) 
		{
			int vallen = val.Length;
			m_extent += WriteFieldLength(vallen, m_extent);
			m_origData.WriteASCII(val, m_extent);
			m_extent += vallen;
		}

		public override void WriteBytes(byte[] val, int offset, int len) 
		{
			m_extent += WriteFieldLength(len, m_extent);
			m_origData.WriteBytes(val, m_extent, len);
			m_extent += len;
		}

		public override void WriteBytes(byte[] val, int offset) 
		{
			WriteBytes(val, offset, val.Length);
		}

		public override void WriteByte(byte val, int offset) 
		{
			int len = 1;
			m_extent += WriteFieldLength(len, m_extent);
			m_origData.WriteByte(val, m_extent);
			m_extent += len;
		}

		public override void WriteInt16(short val, int offset) 
		{
			int len = 2;
			m_extent += WriteFieldLength(len, m_extent);
			m_origData.WriteInt16(val, m_extent);
			m_extent += len;
		}

		public override void WriteInt32(int val, int offset) 
		{
			int len = 4;
			m_extent += WriteFieldLength(len, m_extent);
			m_origData.WriteInt32(val, m_extent);
			m_extent += len;
		}

		public override void WriteInt64(long val, int offset) 
		{
			int len = 8;
			m_extent += WriteFieldLength(len, m_extent);
			m_origData.WriteInt64(val, m_extent);
			m_extent += len;
		}
    
		public override void WriteNull(int pos, int len) 
		{
			m_origData.WriteByte(Packet.NullValue, m_extent);
			m_extent++; 
			AddArg(pos, len);
		}

		public override ByteArray WriteDescriptor(int pos, byte[] descriptor)
		{
			int offset = m_extent + 1;
			m_origData.WriteBytes(descriptor, m_extent);
			return m_origData.Clone(offset);
		}

		public override bool FillWithProcedureReader(TextReader reader, short rowCount)
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

		public override void WriteDefineByte(byte val, int offset) 
		{
			WriteByte(val, offset);
		}

		public override void AddRow(short fieldCount) 
		{
			// nothing to do with fixed Datapart
		}

		public override void AddArg(int pos, int len) 
		{
			m_argCount++;
			m_extent = Math.Max(m_extent, pos + len);
		}

		public override void WriteNull(int pos, int len) 
		{
			WriteByte((byte)255, pos - 1);
			WriteBytes(new byte[len], pos);
			AddArg(pos, len);
		}

		public void writeDefault(int pos, int len) 
		{
			WriteByte(253, pos - 1);
			WriteBytes(new byte[len], pos);
			AddArg(pos, len);
		}

		public override ByteArray WriteDescriptor(int pos, byte[] descriptor) 
		{
			WriteDefineByte(0, pos++);
			WriteBytes(descriptor, pos);
			return m_origData.Clone(pos);
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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ioex.Message));
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
    
				WriteUnicode(new string(readBuf), m_extent, charsRead);
				m_extent += charsRead * Consts.UnicodeWidth;
                maxDataSize -= charsRead * Consts.UnicodeWidth;
                bytesWritten += charsRead * Consts.UnicodeWidth;
			}
			// The number of arguments is the number of rows
            m_argCount = (short)(bytesWritten / Consts.UnicodeWidth);
			// the data must be marked as 'last part' in case it is a last part.
			if (streamExhausted) 
				SetLastPart();
        
			return streamExhausted;
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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ioex.Message));
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
					WriteBytes(expandbuf, m_extent, bytesRead * 2);
					m_extent += bytesRead * 2;
					maxDataSize -= bytesRead * 2;
					bytesWritten += bytesRead * 2;
				} 
				else 
				{
					WriteBytes(readBuf, m_extent, bytesRead);
					m_extent += bytesRead;
					maxDataSize -= bytesRead;
					bytesWritten += bytesRead;
				}
			}
			// The number of arguments is the number of rows
			m_argCount = (short)(bytesWritten / (asciiForUnicode ? 2 : 1));
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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_IOEXCEPTION, ioEx.Message));
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
				WriteBytes(readBuffer, m_extent, bytesRead);
				m_extent += bytesRead;
				maxDataSize -= bytesRead;
				bytesWritten += bytesRead;
			}
    
			m_argCount = (short)(bytesWritten / rowsize);
    
			if (streamExhausted) 
				SetLastPart();
    
			return streamExhausted;
		}  
	}

	#endregion

#endif // SAFE
}
