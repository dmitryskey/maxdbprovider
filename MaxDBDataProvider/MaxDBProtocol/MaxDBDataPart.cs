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
using MaxDB.Data.Utilities;

namespace MaxDB.Data.MaxDBProtocol
{
	#region "DataPart class"

	internal abstract class DataPart
	{
		private const int iMaxArgCount = Int16.MaxValue;
		protected short sArgCount;
		protected int iExtent;
		protected int iMassExtent;
		internal ByteArray baData;
		internal ByteArray baOrigData;

		internal MaxDBRequestPacket reqPacket;

		virtual protected internal int MaxDataSize
		{
			get
			{
				return baOrigData.Length - baOrigData.Offset - iExtent - 8;
			}
		}

		virtual public int Extent
		{
			get
			{
				return iExtent;
			}
		}

		public int Length
		{
			get
			{
				if (baOrigData != null)
					return baOrigData.Length;
				else
					return 0;
			}
		}

		internal DataPart(ByteArray data, MaxDBRequestPacket packet)
		{
			baData = data.Clone();
			baOrigData = data;
			reqPacket = packet;
		}

		public abstract void AddArg(int pos, int len);

		public virtual void Close()
		{
			baOrigData.WriteInt16(sArgCount, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
			reqPacket.ClosePart(iMassExtent + iExtent, sArgCount);
		}

		public virtual void CloseArrayPart(short rows)
		{
			baData.WriteInt16(rows, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
			reqPacket.ClosePart(iMassExtent + iExtent * rows, rows);
		}

		public virtual bool HasRoomFor(int recordSize, int reserve)
		{
			return (sArgCount < iMaxArgCount && (baOrigData.Length - baOrigData.Offset - iExtent) > (recordSize + reserve));
		}

		public virtual bool HasRoomFor(int recordSize)
		{
			return (sArgCount < iMaxArgCount && (baOrigData.Length - baOrigData.Offset - iExtent) > recordSize);
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
			baOrigData.Offset += iExtent;
			iMassExtent += iExtent;
			iExtent = 0;
		}

		public abstract void WriteDefineByte(byte value, int offset);

		public virtual void WriteByte(byte value, int offset)
		{
			baOrigData.WriteByte(value, offset);
		}

		public virtual void WriteBytes(byte[] value, int offset, int len)
		{
			baOrigData.WriteBytes(value, offset, len, Consts.ZeroBytes);
		}

		public virtual void WriteBytes(byte[] value, int offset)
		{
			baOrigData.WriteBytes(value, offset);
		}

		public void WriteAsciiBytes(byte[] value, int offset, int len)
		{
			baOrigData.WriteBytes(value, offset, len, Consts.BlankBytes);
		}

		public void WriteUnicodeBytes(byte[] value, int offset, int len)
		{
			baOrigData.WriteBytes(value, offset, len, Consts.BlankUnicodeBytes);
		}

		public virtual void WriteInt16(short value, int offset)
		{
			baOrigData.WriteInt16(value, offset);
		}

		public virtual void WriteInt32(int value, int offset)
		{
			baOrigData.WriteInt32(value, offset);
		}

		public virtual void WriteInt64(long value, int offset)
		{
			baOrigData.WriteInt64(value, offset);
		}

		public void WriteUnicode(string value, int offset, int len)
		{
			baOrigData.WriteUnicode(value, offset, len);
		}

		public byte[] ReadBytes(int offset, int len)
		{
			return baOrigData.ReadBytes(offset, len);
		}

		public string ReadAscii(int offset, int len)
		{
			return baOrigData.ReadAscii(offset, len);
		}

		public virtual void MarkEmptyStream(ByteArray descriptionMark)
		{
            if (descriptionMark == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "descriptionMark"));
            }

			descriptionMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
			descriptionMark.WriteInt32(iMassExtent + iExtent + 1, LongDesc.ValPos);
			descriptionMark.WriteInt32(0, LongDesc.ValLen);
		}

		public abstract bool FillWithProcedureReader(TextReader reader, short rowCount);

		public abstract void AddRow(short fieldCount);

		public abstract void WriteNull(int pos, int len);

		public abstract ByteArray WriteDescriptor(int pos, byte[] descriptor);

		public abstract bool FillWithOmsStream(Stream stream, bool asciiForUnicode);

		public abstract bool FillWithProcedureStream(Stream stream, short rowCount);

		public virtual bool FillWithStream(Stream stream, ByteArray descriptionMark, PutValue putValue)
		{
            if (stream == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "stream"));
            }

            if (descriptionMark == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "descriptionMark"));
            }

            if (putValue == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "putValue"));
            }

			// not exact, but enough to prevent an overflow - adding this
			// part to the packet may at most eat up 8 bytes more, if
			// the size is weird.
			int maxDataSize = MaxDataSize;

			if (maxDataSize <= 1)
			{
				descriptionMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
				return false;
			}

			int dataStart = iExtent;
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
                        baOrigData.WriteBytes(readBuf, iExtent, bytesRead);
                        iExtent += bytesRead;
                        maxDataSize -= bytesRead;
                    }
                    else
                    {
                        streamExhausted = true;
                    }
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ex.Message), ex);
			}

			// patch pos, length and kind
            if (streamExhausted)
            {
                descriptionMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
            }
            else
            {
                descriptionMark.WriteByte(LongDesc.DataPart, LongDesc.ValMode);
            }

			descriptionMark.WriteInt32(iMassExtent + dataStart + 1, LongDesc.ValPos);
			descriptionMark.WriteInt32(iExtent - dataStart, LongDesc.ValLen);
			putValue.MarkRequestedChunk(baOrigData.Clone(dataStart), iExtent - dataStart);
			return streamExhausted;
		}

		public virtual bool FillWithReader(TextReader reader, ByteArray descriptionMark, PutValue putValue)
		{
            if (reader == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "reader"));
            }

            if (descriptionMark == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "descriptionMark"));
            }

            if (putValue == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "putValue"));
            }

			// not exact, but enough to prevent an overflow - adding this
			// part to the packet may at most eat up 8 bytes more, if
			// the size is weird.
			int maxDataSize = (baOrigData.Length - baOrigData.Offset - iExtent - 8) / Consts.UnicodeWidth;
			if (maxDataSize <= 1)
			{
				descriptionMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
				return false;
			}

			int dataStart = iExtent;
			int charsRead;
			char[] readBuf = new char[4096];
			bool streamExhausted = false;
			try
			{
				while (!streamExhausted && maxDataSize > 0)
				{
					charsRead = reader.Read(readBuf, 0, Math.Min(maxDataSize, readBuf.Length));
					if (charsRead > 0)
					{
						baOrigData.WriteUnicode(new string(readBuf, 0, charsRead), iExtent);
						iExtent += charsRead * Consts.UnicodeWidth;
						maxDataSize -= charsRead;
					}
					else
						streamExhausted = true;
				}
			}
			catch (Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ex.Message), ex);
			}

			// patch pos, length and kind
            if (streamExhausted)
            {
                descriptionMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
            }
            else
            {
                descriptionMark.WriteByte(LongDesc.DataPart, LongDesc.ValMode);
            }

			descriptionMark.WriteInt32(iMassExtent + dataStart + 1, LongDesc.ValPos);
			descriptionMark.WriteInt32(iExtent - dataStart, LongDesc.ValLen);
			putValue.MarkRequestedChunk(baOrigData.Clone(dataStart), iExtent - dataStart);
			return streamExhausted;
		}
	}

	#endregion

	#region "Variable Data Part class"

	internal class DataPartVariable : DataPart
	{
		private int iFieldCount;
		private int iCurrentArgCount;
		private int iCurrentFieldCount;
		private int iCurrentFieldLen;

		public DataPartVariable(ByteArray data, MaxDBRequestPacket packet)
			: base(data, packet)
		{
		}

		public DataPartVariable(ByteArray data, short argCount)
			: base(data, null)
		{
			sArgCount = argCount;
		}

		public bool NextRow()
		{
			if (iCurrentArgCount >= sArgCount)
				return false;

			iCurrentArgCount++;
			iFieldCount = baOrigData.ReadInt16(iExtent);
			iExtent += 2;
			iCurrentFieldCount = 0;
			iCurrentFieldLen = 0;
			return true;
		}

		public bool NextField()
		{
			if (iCurrentFieldCount >= iFieldCount)
				return false;

			iCurrentFieldCount++;
			iExtent += iCurrentFieldLen;
			iCurrentFieldLen = ReadFieldLength(iExtent);
			iExtent += (iCurrentFieldLen > 250) ? 3 : 1;
			return true;
		}

		public int CurrentFieldLen
		{
			get
			{
				return iCurrentFieldLen;
			}
		}

		public int CurrentOffset
		{
			get
			{
				return iExtent;
			}
		}

		public override void AddArg(int pos, int len)
		{
			sArgCount++;
		}

		public override void AddRow(short fieldCount)
		{
			baOrigData.WriteInt16(fieldCount, iExtent);
			iExtent += 2;
		}

		public int ReadFieldLength(int offset)
		{
			int erg = baOrigData.ReadByte(offset);
			if (erg <= 250)
				return erg;
			else
				return baOrigData.ReadInt16(offset + 1);
		}

		public int WriteFieldLength(int value, int offset)
		{
			if (value <= 250)
			{
				baOrigData.WriteByte((byte)value, offset);
				return 1;
			}
			else
			{
				baOrigData.WriteByte(255, offset);
				baOrigData.WriteInt16((short)value, offset + 1);
				return 3;
			}
		}

		public override void WriteDefineByte(byte value, int offset)
		{
			//vardata part has no define byte
			return;
		}

		public override void WriteBytes(byte[] value, int offset, int len)
		{
			if (value == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

			iExtent += WriteFieldLength(len, iExtent);
			baOrigData.WriteBytes(value, iExtent, len);
			iExtent += len;
		}

		public override void WriteBytes(byte[] value, int offset)
		{
			if (value == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));

			WriteBytes(value, offset, value.Length);
		}

		public override void WriteByte(byte value, int offset)
		{
			int len = 1;
			iExtent += WriteFieldLength(len, iExtent);
			baOrigData.WriteByte(value, iExtent);
			iExtent += len;
		}

		public override void WriteInt16(short value, int offset)
		{
			int len = 2;
			iExtent += WriteFieldLength(len, iExtent);
			baOrigData.WriteInt16(value, iExtent);
			iExtent += len;
		}

		public override void WriteInt32(int value, int offset)
		{
			int len = 4;
			iExtent += WriteFieldLength(len, iExtent);
			baOrigData.WriteInt32(value, iExtent);
			iExtent += len;
		}

		public override void WriteInt64(long value, int offset)
		{
			int len = 8;
			iExtent += WriteFieldLength(len, iExtent);
			baOrigData.WriteInt64(value, iExtent);
			iExtent += len;
		}

		public override void WriteNull(int pos, int len)
		{
			baOrigData.WriteByte(Packet.NullValue, iExtent);
			iExtent++;
			AddArg(pos, len);
		}

		public override ByteArray WriteDescriptor(int pos, byte[] descriptor)
		{
			int offset = iExtent + 1;
			baOrigData.WriteBytes(descriptor, iExtent);
			return baOrigData.Clone(offset);
		}

		public override bool FillWithProcedureReader(TextReader reader, short rowCount)
		{
			throw new NotImplementedException();
		}

		public override bool FillWithOmsStream(Stream stream, bool asciiForUnicode)
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

	internal class DataPartFixed : DataPart
	{
		public DataPartFixed(ByteArray rawMemory, MaxDBRequestPacket requestPacket)
			: base(rawMemory, requestPacket)
		{
		}

		public override void WriteDefineByte(byte value, int offset)
		{
			WriteByte(value, offset);
		}

		public override void AddRow(short fieldCount)
		{
			// nothing to do with fixed Datapart
		}

		public override void AddArg(int pos, int len)
		{
			sArgCount++;
			iExtent = Math.Max(iExtent, pos + len);
		}

		public override void WriteNull(int pos, int len)
		{
			WriteByte((byte)255, pos - 1);
			WriteBytes(new byte[len], pos);
			AddArg(pos, len);
		}

		public override ByteArray WriteDescriptor(int pos, byte[] descriptor)
		{
			WriteDefineByte(0, pos++);
			WriteBytes(descriptor, pos);
			return baOrigData.Clone(pos);
		}

		public override bool FillWithProcedureReader(TextReader reader, short rowCount)
		{
			if (reader == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "reader"));

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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ioex.Message));
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

				WriteUnicode(new string(readBuf), iExtent, charsRead);
				iExtent += charsRead * Consts.UnicodeWidth;
				maxDataSize -= charsRead * Consts.UnicodeWidth;
				bytesWritten += charsRead * Consts.UnicodeWidth;
			}
			// The number of arguments is the number of rows
			sArgCount = (short)(bytesWritten / Consts.UnicodeWidth);
			// the data must be marked as 'last part' in case it is a last part.
			if (streamExhausted)
				SetLastPart();

			return streamExhausted;
		}

		public override bool FillWithOmsStream(Stream stream, bool asciiForUnicode)
		{
			if (stream == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "stream"));

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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ioex.Message));
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
					WriteBytes(expandbuf, iExtent, bytesRead * 2);
					iExtent += bytesRead * 2;
					maxDataSize -= bytesRead * 2;
					bytesWritten += bytesRead * 2;
				}
				else
				{
					WriteBytes(readBuf, iExtent, bytesRead);
					iExtent += bytesRead;
					maxDataSize -= bytesRead;
					bytesWritten += bytesRead;
				}
			}
			// The number of arguments is the number of rows
			sArgCount = (short)(bytesWritten / (asciiForUnicode ? 2 : 1));
			// the data must be marked as 'last part' in case it is a last part.
			if (streamExhausted)
				SetLastPart();

			return streamExhausted;
		}

		public override bool FillWithProcedureStream(Stream stream, short rowCount)
		{
			if (stream == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "stream"));

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
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_IOEXCEPTION, ioEx.Message));
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
				WriteBytes(readBuffer, iExtent, bytesRead);
				iExtent += bytesRead;
				maxDataSize -= bytesRead;
				bytesWritten += bytesRead;
			}

			sArgCount = (short)(bytesWritten / rowsize);

			if (streamExhausted)
				SetLastPart();

			return streamExhausted;
		}
	}

	#endregion
}
