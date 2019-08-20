// Copyright © 2005-2018 Dmitry S. Kataev
// Copyright © 2002-2003 SAP AG
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.MaxDBProtocol
{
    using System;
    using System.IO;
    using MaxDB.Data.Utilities;

    #region "DataPart class"

    internal abstract class DataPart
    {
        private const int iMaxArgCount = short.MaxValue;
        protected short sArgCount;
        protected int iExtent;
        protected int iMassExtent;
        internal ByteArray baData;
        internal ByteArray baOrigData;

        internal MaxDBRequestPacket reqPacket;

        virtual protected internal int MaxDataSize => this.baOrigData.Length - this.baOrigData.Offset - this.iExtent - 8;

        virtual public int Extent => this.iExtent;

        public int Length => this.baOrigData != null ? this.baOrigData.Length : 0;

        internal DataPart(ByteArray data, MaxDBRequestPacket packet)
        {
            this.baData = data.Clone();
            this.baOrigData = data;
            this.reqPacket = packet;
        }

        public abstract void AddArg(int pos, int len);

        public virtual void Close()
        {
            this.baOrigData.WriteInt16(this.sArgCount, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
            this.reqPacket.ClosePart(this.iMassExtent + this.iExtent, this.sArgCount);
        }

        public virtual void CloseArrayPart(short rows)
        {
            this.baData.WriteInt16(rows, -PartHeaderOffset.Data + PartHeaderOffset.ArgCount);
            this.reqPacket.ClosePart(this.iMassExtent + this.iExtent * rows, rows);
        }

        public virtual bool HasRoomFor(int recordSize, int reserve)
        {
            return (this.sArgCount < iMaxArgCount && (this.baOrigData.Length - this.baOrigData.Offset - this.iExtent) > (recordSize + reserve));
        }

        public virtual bool HasRoomFor(int recordSize) => this.sArgCount < iMaxArgCount && (this.baOrigData.Length - this.baOrigData.Offset - this.iExtent) > recordSize;

        public virtual void SetFirstPart() => this.reqPacket.AddPartAttr(PartAttributes.FirstPacket);

        public virtual void SetLastPart() => this.reqPacket.AddPartAttr(PartAttributes.LastPacket_Ext);

        public void MoveRecordBase()
        {
            this.baOrigData.Offset += this.iExtent;
            this.iMassExtent += this.iExtent;
            this.iExtent = 0;
        }

        public abstract void WriteDefineByte(byte value, int offset);

        public virtual void WriteByte(byte value, int offset) => this.baOrigData.WriteByte(value, offset);

        public virtual void WriteBytes(byte[] value, int offset, int len) => this.baOrigData.WriteBytes(value, offset, len, Consts.ZeroBytes);

        public virtual void WriteBytes(byte[] value, int offset) => this.baOrigData.WriteBytes(value, offset);

        public void WriteAsciiBytes(byte[] value, int offset, int len) => this.baOrigData.WriteBytes(value, offset, len, Consts.BlankBytes);

        public void WriteUnicodeBytes(byte[] value, int offset, int len) => this.baOrigData.WriteBytes(value, offset, len, Consts.BlankUnicodeBytes);

        public virtual void WriteInt16(short value, int offset) => this.baOrigData.WriteInt16(value, offset);

        public virtual void WriteInt32(int value, int offset) => this.baOrigData.WriteInt32(value, offset);

        public virtual void WriteInt64(long value, int offset) => this.baOrigData.WriteInt64(value, offset);

        public void WriteUnicode(string value, int offset, int len) => this.baOrigData.WriteUnicode(value, offset, len);

        public byte[] ReadBytes(int offset, int len) => this.baOrigData.ReadBytes(offset, len);

        public string ReadAscii(int offset, int len) => this.baOrigData.ReadAscii(offset, len);

        public virtual void MarkEmptyStream(ByteArray descriptionMark)
        {
            if (descriptionMark == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "descriptionMark"));
            }

            descriptionMark.WriteByte(LongDesc.LastData, LongDesc.ValMode);
            descriptionMark.WriteInt32(this.iMassExtent + this.iExtent + 1, LongDesc.ValPos);
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
            int maxDataSize = this.MaxDataSize;

            if (maxDataSize <= 1)
            {
                descriptionMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
                return false;
            }

            int dataStart = this.iExtent;
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
                        this.baOrigData.WriteBytes(readBuf, this.iExtent, bytesRead);
                        this.iExtent += bytesRead;
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

            descriptionMark.WriteInt32(this.iMassExtent + dataStart + 1, LongDesc.ValPos);
            descriptionMark.WriteInt32(this.iExtent - dataStart, LongDesc.ValLen);
            putValue.MarkRequestedChunk(this.baOrigData.Clone(dataStart), this.iExtent - dataStart);
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
            int maxDataSize = (this.baOrigData.Length - this.baOrigData.Offset - this.iExtent - 8) / Consts.UnicodeWidth;
            if (maxDataSize <= 1)
            {
                descriptionMark.WriteByte(LongDesc.NoData, LongDesc.ValMode);
                return false;
            }

            int dataStart = this.iExtent;
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
                        this.baOrigData.WriteUnicode(new string(readBuf, 0, charsRead), this.iExtent);
                        this.iExtent += charsRead * Consts.UnicodeWidth;
                        maxDataSize -= charsRead;
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

            descriptionMark.WriteInt32(this.iMassExtent + dataStart + 1, LongDesc.ValPos);
            descriptionMark.WriteInt32(this.iExtent - dataStart, LongDesc.ValLen);
            putValue.MarkRequestedChunk(this.baOrigData.Clone(dataStart), this.iExtent - dataStart);
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

        public DataPartVariable(ByteArray data, MaxDBRequestPacket packet)
            : base(data, packet)
        {
        }

        public DataPartVariable(ByteArray data, short argCount)
            : base(data, null) => this.sArgCount = argCount;

        public bool NextRow()
        {
            if (this.iCurrentArgCount >= this.sArgCount)
            {
                return false;
            }

            this.iCurrentArgCount++;
            this.iFieldCount = this.baOrigData.ReadInt16(this.iExtent);
            this.iExtent += 2;
            this.iCurrentFieldCount = 0;
            this.CurrentFieldLen = 0;
            return true;
        }

        public bool NextField()
        {
            if (this.iCurrentFieldCount >= this.iFieldCount)
            {
                return false;
            }

            this.iCurrentFieldCount++;
            this.iExtent += this.CurrentFieldLen;
            this.CurrentFieldLen = this.ReadFieldLength(this.iExtent);
            this.iExtent += (this.CurrentFieldLen > 250) ? 3 : 1;
            return true;
        }

        public int CurrentFieldLen { get; private set; }

        public int CurrentOffset => this.iExtent;

        public override void AddArg(int pos, int len) => this.sArgCount++;

        public override void AddRow(short fieldCount)
        {
            this.baOrigData.WriteInt16(fieldCount, this.iExtent);
            this.iExtent += 2;
        }

        public int ReadFieldLength(int offset)
        {
            int erg = this.baOrigData.ReadByte(offset);
            return erg <= 250 ? erg : this.baOrigData.ReadInt16(offset + 1);
        }

        public int WriteFieldLength(int value, int offset)
        {
            if (value <= 250)
            {
                this.baOrigData.WriteByte((byte)value, offset);
                return 1;
            }
            else
            {
                this.baOrigData.WriteByte(255, offset);
                this.baOrigData.WriteInt16((short)value, offset + 1);
                return 3;
            }
        }

        public override void WriteDefineByte(byte value, int offset)
        {
            // vardata part has no define byte
            return;
        }

        public override void WriteBytes(byte[] value, int offset, int len)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            this.iExtent += this.WriteFieldLength(len, this.iExtent);
            this.baOrigData.WriteBytes(value, this.iExtent, len);
            this.iExtent += len;
        }

        public override void WriteBytes(byte[] value, int offset)
        {
            if (value == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "value"));
            }

            this.WriteBytes(value, offset, value.Length);
        }

        public override void WriteByte(byte value, int offset)
        {
            int len = 1;
            this.iExtent += this.WriteFieldLength(len, this.iExtent);
            this.baOrigData.WriteByte(value, this.iExtent);
            this.iExtent += len;
        }

        public override void WriteInt16(short value, int offset)
        {
            int len = 2;
            this.iExtent += this.WriteFieldLength(len, this.iExtent);
            this.baOrigData.WriteInt16(value, this.iExtent);
            this.iExtent += len;
        }

        public override void WriteInt32(int value, int offset)
        {
            int len = 4;
            this.iExtent += this.WriteFieldLength(len, this.iExtent);
            this.baOrigData.WriteInt32(value, this.iExtent);
            this.iExtent += len;
        }

        public override void WriteInt64(long value, int offset)
        {
            int len = 8;
            this.iExtent += this.WriteFieldLength(len, this.iExtent);
            this.baOrigData.WriteInt64(value, this.iExtent);
            this.iExtent += len;
        }

        public override void WriteNull(int pos, int len)
        {
            this.baOrigData.WriteByte(Packet.NullValue, this.iExtent);
            this.iExtent++;
            this.AddArg(pos, len);
        }

        public override ByteArray WriteDescriptor(int pos, byte[] descriptor)
        {
            int offset = this.iExtent + 1;
            this.baOrigData.WriteBytes(descriptor, this.iExtent);
            return this.baOrigData.Clone(offset);
        }

        public override bool FillWithProcedureReader(TextReader reader, short rowCount) => throw new NotImplementedException();

        public override bool FillWithOmsStream(Stream stream, bool asciiForUnicode) => throw new NotImplementedException();

        public override bool FillWithProcedureStream(Stream stream, short rowCount) => throw new NotImplementedException();
    }

    #endregion

    #region "Fixed Data Part class"

    internal class DataPartFixed : DataPart
    {
        public DataPartFixed(ByteArray rawMemory, MaxDBRequestPacket requestPacket)
            : base(rawMemory, requestPacket)
        {
        }

        public override void WriteDefineByte(byte value, int offset) => this.WriteByte(value, offset);

        public override void AddRow(short fieldCount)
        {
            // nothing to do with fixed Datapart
        }

        public override void AddArg(int pos, int len)
        {
            this.sArgCount++;
            this.iExtent = Math.Max(this.iExtent, pos + len);
        }

        public override void WriteNull(int pos, int len)
        {
            this.WriteByte((byte)255, pos - 1);
            this.WriteBytes(new byte[len], pos);
            this.AddArg(pos, len);
        }

        public override ByteArray WriteDescriptor(int pos, byte[] descriptor)
        {
            this.WriteDefineByte(0, pos++);
            this.WriteBytes(descriptor, pos);
            return this.baOrigData.Clone(pos);
        }

        public override bool FillWithProcedureReader(TextReader reader, short rowCount)
        {
            if (reader == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "reader"));
            }

            bool streamExhausted = false;
            int maxDataSize = (this.MaxDataSize / 2) * 2;

            int readBufSize = (4096 / 2) * 2;
            if (readBufSize == 0)
            {
                readBufSize = 2;
            }

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
                        {
                            charsToRead = 0;
                        }
                        else
                        {
                            // else advance in the buffer
                            charsToRead -= currCharsRead;
                            startPos += currCharsRead;
                        }
                    }
                }

                this.WriteUnicode(new string(readBuf), this.iExtent, charsRead);
                this.iExtent += charsRead * Consts.UnicodeWidth;
                maxDataSize -= charsRead * Consts.UnicodeWidth;
                bytesWritten += charsRead * Consts.UnicodeWidth;
            }

            // The number of arguments is the number of rows
            this.sArgCount = (short)(bytesWritten / Consts.UnicodeWidth);

            // the data must be marked as 'last part' in case it is a last part.
            if (streamExhausted)
            {
                this.SetLastPart();
            }

            return streamExhausted;
        }

        public override bool FillWithOmsStream(Stream stream, bool asciiForUnicode)
        {
            if (stream == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "stream"));
            }

            // We have to:
            // - read and write only multiples of 'rowSize'
            // - but up to maxReadLength

            bool streamExhausted = false;
            int maxDataSize = this.MaxDataSize;
            int readBufSize = 4096;
            byte[] readBuf = new byte[readBufSize];
            byte[] expandbuf = null;
            if (asciiForUnicode)
            {
                expandbuf = new byte[readBufSize * 2];
            }

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

                    // if the stream is exhausted, we have to look whether it is wholly written.
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

                    this.WriteBytes(expandbuf, this.iExtent, bytesRead * 2);
                    this.iExtent += bytesRead * 2;
                    maxDataSize -= bytesRead * 2;
                    bytesWritten += bytesRead * 2;
                }
                else
                {
                    this.WriteBytes(readBuf, this.iExtent, bytesRead);
                    this.iExtent += bytesRead;
                    maxDataSize -= bytesRead;
                    bytesWritten += bytesRead;
                }
            }

            // The number of arguments is the number of rows
            this.sArgCount = (short)(bytesWritten / (asciiForUnicode ? 2 : 1));

            // the data must be marked as 'last part' in case it is a last part.
            if (streamExhausted)
            {
                this.SetLastPart();
            }

            return streamExhausted;
        }

        public override bool FillWithProcedureStream(Stream stream, short rowCount)
        {
            if (stream == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "stream"));
            }

            bool streamExhausted = false;
            int maxDataSize = this.MaxDataSize;
            if (maxDataSize > short.MaxValue)
            {
                maxDataSize = short.MaxValue;
            }

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

                this.WriteBytes(readBuffer, this.iExtent, bytesRead);
                this.iExtent += bytesRead;
                maxDataSize -= bytesRead;
                bytesWritten += bytesRead;
            }

            this.sArgCount = (short)(bytesWritten / rowsize);

            if (streamExhausted)
            {
                this.SetLastPart();
            }

            return streamExhausted;
        }
    }

    #endregion
}
