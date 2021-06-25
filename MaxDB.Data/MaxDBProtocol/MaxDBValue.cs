//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBValue.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
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

using System;
using System.Data;
using System.Globalization;
using System.IO;
using MaxDB.Data.Interfaces;
using MaxDB.Data.Interfaces.MaxDBProtocol;
using MaxDB.Data.Interfaces.Utils;
using MaxDB.Data.Utils;

namespace MaxDB.Data.MaxDBProtocol
{
    #region "Put Value class"

    internal class PutValue : IPutValue
    {
        private byte[] byDescription;
        protected IByteArray baDescriptionMark;
        private Stream mStream;

        /// <summary>
        /// The following is used to reread data to recover from a timeout.
        /// </summary>
        protected IByteArray baRequestData;
        protected int iRequestLength;

        public PutValue(Stream stream, int length, int position)
        {
            this.mStream = length >= 0 ? new FilteredStream(stream, length) : stream;

            this.BufferPosition = position;
        }

        public PutValue(byte[] bytes, int bufferPosition)
        {
            this.mStream = new MemoryStream(bytes);
            this.BufferPosition = bufferPosition;
        }

        protected PutValue(int bufferPosition)
        {
            this.BufferPosition = bufferPosition;
        }

        public int BufferPosition { get; }

        public virtual bool AtEnd => this.mStream == null;

        public void WriteDescriptor(IMaxDBDataPart memory, int pos)
        {
            if (memory == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, "memory"));
            }

            if (this.byDescription == null)
            {
                this.byDescription = new byte[LongDesc.Size];
            }

            this.baDescriptionMark = memory.WriteDescriptor(pos, this.byDescription);
        }

        private static byte[] NewDescriptor => new byte[LongDesc.Size];

        public void PutDescriptor(IMaxDBDataPart memory, int pos)
        {
            if (memory == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, "memory"));
            }

            if (this.byDescription == null)
            {
                this.byDescription = NewDescriptor;
            }

            this.baDescriptionMark = memory.WriteDescriptor(pos, this.byDescription);
        }

        public void SetDescriptor(byte[] description) => this.byDescription = description;

        public virtual void TransferStream(IMaxDBDataPart dataPart)
        {
            if (dataPart == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, "dataPart"));
            }

            if (this.AtEnd)
            {
                dataPart.MarkEmptyStream(this.baDescriptionMark);
            }
            else if (dataPart.FillWithStream(this.mStream, this.baDescriptionMark, this))
            {
                try
                {
                    this.mStream.Close();
                }
                catch (ObjectDisposedException)
                {
                    // ignore
                }
                finally
                {
                    this.mStream = null;
                }
            }
        }

        public void TransferStream(IMaxDBDataPart dataPart, short streamIndex)
        {
            if (dataPart == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, "dataPart"));
            }

            this.TransferStream(dataPart);
            this.baDescriptionMark.WriteInt16(streamIndex, LongDesc.ValInd);
        }

        public void MarkAsLast(IMaxDBDataPart dataPart)
        {
            if (dataPart == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, "dataPart"));
            }

            // avoid putting it in if this would break the aligned boundary.
            if (dataPart.Length - dataPart.Extent - 8 - LongDesc.Size - 1 < 0)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INDEXOUTOFRANGE, dataPart.Length));
            }

            int descriptorPos = dataPart.Extent;
            this.WriteDescriptor(dataPart, descriptorPos);
            dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
            this.baDescriptionMark.WriteByte(LongDesc.LastPutval, LongDesc.ValMode);
        }

        public void MarkRequestedChunk(IByteArray data, int length)
        {
            this.baRequestData = data;
            this.iRequestLength = length;
        }

        public void MarkErrorStream()
        {
            this.baDescriptionMark.WriteByte(LongDesc.Error, LongDesc.ValMode);
            this.baDescriptionMark.WriteInt32(0, LongDesc.ValPos);
            this.baDescriptionMark.WriteInt32(0, LongDesc.ValLen);

            try
            {
                this.mStream.Close();
            }
            catch (IOException)
            {
                // ignore
            }

            this.mStream = null;
        }

        public virtual void Reset()
        {
            if (this.baRequestData != null)
            {
                byte[] data = this.baRequestData.ReadBytes(0, this.iRequestLength);
                var firstChunk = new MemoryStream(data);
                this.mStream = this.mStream == null ? firstChunk : (Stream)new JoinStream(new Stream[] { firstChunk, this.mStream });
            }

            this.baRequestData = null;
        }

        #region IDisposable Members

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing && this.mStream != null)
            {
                ((IDisposable)this.mStream).Dispose();
            }
        }

        #endregion
    }

    #endregion

    #region "Put Unicode Value class"

    internal class PutUnicodeValue : PutValue
    {
        private TextReader mReader;

        public PutUnicodeValue(TextReader readerp, int length, int bufpos)
            : base(bufpos) => this.mReader = length >= 0 ? new TextReaderFilter(readerp, length) : readerp;

        public PutUnicodeValue(char[] source, int bufpos)
            : base(bufpos) => this.mReader = new StringReader(new string(source));

        public override bool AtEnd => this.mReader == null;

        public override void TransferStream(IMaxDBDataPart dataPart)
        {
            if (!this.AtEnd && dataPart.FillWithReader(this.mReader, this.baDescriptionMark, this))
            {
                try
                {
                    this.mReader.Close();
                }
                catch (ObjectDisposedException)
                {
                    // ignore
                }
                finally
                {
                    this.mReader = null;
                }
            }
        }

        public override void Reset()
        {
            if (this.baRequestData != null)
            {
                var firstChunk = new StringReader(this.baRequestData.ReadUnicode(0, this.iRequestLength));

                if (this.mReader == null)
                {
                    this.mReader = firstChunk;
                }
                else
                {
                    this.mReader = new JoinTextReader(new TextReader[] { firstChunk, this.mReader });
                }

                this.baRequestData = null;
            }
        }
    }

    #endregion

    #region "Abstract Procedure Put Value class"

    internal abstract class AbstractProcedurePutValue
    {
        private readonly MaxDBTranslators.DBTechTranslator m_translator;
        private readonly IByteArray m_descriptor;
        private IByteArray m_descriptorMark;

        public AbstractProcedurePutValue(MaxDBTranslators.DBTechTranslator translator)
        {
            this.m_translator = translator;
            this.m_descriptor = new ByteArray(LongDesc.Size);
            this.m_descriptor.WriteByte(LongDesc.StateStream, LongDesc.State);
        }

        public void UpdateIndex(int index) => this.m_descriptorMark.WriteInt16((short)index, LongDesc.ValInd);

        public void PutDescriptor(IMaxDBDataPart memory)
        {
            memory.WriteDefineByte(0, this.m_translator.BufPos - 1);
            memory.WriteBytes(this.m_descriptor.GetArrayData(), this.m_translator.BufPos);
            this.m_descriptorMark = memory.OrigData.Clone(this.m_translator.BufPos);
        }

        public abstract void TransferStream(MaxDBDataPart dataPart, short rowCount);

        public abstract void CloseStream();
    }

    #endregion

    #region "Basic Procedure Put Value class"

    internal abstract class BasicProcedurePutValue : AbstractProcedurePutValue
    {
        protected Stream mStream;

        internal BasicProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, Stream stream, int length)
            : base(translator) => this.mStream = length == -1 ? stream : new FilteredStream(stream, length);

        public override void TransferStream(MaxDBDataPart dataPart, short rowCount) => dataPart.FillWithProcedureStream(this.mStream, rowCount);

        public override void CloseStream()
        {
            try
            {
                this.mStream.Close();
            }
            catch (Exception ex)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAMIOEXCEPTION, ex.Message));
            }
        }
    }

    #endregion

    #region "ASCII Procedure Put Value class"

    internal class ASCIIProcedurePutValue : BasicProcedurePutValue
    {
        public ASCIIProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, byte[] bytes)
            : this(translator, new MemoryStream(bytes), -1)
        {
        }

        public ASCIIProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, Stream stream, int length)
            : base(translator, stream, length)
        {
        }
    }
    #endregion

    #region "Binary Procedure Put Value class"

    internal class BinaryProcedurePutValue : BasicProcedurePutValue
    {
        public BinaryProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, Stream stream, int length)
            : base(translator, stream, length)
        {
        }
    }

    #endregion

    #region "Unicode Procedure Put Value class"

    internal class UnicodeProcedurePutValue : AbstractProcedurePutValue
    {
        protected TextReader mReader;

        public UnicodeProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, char[] buffer)
            : this(translator, new StringReader(new string(buffer)), -1)
        {
        }

        public UnicodeProcedurePutValue(MaxDBTranslators.DBTechTranslator translator, TextReader reader, int length)
            : base(translator) => this.mReader = length == -1 ? reader : new TextReaderFilter(reader, length);

        public override void TransferStream(MaxDBDataPart dataPart, short rowCount) => dataPart.FillWithProcedureReader(this.mReader, rowCount);

        public override void CloseStream()
        {
            try
            {
                this.mReader.Close();
            }
            catch (Exception ex)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAMIOEXCEPTION, ex.Message));
            }
        }
    }

    #endregion

    #region "Abstract Get Value class"

    internal abstract class AbstractGetValue
    {
        protected IMaxDBConnection dbConnection;
        protected byte[] byDescriptor;
        internal IByteArray bsStreamBuffer;
        internal int iItemsInBuffer;
        protected int iItemSize;
        internal bool bAtEnd;
        internal long lLongPosition;

        public AbstractGetValue(IMaxDBConnection connection, byte[] descriptor, IByteArray dataPart, int itemSize)
        {
            this.dbConnection = connection;
            this.iItemSize = itemSize;
            this.SetupStreamBuffer(descriptor, dataPart);
        }

        internal bool NextChunk()
        {
            try
            {
                int valMode = this.byDescriptor[LongDesc.ValMode];
                if (valMode == LongDesc.LastData || valMode == LongDesc.AllData)
                {
                    this.bAtEnd = true;
                    return false;
                }

                var requestPacket = this.dbConnection.Comm.GetRequestPacket();
                IMaxDBReplyPacket replyPacket;
                var longpart = requestPacket.InitGetValue(this.dbConnection.AutoCommit);
                longpart.WriteByte(0, 0);
                longpart.WriteBytes(this.byDescriptor, 1);
                int maxval = int.MaxValue - 1;
                longpart.WriteInt32(maxval, 1 + LongDesc.ValLen);
                longpart.AddArg(1, LongDesc.Size);
                longpart.Close();
                try
                {
                    replyPacket = this.dbConnection.Comm.Execute(this.dbConnection.ConnArgs, requestPacket, this, GCMode.DELAYED);
                }
                catch (MaxDBException ex)
                {
                    throw new IOException(ex.Message, ex);
                }

                replyPacket.FindPart(PartKind.LongData);
                int dataPos = replyPacket.PartDataPos;
                this.byDescriptor = replyPacket.ReadDataBytes(dataPos, LongDesc.Size + 1);
                if (this.byDescriptor[LongDesc.ValMode] == LongDesc.StartposInvalid)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALIDSTARTPOSITION));
                }

                this.SetupStreamBuffer(this.byDescriptor, replyPacket.Clone(dataPos));
                return true;
            }
            catch (PartNotFoundException)
            {
                throw new IOException(MaxDBMessages.Extract(MaxDBError.LONGDATAEXPECTED));
            }
            catch (DataException ex)
            {
                throw new StreamIOException(ex);
            }
        }

        private void SetupStreamBuffer(byte[] descriptor, IByteArray dataPart)
        {
            var desc = new ByteArray(descriptor); // ??? swapMode?
            int dataStart;

            dataStart = desc.ReadInt32(LongDesc.ValPos) - 1;
            this.iItemsInBuffer = desc.ReadInt32(LongDesc.ValLen) / this.iItemSize;
            this.bsStreamBuffer = dataPart.Clone(dataStart);
            this.byDescriptor = descriptor;
            if (descriptor[LongDesc.InternPos] == 0 && descriptor[LongDesc.InternPos + 1] == 0 &&
                descriptor[LongDesc.InternPos + 2] == 0 && descriptor[LongDesc.InternPos + 3] == 0)
            {
                descriptor[LongDesc.InternPos + 3] = 1;
            }
        }

        public abstract Stream ASCIIStream { get; }

        public abstract Stream BinaryStream { get; }

        public abstract TextReader CharacterStream { get; }
    }

    #endregion

    #region "Get Value class"

    internal class GetValue : AbstractGetValue
    {
        public GetValue(IMaxDBConnection connection, byte[] descriptor, IByteArray dataPart)
            : base(connection, descriptor, dataPart, 1)
        {
        }

        public override Stream ASCIIStream => new GetValueStream(this);

        public override Stream BinaryStream => new GetValueStream(this);

        public override TextReader CharacterStream => new RawByteReader(this.ASCIIStream);

        #region "Get Value Stream class"

        class GetValueStream : Stream
        {
            readonly GetValue m_value;

            public GetValueStream(GetValue val) => this.m_value = val;

            public override bool CanRead => true;

            public override bool CanSeek => true;

            public override bool CanWrite => false;

            public override void Flush() => throw new NotSupportedException();

            public override long Length => this.m_value.bsStreamBuffer.Length;

            public override long Position
            {
                get => this.m_value.bsStreamBuffer.Offset;
                set => this.m_value.bsStreamBuffer.Offset = (int)value;
            }

            public override int ReadByte()
            {
                int result;

                if (this.m_value.iItemsInBuffer <= 0)
                {
                    this.m_value.NextChunk();
                }

                if (this.m_value.bAtEnd)
                {
                    return -1;
                }

                result = this.m_value.bsStreamBuffer.ReadByte(0);
                this.m_value.bsStreamBuffer.Offset++;
                this.m_value.iItemsInBuffer--;
                this.m_value.lLongPosition++;
                return result;
            }

            public override int Read(byte[] b, int off, int len)
            {
                int bytesCopied = 0;
                int chunkSize;
                byte[] chunk;

                while ((len > 0) && !this.m_value.bAtEnd)
                {
                    if (this.m_value.iItemsInBuffer <= 0)
                    {
                        this.m_value.NextChunk();
                    }

                    if (!this.m_value.bAtEnd)
                    {
                        // copy bytes in buffer
                        chunkSize = Math.Min(len, this.m_value.iItemsInBuffer);
                        chunk = this.m_value.bsStreamBuffer.ReadBytes(0, chunkSize);
                        Buffer.BlockCopy(chunk, 0, b, off, chunkSize);
                        len -= chunkSize;
                        off += chunkSize;
                        this.m_value.iItemsInBuffer -= chunkSize;
                        this.m_value.bsStreamBuffer.Offset += chunkSize;
                        bytesCopied += chunkSize;
                    }
                }

                if (bytesCopied == 0 && this.m_value.bAtEnd)
                {
                    bytesCopied = -1;
                }
                else
                {
                    this.m_value.lLongPosition += bytesCopied;
                }

                return bytesCopied;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        this.m_value.bsStreamBuffer.Offset = (int)offset;
                        break;
                    case SeekOrigin.Current:
                        this.m_value.bsStreamBuffer.Offset += (int)offset;
                        break;
                    case SeekOrigin.End:
                        this.m_value.bsStreamBuffer.Offset = this.m_value.bsStreamBuffer.Length - (int)offset - 1;
                        break;
                }

                return this.m_value.bsStreamBuffer.Offset;
            }

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        #endregion
    }

    #endregion

    #region "Get LOB Value class"

    internal class GetLOBValue : GetValue
    {
        public GetLOBValue(IMaxDBConnection connection, byte[] descriptor, IByteArray dataPart)
            : base(connection, descriptor, dataPart)
        {
        }
    }

    #endregion

    #region "Get Unicode Value class"

    internal class GetUnicodeValue : AbstractGetValue
    {
        public GetUnicodeValue(IMaxDBConnection connection, byte[] descriptor, IByteArray dataPart)
            : base(connection, descriptor, dataPart, 2)
        {
        }

        public override Stream ASCIIStream => new ReaderStream(this.CharacterStream, false);

        public override Stream BinaryStream => new ReaderStream(this.CharacterStream, false);

        public override TextReader CharacterStream => new GetUnicodeValueReader(this);

        #region "Get Unicode Value Reader class"

        public class GetUnicodeValueReader : TextReader
        {
            private readonly GetUnicodeValue m_value;

            public GetUnicodeValueReader(GetUnicodeValue val) => this.m_value = val;

            public override int Read(char[] b, int offset, int count)
            {
                int charsCopied = 0;
                int chunkChars;
                int chunkBytes;
                char[] chunk;

                while ((count > 0) && !this.m_value.bAtEnd)
                {
                    if (this.m_value.iItemsInBuffer <= 0)
                    {
                        this.m_value.NextChunk();
                    }

                    if (!this.m_value.bAtEnd)
                    {
                        chunkChars = Math.Min(count, this.m_value.iItemsInBuffer);
                        chunkBytes = chunkChars * Consts.UnicodeWidth;
                        chunk = this.m_value.bsStreamBuffer.ReadUnicode(0, chunkBytes).ToCharArray();
                        Array.Copy(chunk, 0, b, offset, chunkChars);
                        count -= chunkChars;
                        offset += chunkChars;
                        this.m_value.iItemsInBuffer -= chunkChars;
                        this.m_value.bsStreamBuffer = this.m_value.bsStreamBuffer.Clone(chunkBytes);
                        charsCopied += chunkChars;
                    }
                }

                if (charsCopied == 0 && this.m_value.bAtEnd)
                {
                    charsCopied = -1;
                }
                else
                {
                    this.m_value.lLongPosition += charsCopied;
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
        public GetUnicodeLOBValue(IMaxDBConnection connection, byte[] descriptor, IByteArray dataPart)
            : base(connection, descriptor, dataPart)
        {
        }
    }

    #endregion

    #region "Fetch chunk class"

    /// <summary>
    /// The outcome of a particular fetch operation.  A fetch operation
    /// results in one(when the fetch size is 1) or more(when the fetch
    /// size is >1) data rows returned from the database server.Depending on
    /// the kind of the fetch, the positioning in the result at the database
    /// server and the start and end index computation does differ.
    /// </summary>
    internal class FetchChunk
    {
        private readonly MaxDBConnection dbConnection; // database connection
        private readonly IMaxDBReplyPacket mReplyPacket;  // The data packet from the fetch operation.
        private readonly FetchType mType;   // type of fetch chunk
        private int iCurrentOffset; // The current index within this chunk, starting with 0.
        private readonly int iRecordSize;   // The number of bytes in a row.
        private int iRowsInResultSet;   // The number of rows in the complete result set, or -1 if this is not known.

        public FetchChunk(MaxDBConnection conn, FetchType type, int absoluteStartRow, IMaxDBReplyPacket replyPacket, int recordSize, int maxRows, int rowsInResultSet)
        {
            this.dbConnection = conn;
            this.mReplyPacket = replyPacket;
            this.mType = type;
            this.iRecordSize = recordSize;
            this.iRowsInResultSet = rowsInResultSet;
            try
            {
                replyPacket.FirstSegment();
                replyPacket.FindPart(PartKind.Data);
            }
            catch (PartNotFoundException)
            {
                throw new DataException(MaxDBMessages.Extract(MaxDBError.FETCHNODATAPART));
            }

            this.Size = replyPacket.PartArgsCount;
            int dataPos = replyPacket.PartDataPos;
            this.ReplyData = replyPacket.Clone(dataPos);
            this.CurrentRecord = this.ReplyData.Clone(this.iCurrentOffset * this.iRecordSize);
            if (absoluteStartRow > 0)
            {
                this.Start = absoluteStartRow;
                this.End = absoluteStartRow + this.Size - 1;
            }
            else
            {
                if (rowsInResultSet != -1)
                {
                    this.Start = rowsInResultSet + (absoluteStartRow < 0 ? absoluteStartRow + 1 : -absoluteStartRow + this.Size);

                    this.End = this.Start + this.Size - 1;
                }
                else
                {
                    this.Start = absoluteStartRow;
                    this.End = absoluteStartRow + this.Size - 1;
                }
            }

            DateTime dt = DateTime.Now;

            // >>> SQL TRACE
            this.dbConnection.mLogger.SqlTrace(dt, "FETCH BUFFER START: " + this.Start.ToString(CultureInfo.InvariantCulture));
            this.dbConnection.mLogger.SqlTrace(dt, "FETCH BUFFER END  : " + this.End.ToString(CultureInfo.InvariantCulture));
            // <<< SQL TRACE

            this.DetermineFlags(maxRows);
        }

        /// <summary>
        /// Determines whether this chunk is the first and/or last of
        /// a result set.This is done by checking the index boundaries,
        /// and also the LAST PART information of the reply packet.
        /// A forward chunk is also the last if it contains the record at
        /// the maxRows row, as the user decided to make
        /// the limit here.
        /// </summary>
        /// <param name="maxRows">Maximum number of rows.</param>
        private void DetermineFlags(int maxRows)
        {
            if (this.mReplyPacket.WasLastPart)
            {
                switch (this.mType)
                {
                    case FetchType.FIRST:
                    case FetchType.LAST:
                    case FetchType.RELATIVE_DOWN:
                        this.IsFirst = true;
                        this.IsLast = true;
                        break;
                    case FetchType.ABSOLUTE_UP:
                    case FetchType.ABSOLUTE_DOWN:
                    case FetchType.RELATIVE_UP:
                        this.IsLast = true;
                        break;
                }
            }

            if (this.Start == 1)
            {
                this.IsFirst = true;
            }

            if (this.End == -1)
            {
                this.IsLast = true;
            }

            // one special last for maxRows set
            if (maxRows != 0 && this.IsForward && this.End >= maxRows)
            {
                // if we have fetched too much, we have to cut here ...
                this.End = maxRows;
                this.Size = maxRows + 1 - this.Start;
                this.IsLast = true;
            }
        }

        /// <summary>
        /// Gets or sets the current record.
        /// </summary>
        public IByteArray CurrentRecord { get; private set; }

        /// <summary>
        /// Gets the reply data.
        /// </summary>
        public IByteArray ReplyData { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this chunk is the first one or sets the first flag.
        /// Take care, that this information may not be reliable.
        /// </summary>
        /// <value>return true if this is the first, and false if this is not first or the information is not known.</value>
        public bool IsFirst { get; private set; }

        /// <summary>
        /// Returns whether this chunk is the last one or sets the last flag.
        /// Take care, that this information may not be reliable.
        /// </summary>
        /// <value>Returns <c>true</c> if this is the last, and <c>false</c> if this is not first or the information is not known</value>
        public bool IsLast { get; set; }

        /// <summary>
        /// Gets or sets the size of this chunk.
        /// </summary>
        public int Size { get; private set; }

        /// <summary>
        /// Gets the position where the internal position is after the fetch if this chunk is the current chunk.
        /// </summary>
        public int KernelPos
        {
            get
            {
                switch (this.mType)
                {
                    case FetchType.ABSOLUTE_DOWN:
                    case FetchType.RELATIVE_UP:
                    case FetchType.LAST:
                        return this.Start;
                    case FetchType.FIRST:
                    case FetchType.ABSOLUTE_UP:
                    case FetchType.RELATIVE_DOWN:
                    default:
                        return this.End;
                }
            }
        }

        public bool IsForward => this.mType == FetchType.FIRST || this.mType == FetchType.ABSOLUTE_UP || this.mType == FetchType.RELATIVE_UP;

        /// <summary>
        /// Sets the number of rows in the result set.
        /// </summary>
        public int RowsInResultSet
        {
            set => this.iRowsInResultSet = value;
        }

        /// <summary>
        /// Gets the start index of the fetch chunk.
        /// </summary>
        public int Start { get; }

        /// <summary>
        /// Gets the end index of the fetch chunk.
        /// </summary>
        public int End { get; private set; }

        /// <summary>
        /// Returns whether the given row is truly inside the chunk.
        /// </summary>
        /// <param name="row">row the row to check. Rows &lt; 0 count from the end of the result.</param>
        /// <returns>Returns <c>true</c> if the row is inside, <c>false</c> if it's not or the condition could not be determined due to an unknown end of result set.</returns>
        public bool ContainsRow(int row)
        {
            if (this.Start <= row && this.End >= row)
            {
                return true;
            }

            // some tricks depending on whether we are on last/first chunk
            if (this.IsForward && this.IsLast && row < 0)
            {
                return row >= this.Start - this.End - 1;
            }

            if (!this.IsForward && this.IsFirst && row > 0)
            {
                return row <= this.End - this.Start + 1;
            }

            // if we know the number of rows, we can compute this anyway by inverting the row
            if (this.iRowsInResultSet != -1 && ((this.Start < 0 && row > 0) || (this.Start > 0 && row < 0)))
            {
                int inverted_row = (row > 0) ? (row - this.iRowsInResultSet - 1) : (row + this.iRowsInResultSet + 1);
                return this.Start <= inverted_row && this.End >= inverted_row;
            }

            return false;
        }

        /// <summary>
        /// Moves the position inside the chunk by a relative offset.
        /// </summary>
        /// <param name="relativepos">Relative position.</param>
        /// <returns></returns>
        public bool Move(int relativepos)
        {
            if (this.iCurrentOffset + relativepos < 0 || this.iCurrentOffset + relativepos >= this.Size)
            {
                return false;
            }
            else
            {
                this.UnsafeMove(relativepos);
                return true;
            }
        }

        // Moves the position inside the chunk by a relative offset, but unchecked.
        private void UnsafeMove(int relativepos)
        {
            this.iCurrentOffset += relativepos;
            this.CurrentRecord = this.CurrentRecord.Clone(relativepos * this.iRecordSize);
        }

        // Sets the current record to the supplied absolute position.
        public bool SetRow(int row)
        {
            if (this.Start <= row && this.End >= row)
            {
                this.UnsafeMove(row - this.Start - this.iCurrentOffset);
                return true;
            }

            // some tricks depending on whether we are on last/first chunk
            if (this.IsForward && this.IsLast && row < 0 && row >= this.Start - this.End - 1)
            {
                // move backward to the row from the end index, but
                // honor the row number start at 1, make this
                // relative to chunk by subtracting start index
                // and relative for the move by subtracting the
                // current offset
                this.UnsafeMove(this.End + row + 1 - this.Start - this.iCurrentOffset);
                return true;
            }

            if (!this.IsForward && this.IsFirst && row > 0 && row <= this.End - this.Start + 1)
            {
                // simple. row is > 0. m_startIndex if positive were 1 ...
                this.UnsafeMove(row - 1 - this.iCurrentOffset);
            }

            // if we know the number of rows, we can compute this anyway by inverting the row
            if (this.iRowsInResultSet != -1 && ((this.Start < 0 && row > 0) || (this.Start > 0 && row < 0)))
            {
                int inverted_row = (row > 0) ? (row - this.iRowsInResultSet - 1) : (row + this.iRowsInResultSet + 1);
                return this.SetRow(inverted_row);
            }

            return false;
        }
    }

    #endregion
}
