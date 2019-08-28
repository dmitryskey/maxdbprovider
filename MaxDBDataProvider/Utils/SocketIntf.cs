// Copyright © 2005-2018 Dmitry S. Kataev
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

namespace MaxDB.Data.Utilities
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Text;

    /// <summary>
    /// Interface to support tcp and ssl connection.
    /// </summary>
    internal interface IMaxDBSocket
    {
        bool ReopenSocketAfterInfoPacket { get; }

        bool DataAvailable { get; }

        Stream Stream { get; }

        string Host { get; }

        int Port { get; }

        IMaxDBSocket Clone();

        void Close();
    }

    internal class SocketClass : IMaxDBSocket, IDisposable
    {
        public static void CheckConnection(string host, int port, int timeout)
        {
            using (var sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                bool isConnected = false;

                var entries = Dns.GetHostEntry(host);
                foreach (var ipAddr in entries.AddressList)
                {
                    sock.Blocking = false;
                    try
                    {
                        sock.Connect(new IPEndPoint(ipAddr, port));
                    }
                    catch (SocketException ex)
                    {
                        if (ex.ErrorCode != 10035 && ex.ErrorCode != 10036)
                        {
                            throw;
                        }
                    }

                    isConnected = sock.Poll(timeout * 1000000, SelectMode.SelectWrite);
                    if (isConnected)
                    {
                        break;
                    }
                }

                if (!isConnected)
                {
                    throw new MaxDBException();
                }
            }
        }

        public SocketClass(string host, int port, int timeout, bool checkSocket)
        {
            this.Host = host;
            this.Port = port;
            this.Timeout = timeout;

            try
            {
                if (checkSocket && timeout > 0)
                {
                    CheckConnection(host, port, timeout);
                }

                this.Client = new TcpClient(host, port)
                {
                    ReceiveTimeout = this.Timeout * 1000,
                };
            }
            catch (Exception ex)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT_FAILED, this.Host, this.Port), ex);
            }
        }

        #region SocketIntf Members

        public virtual bool ReopenSocketAfterInfoPacket => true;

        public virtual bool DataAvailable => this.Client != null ? this.Client.GetStream().DataAvailable : false;

        protected TcpClient Client { get; set; }

        public int Timeout { get; }

        public virtual Stream Stream => this.Client?.GetStream();

        public string Host { get; }

        public int Port { get; }

        public virtual IMaxDBSocket Clone() => new SocketClass(this.Host, this.Port, this.Timeout, false);

        public virtual void Close()
        {
            this.Client.Close();
            this.Client = null;
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing && this.Client != null)
            {
                ((IDisposable)this.Client).Dispose();
            }
        }

        #endregion
    }

    internal class SslSocketClass : SocketClass, IMaxDBSocket
    {
        private SslStream mSslStream;
        private string strCertificateError;
        private readonly string strCertificateName;

        public SslSocketClass(string host, int port, int timeout, bool check_socket, string certificate)
            : base(host, port, timeout, check_socket)
        {
            this.strCertificateName = certificate;
            this.mSslStream = new SslStream(
                this.Client.GetStream(),
                false,
                (sender, cert, chain, sslPolicyErrors) =>
                {
                    this.strCertificateError = string.Empty;

                    if (sslPolicyErrors == SslPolicyErrors.None)
                    {
                        return true;
                    }

                    this.strCertificateError = MaxDBMessages.Extract(MaxDBError.SSL_CERTIFICATE, sslPolicyErrors);

                    // Do not allow this client to communicate with unauthenticated servers.
                    return false;
                },
                null
            );

            try
            {
                this.mSslStream.AuthenticateAsClient(certificate);
            }
            catch (AuthenticationException ex)
            {
                throw new MaxDBException(this.strCertificateError + ". " + ex.Message);
            }
        }

        public override Stream Stream => this.mSslStream;

        public override IMaxDBSocket Clone() => new SslSocketClass(this.Host, this.Port, this.Timeout, false, this.strCertificateName);

        public override void Close()
        {
            this.mSslStream.Close();
            this.mSslStream = null;
            this.Client.Close();
            this.Client = null;
        }
    }

    #region "Join stream class reimplementation"

    internal class JoinStream : Stream
    {
        private readonly Stream[] mStreams;
        private Stream mCurrentStream;
        private int mCurrentIndex = -1;

        public JoinStream(Stream[] streams)
        {
            this.mStreams = streams;
            this.NextStream();
        }

        protected void NextStream()
        {
            this.mCurrentIndex++;
            this.mCurrentStream = this.mCurrentIndex >= this.mStreams.Length ? null : this.mStreams[this.mCurrentIndex];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int result = 0;
            int chunkLen;

            while (this.mCurrentStream != null && count > 0)
            {
                chunkLen = this.mCurrentStream.Read(buffer, offset, count);

                if (chunkLen == 0)
                {
                    this.NextStream();
                }
                else
                {
                    offset += chunkLen;
                    count -= chunkLen;
                    result += chunkLen;
                }
            }

            if (result == 0 && this.mCurrentStream == null)
            {
                result = -1;
            }

            return result;
        }

        public override void Close()
        {
            for (int i = this.mCurrentIndex; i < this.mStreams.Length; i++)
            {
                try
                {
                    if (this.mStreams[i] != null)
                    {
                        this.mStreams[i].Close();
                    }
                }
                catch (ObjectDisposedException)
                {
                    // ignore
                }
            }
        }

        public override void Flush()
        {
            for (int i = this.mCurrentIndex; i < this.mStreams.Length; i++)
            {
                if (this.mStreams[i] != null)
                {
                    this.mStreams[i].Flush();
                }
            }
        }

        public override long Length
        {
            get
            {
                long length = 0;
                for (int i = this.mCurrentIndex; i < this.mStreams.Length; i++)
                {
                    try
                    {
                        if (this.mStreams[i] != null)
                        {
                            length += this.mStreams[i].Length;
                        }
                    }
                    catch (NotSupportedException)
                    {
                        // ignore
                    }
                    catch (ObjectDisposedException)
                    {
                        // ignore
                    }
                }

                return length;
            }
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    #endregion

    #region "Join text reader class reimplemetation"

    internal class JoinTextReader : TextReader
    {
        protected TextReader[] mReaders;
        protected TextReader mCurrentReader;
        protected int iCurrentIndex = -1;

        public JoinTextReader(TextReader[] readers)
        {
            this.mReaders = readers;
            this.NextReader();
        }

        protected void NextReader()
        {
            this.iCurrentIndex++;
            this.mCurrentReader = this.iCurrentIndex >= this.mReaders.Length ? null : this.mReaders[this.iCurrentIndex];
        }

        public override int Read(char[] buffer, int index, int count)
        {
            int result = 0;
            int chunkLen;

            while (this.mCurrentReader != null && count > 0)
            {
                chunkLen = this.mCurrentReader.Read(buffer, index, count);
                if (chunkLen == -1)
                {
                    this.NextReader();
                }
                else
                {
                    index += chunkLen;
                    count -= chunkLen;
                    result += chunkLen;
                }
            }

            if (result == 0 && this.mCurrentReader == null)
            {
                result = -1;
            }

            return result;
        }

        public override int Read()
        {
            int result = 0;
            while (this.mCurrentReader != null)
            {
                result = this.mCurrentReader.Read();
                if (result == -1)
                {
                    this.NextReader();
                }
            }

            if (this.mCurrentReader == null)
            {
                result = -1;
            }

            return result;
        }

        public override void Close()
        {
            foreach (var reader in this.mReaders)
            {
                try
                {
                    if (reader != null)
                    {
                        reader.Close();
                    }
                }
                catch (ObjectDisposedException)
                {
                    // ignore
                }
            }
        }
    }
    #endregion

    #region "Stream filter class reimplementation"

    internal class FilteredStream : Stream
    {
        private readonly int iMaxLength;
        private readonly Stream mStream;
        private int iReadlength;

        public FilteredStream(Stream stream, int length)
        {
            this.iMaxLength = length;
            this.mStream = stream;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new InvalidOperationException();
            }

            if (offset < 0 || offset > buffer.Length || count < 0 || (offset + count) > buffer.Length || (offset + count) < 0)
            {
                throw new OverflowException();
            }

            if (this.iReadlength >= this.iMaxLength)
            {
                return -1;
            }

            if (this.iReadlength + count > this.iMaxLength)
            {
                count = this.iMaxLength - this.iReadlength;
            }

            if (count <= 0)
            {
                return 0;
            }

            count = this.mStream.Read(buffer, offset, count);
            this.iReadlength += count;
            return count;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override void Flush() => this.mStream.Flush();

        public override long Length => this.mStream.Length;
    }

    #endregion

    #region "Text reader filter class reimplementation"

    internal class TextReaderFilter : TextReader
    {
        private readonly int iMaxLength;
        private readonly TextReader mStream;
        private int iReadLength;

        public TextReaderFilter(TextReader stream, int length)
        {
            this.iMaxLength = length;
            this.mStream = stream;
        }

        public override int Read() => this.iMaxLength <= this.iReadLength++ ? -1 : this.mStream.Read();

        public override int Read(char[] buffer, int index, int count)
        {
            if (buffer == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));
            }

            if (index < 0 || index > buffer.Length || count < 0 || (index + count) > buffer.Length || (index + count) < 0)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INDEX_OUTOFRANGE, index));
            }

            if (this.iReadLength >= this.iMaxLength)
            {
                return -1;
            }

            if (this.iReadLength + count > this.iMaxLength)
            {
                count = this.iMaxLength - this.iReadLength;
            }

            if (count <= 0)
            {
                return 0;
            }

            count = this.mStream.Read(buffer, index, count);
            this.iReadLength += count;
            return count;
        }

        public override void Close() => this.mStream.Close();
    }

    #endregion

    #region "Raw byte stream reader class implementation"

    internal class RawByteReader : StreamReader
    {
        public RawByteReader(Stream stream)
            : base(stream)
        {
        }

        public override int Read(char[] buffer, int index, int count)
        {
            if (buffer == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));
            }

            try
            {
                byte[] bbuf = new byte[count];
                int result = this.BaseStream.Read(bbuf, 0, count);

                if (result == -1)
                {
                    return -1;
                }

                int off_i = index;
                for (int i = 0; i < result; i++, off_i++)
                {
                    int current = bbuf[i] & 0xFF;
                    buffer[off_i] = (char)current;
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new IOException(ex.Message);
            }
        }
    }

    #endregion

    #region "Stream class based on reader"

    internal class ReaderStream : Stream
    {
        private readonly TextReader mReader;
        private readonly char[] chBuffer = new char[4096];
        private byte[] byBuffer;
        private int iBufPos;
        private int iBufExtent;
        private bool bAtEnd;
        private readonly bool bSevenBit;

        public ReaderStream(TextReader reader, bool sevenBit)
        {
            this.mReader = reader;
            this.bSevenBit = sevenBit;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => 0;

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override void Close()
        {
            this.iBufPos = 0;
            this.iBufExtent = 0;
            this.mReader.Close();
        }

        public override int ReadByte()
        {
            int result;

            if (this.iBufPos >= this.iBufExtent)
            {
                this.FillBuffer();
            }

            result = this.byBuffer[this.iBufPos];
            this.iBufPos++;
            return result;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int bytesCopied = 0;
            bool atEnd = false;

            while (count > 0 && !atEnd)
            {
                if (this.iBufPos >= this.iBufExtent)
                {
                    this.FillBuffer();
                }

                if (this.iBufPos >= this.iBufExtent)
                {
                    break;
                }

                int copySize = Math.Min(count, this.iBufExtent - this.iBufPos);
                Buffer.BlockCopy(this.byBuffer, this.iBufPos, buffer, offset, copySize);
                this.iBufPos += copySize;
                offset += copySize;
                count -= copySize;
                bytesCopied += copySize;
                atEnd = this.bAtEnd;
            }

            if (bytesCopied == 0)
            {
                bytesCopied = -1;
            }

            return bytesCopied;
        }

        private void FillBuffer()
        {
            this.iBufPos = 0;
            this.iBufExtent = 0;
            int charsRead = this.mReader.Read(this.chBuffer, 0, this.chBuffer.Length);
            if (charsRead < this.chBuffer.Length)
            {
                this.bAtEnd = true;
            }

            if (charsRead < 0)
            {
                return;
            }

            this.byBuffer = this.bSevenBit ? Encoding.ASCII.GetBytes(this.chBuffer, 0, charsRead) : Encoding.GetEncoding(1252).GetBytes(this.chBuffer, 0, charsRead);
            this.iBufExtent = this.byBuffer.Length;
        }
    }
    #endregion
}
