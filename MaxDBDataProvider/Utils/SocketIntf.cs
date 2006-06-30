//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using System.Text;
using System.Net.Sockets;
using System.Reflection;
using System.Net;
using System.Collections;

#if NET20
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Security.Authentication;
#endif // NET20

namespace MaxDB.Data.Utilities
{
	/// <summary>
	/// Interface to support tcp and ssl connection.
	/// </summary>
    /// 

#if SAFE
	public interface IMaxDBSocket
	{
		bool ReopenSocketAfterInfoPacket{get;}
		bool DataAvailable{get;}
		Stream Stream{get;}
		string Host{get;}
		int Port{get;}

		IMaxDBSocket Clone();
		void Close();
	}

	internal class SocketClass : IMaxDBSocket, IDisposable
	{
		private string strHost;
        private int iPort;
        private int iTimeout;
        private TcpClient mClient;

		public SocketClass(string host, int port, int timeout, bool check_socket) 
		{
			strHost = host;
			iPort = port;
			iTimeout = timeout;
			
			try
			{
				if (check_socket && timeout > 0)
				{
					Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
					bool connect_succeeded = false;

                    IPHostEntry entries =
#if NET20
                        Dns.GetHostEntry(host);
#else
					    Dns.GetHostByName(host);
#endif
					foreach(IPAddress ipAddr in entries.AddressList)
					{
						sock.Blocking = false;
						try 
						{
							sock.Connect(new IPEndPoint(ipAddr, iPort));
						}
						catch(SocketException ex)
						{
							if (ex.ErrorCode != 10035 && ex.ErrorCode != 10036)
								throw;
						}

						ArrayList checkWrite = new ArrayList();
						checkWrite.Add(sock);
						ArrayList checkError = new ArrayList();
						checkError.Add(sock);

						Socket.Select(null, checkWrite, checkError, iTimeout * 1000000);
						sock.Blocking = true;
						sock.Close();

						if (checkWrite.Count > 0)
						{
							connect_succeeded = true;
							break;
						}
					}

					if (!connect_succeeded)
						throw new MaxDBException();
				}

				mClient = new TcpClient(host, port);
				mClient.ReceiveTimeout = iTimeout;
			}
			catch(Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT, strHost, iPort), ex);
			}
		}

		#region SocketIntf Members

		public virtual bool ReopenSocketAfterInfoPacket
		{
			get
			{
				return true;
			}
		}

		public virtual bool DataAvailable
		{
			get
			{
				if (mClient != null)
					return mClient.GetStream().DataAvailable;
				else
					return false;
			}
		}

#if NET20
        protected TcpClient Client
        {
            get 
            { 
                return mClient; 
            }
            set
            {
                mClient = value;
            }
        }

		public int Timeout
        {
            get 
            { 
                return iTimeout; 
            }
        }
#endif // NET20

		public virtual Stream Stream
		{
			get
			{
				if (mClient != null)
					return mClient.GetStream();
				else
					return null;
			}
		}

		public string Host
		{
			get
			{
				return strHost;
			}
		}

		public int Port
		{
			get
			{
				return iPort;
			}
		}

		public virtual IMaxDBSocket Clone()
		{
			return new SocketClass(strHost, iPort, iTimeout, false);
		}

		public virtual void Close()
		{
			mClient.Close();
			mClient = null;
		}

		#endregion

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing && mClient != null)
                ((IDisposable)mClient).Dispose();
        }

        #endregion
    }

#if NET20

    internal class SslSocketClass : SocketClass, IMaxDBSocket
    {
        SslStream mSslStream;
        private string strCertificateError;
        private string strServer;

        public SslSocketClass(string host, int port, int timeout, bool check_socket, string server)
            : base(host, port, timeout, check_socket)
        {
            strServer = server;
            mSslStream = new SslStream(Client.GetStream(),
                false,
                new RemoteCertificateValidationCallback(ValidateServerCertificate),
                null
                );
            try
            {
                mSslStream.AuthenticateAsClient(server);
            }
            catch (AuthenticationException e)
            {
                throw new MaxDBException(strCertificateError + ". " + e.Message);
            }
        }

        // The following method is invoked by the RemoteCertificateValidationDelegate.
        private bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            strCertificateError = string.Empty;

            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            strCertificateError = MaxDBMessages.Extract(MaxDBError.SSL_CERTIFICATE, sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }

        public override Stream Stream
        {
            get
            {
                return mSslStream;
            }
        }

        public override IMaxDBSocket Clone()
        {
            return new SslSocketClass(Host, Port, Timeout, false, strServer);
        }

        public override void Close()
        {
            mSslStream.Close();
            mSslStream = null;
            Client.Close();
            Client = null;
        }
    }

#endif // NET20

    #region "Join stream class reimplementation"

    public class JoinStream : Stream
	{
		private Stream[] mStreams;
		private Stream mCurrentStream;
		private int mCurrentIndex = -1;

		public JoinStream(Stream[] streams)
		{
			this.mStreams = streams;
			NextStream();
		}

		protected void NextStream()
		{
			mCurrentIndex++;
			if (mCurrentIndex >= mStreams.Length) 
				mCurrentStream = null;
			else 
				mCurrentStream = mStreams[mCurrentIndex];
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			int result = 0;
			int chunkLen;

			while (mCurrentStream != null && count > 0) 
			{
				chunkLen = mCurrentStream.Read(buffer, offset, count);
				if (chunkLen == 0) 
					NextStream();
				else 
				{
					offset += chunkLen;
					count -= chunkLen;
					result += chunkLen;
				}
			}
			if (result == 0 && mCurrentStream == null) 
				result = 0;
			return result;
		}

		public override void Close()
		{
			for (int i = mCurrentIndex; i < mStreams.Length; i++) 
			{
				try 
				{
					if (mStreams[i] != null) 
						mStreams[i].Close();
				}
				catch(ObjectDisposedException) 
				{
					// ignore
				}
			}
		}

		public override void Flush()
		{
			for (int i = mCurrentIndex; i < mStreams.Length; i++) 
			{
    			if (mStreams[i] != null) 
						mStreams[i].Flush();
			}
		}

		public override long Length
		{
			get
			{
				long length = 0;
				for (int i = mCurrentIndex; i < mStreams.Length; i++) 
				{
                    try
                    {
                        if (mStreams[i] != null)
                            length += mStreams[i].Length;
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
				return false;
			}
		}

		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

		public override long Position
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
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

	#region "Join text reader class reimplemetation"

	internal class JoinTextReader : TextReader
	{
		protected TextReader[] mReaders;
		protected TextReader mCurrentReader;
		protected int iCurrentIndex = -1;
    
		public JoinTextReader(TextReader[] readers)
		{
			mReaders = readers;
			NextReader();
		}
    

		protected void NextReader()
		{
			iCurrentIndex++;
			if (iCurrentIndex >= mReaders.Length)
				mCurrentReader = null;
			else 
				mCurrentReader = mReaders[iCurrentIndex];
		}
    
		public override int Read(char[] buffer, int index, int count)
		{
			int result = 0;
			int chunkLen;
        
			while (mCurrentReader != null && count > 0) 
			{
				chunkLen = mCurrentReader.Read(buffer, index, count);
				if (chunkLen == -1) 
					NextReader();
				else 
				{
					index += chunkLen;
					count -= chunkLen;
					result += chunkLen;
				}
			}
			if (result == 0 && mCurrentReader == null) 
				result = -1;
      
			return result;
		}
    
		public override int Read()    
		{
			int result = 0;
			while (mCurrentReader != null) 
			{
				result = mCurrentReader.Read();
				if (result == -1)
					NextReader();
			}
			if (mCurrentReader == null) 
				result = -1;
			return result;
		}
    
		public override void Close()
		{
			foreach (TextReader reader in mReaders) 
			{
				try 
				{
					if (reader != null) 
						reader.Close();
				}
				catch(ObjectDisposedException) 
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
		private int iMaxLength;
		private Stream mStream;
		private int iReadlength;
    
		public FilteredStream(Stream stream, int length)
		{
			iMaxLength = length;
			mStream = stream;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			if (buffer == null) 
				throw new InvalidOperationException();
			
			if (offset < 0 || offset > buffer.Length || count < 0	|| (offset + count) > buffer.Length || (offset + count) < 0) 
				throw new OverflowException();

			if (iReadlength >= iMaxLength) 
				return -1;

			if (iReadlength + count > iMaxLength) 
				count = iMaxLength - iReadlength;

			if (count <= 0) 
				return 0;

			count = mStream.Read(buffer, offset, count);
			iReadlength += count;
			return count;
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
				return false;
			}
		}

		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

		public override long Position
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Flush()
		{
			mStream.Flush();
		}

		public override long Length
		{
			get
			{
				return mStream.Length;
			}
		}
	}

	#endregion

	#region "Text reader filter class reimplementation"

	internal class TextReaderFilter : TextReader 
	{
		private int iMaxLength;
		private TextReader mStream;
		private int iReadLength;
	
		public TextReaderFilter(TextReader stream, int length) 
		{
			iMaxLength = length;
			mStream = stream;
		}

		public override int Read()  
		{
			if (iMaxLength <= iReadLength++)
				return -1;
			else
				return mStream.Read();
		}

		public override int Read(char[] buffer, int index, int count) 
		{
			if (buffer == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));

			if (index < 0 || index > buffer.Length || count < 0 || (index + count) > buffer.Length || (index + count) < 0)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INDEX_OUTOFRANGE, index));

			if (iReadLength >= iMaxLength) 
				return -1;
			if (iReadLength + count > iMaxLength) 
				count = iMaxLength - iReadLength;
			if (count <= 0) 
				return 0;

			count = mStream.Read(buffer, index, count);
			iReadLength += count;
			return count;
		}

		public override void Close()
		{
			mStream.Close();
		}
	}

	#endregion

	#region "Raw byte stream reader class implementation"

	public class RawByteReader : StreamReader
	{
		public RawByteReader(Stream stream) : base(stream)
		{
		}

		public override int Read(char[] buffer, int index, int count)
		{
            if (buffer == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "buffer"));

			try 
			{
				byte[] bbuf = new byte[count];
				int result = BaseStream.Read(bbuf, 0, count);
            
				if (result == -1) 
					return -1; 

				int off_i = index;
				for(int i = 0; i < result; i++, off_i++) 
				{
					int current= bbuf[i] & 0xFF;
					buffer[off_i] = (char)current;
				}
				return result;
			} 
			catch(Exception ex) 
			{
				throw new IOException(ex.Message);
			}
		}
	}

	#endregion

	#region "Stream class based on reader"

	internal class ReaderStream : Stream
	{
		private TextReader mReader;
		private char[] chBuffer = new char[4096];
		private byte[] byBuffer;
		private int iBufPos;
		private int iBufExtent;
		private bool bAtEnd;
		private bool bSevenBit;

		public ReaderStream(TextReader reader, bool sevenBit)
		{
			mReader = reader;
			bSevenBit = sevenBit;
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
				return false;
			}
		}

		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}

		public override long Length
		{
			get
			{
				return 0;
			}
		}

		public override long Position
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public override void Flush()
		{
			throw new NotSupportedException();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Close()
		{
			iBufPos = 0;
			iBufExtent = 0;
			mReader.Close();
		}

		public override int ReadByte()
		{
			int result;

			if (iBufPos >= iBufExtent) 
				FillBuffer();

			result = byBuffer[iBufPos];
			iBufPos++;
			return result;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			int bytesCopied = 0;
			bool atEnd = false;

			while ((count > 0) && !atEnd) 
			{
				if (iBufPos >= iBufExtent) 
					FillBuffer();
				if (iBufPos >= iBufExtent) 
					break;

				int copySize = Math.Min(count, iBufExtent - iBufPos);
				Array.Copy(byBuffer, iBufPos, buffer, offset, copySize);
				iBufPos += copySize;
				offset += copySize;
				count -= copySize;
				bytesCopied += copySize;
				atEnd = this.bAtEnd;
			}
			if (bytesCopied == 0) 
				bytesCopied = -1;
			return bytesCopied;
		}

		private void FillBuffer()
		{
			iBufPos = 0;
			iBufExtent = 0;
			int charsRead = mReader.Read(chBuffer, 0, chBuffer.Length);
			if (charsRead < chBuffer.Length) 
				bAtEnd = true;
			
			if (charsRead < 0) 
				return;

			if (bSevenBit)
				byBuffer = Encoding.ASCII.GetBytes(chBuffer, 0, charsRead);
			else
				byBuffer = Encoding.GetEncoding(1252).GetBytes(chBuffer, 0, charsRead);
			iBufExtent = byBuffer.Length;
		}
	}
	#endregion
#endif // SAFE
}
