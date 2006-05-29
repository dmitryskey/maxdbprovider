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
#endif

namespace MaxDBDataProvider.Utils
{
	/// <summary>
	/// Interface to support tcp and ssl connection.
	/// </summary>
	public interface ISocketIntf
	{
		bool ReopenSocketAfterInfoPacket{get;}
		bool DataAvailable{get;}
		Stream Stream{get;}
		string Host{get;}
		int Port{get;}

		ISocketIntf Clone();
		void Close();
	}

	public class SocketClass : ISocketIntf
	{
		protected string m_host;
        protected int m_port;
        protected int m_timeout;
        protected TcpClient m_client;

		public SocketClass(string host, int port, int timeout, bool checksocket) 
		{
			m_host = host;
			m_port = port;
			m_timeout = timeout;
			
			try
			{
				if (checksocket && timeout > 0)
				{
					Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
					bool connect_succeeded = false;
					IPHostEntry entries = Dns.GetHostByName(host);
					foreach(IPAddress ipAddr in entries.AddressList)
					{
						sock.Blocking = false;
						try 
						{
							sock.Connect(new IPEndPoint(ipAddr, m_port));
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

						Socket.Select(null, checkWrite, checkError, m_timeout * 1000000);
						sock.Blocking = true;
						sock.Close();

						if (checkWrite.Count > 0)
						{
							connect_succeeded = true;
							break;
						}
					}

					if (!connect_succeeded)
						throw new Exception();
				}

				m_client = new TcpClient(host, port);
				m_client.ReceiveTimeout = m_timeout;
			}
			catch(Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_HOST_CONNECT, m_host, m_port), ex);
			}
		}

		#region SocketIntf Members

		bool ISocketIntf.ReopenSocketAfterInfoPacket
		{
			get
			{
				return true;
			}
		}

		bool ISocketIntf.DataAvailable
		{
			get
			{
				if (m_client != null)
					return m_client.GetStream().DataAvailable;
				else
					return false;
			}
		}

		Stream ISocketIntf.Stream
		{
			get
			{
				if (m_client != null)
					return m_client.GetStream();
				else
					return null;
			}
		}

		string ISocketIntf.Host
		{
			get
			{
				return m_host;
			}
		}

		int ISocketIntf.Port
		{
			get
			{
				return m_port;
			}
		}

		ISocketIntf ISocketIntf.Clone()
		{
			return new SocketClass(m_host, m_port, m_timeout, false);
		}

		void ISocketIntf.Close()
		{
			m_client.Close();
			m_client = null;
		}

		#endregion
	}

#if NET20

    public class SslSocketClass : SocketClass, ISocketIntf
    {
        SslStream m_sslStream;
        private string m_certificateError;
        private string m_server;

        public SslSocketClass(string host, int port, int timeout, bool checksocket, string server)
            : base(host, port, timeout, checksocket)
        {
            m_server = server;
            m_sslStream = new SslStream(m_client.GetStream(),
                false,
                new RemoteCertificateValidationCallback(ValidateServerCertificate),
                null
                );
            try
            {
                m_sslStream.AuthenticateAsClient(server);
            }
            catch (AuthenticationException e)
            {
                throw new MaxDBException(m_certificateError + ". " + e.Message);
            }
        }

        // The following method is invoked by the RemoteCertificateValidationDelegate.
        private bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            m_certificateError = string.Empty;

            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            m_certificateError = MaxDBMessages.Extract(MaxDBMessages.ERROR_SSL_CERTIFICATE, sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }

        Stream ISocketIntf.Stream
        {
            get
            {
                return m_sslStream;
            }
        }

        ISocketIntf ISocketIntf.Clone()
        {
            return new SslSocketClass(m_host, m_port, m_timeout, false, m_server);
        }

        void ISocketIntf.Close()
        {
            m_sslStream.Close();
            m_sslStream = null;
            m_client.Close();
            m_client = null;
        }
    }

#endif

	#region "Join stream class reimplementation"

	public class JoinStream : Stream
	{
		protected Stream[] streams;
		protected Stream currentStream;
		protected int currentIndex = -1;

		public JoinStream(Stream[] streams)
		{
			this.streams = streams;
			nextStream();
		}

		protected void nextStream()
		{
			currentIndex++;
			if (currentIndex >= streams.Length) 
				currentStream = null;
			else 
				currentStream = streams[currentIndex];
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			int result = 0;
			int chunkLen;

			while (currentStream != null && count > 0) 
			{
				chunkLen = currentStream.Read(buffer, offset, count);
				if (chunkLen == 0) 
					nextStream();
				else 
				{
					offset += chunkLen;
					count -= chunkLen;
					result += chunkLen;
				}
			}
			if (result == 0 && currentStream == null) 
				result = 0;
			return result;
		}

		public override void Close()
		{
			for (int i = currentIndex; i < streams.Length; i++) 
			{
				try 
				{
					if (streams[i] != null) 
						streams[i].Close();
				}
				catch 
				{
					// ignore
				}
			}
		}

		public override void Flush()
		{
			for (int i = currentIndex; i < streams.Length; i++) 
			{
				try 
				{
					if (streams[i] != null) 
						streams[i].Flush();
				}
				catch 
				{
					// ignore
				}
			}
		}

		public override long Length
		{
			get
			{
				long length = 0;
				for (int i = currentIndex; i < streams.Length; i++) 
				{
					try 
					{
						if (streams[i] != null) 
							length += streams[i].Length;
					}
					catch 
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

	public class JoinTextReader : TextReader
	{
		protected TextReader[] readers;
		protected TextReader currentReader;
		protected int currentIndex = -1;
    
		public JoinTextReader(TextReader[] readers)
		{
			this.readers = readers;
			this.NextReader();
		}
    

		protected void NextReader()
		{
			currentIndex++;
			if (currentIndex >= readers.Length)
				currentReader = null;
			else 
				currentReader = readers[currentIndex];
		}
    
		public override int Read(char[] b, int off, int len)
		{
			int result = 0;
			int chunkLen;
        
			while (currentReader != null && len > 0) 
			{
				chunkLen = currentReader.Read(b, off, len);
				if (chunkLen == -1) 
					NextReader();
				else 
				{
					off += chunkLen;
					len -= chunkLen;
					result += chunkLen;
				}
			}
			if (result == 0 && currentReader == null) 
				result = -1;
      
			return result;
		}
    
		public override int Read()    
		{
			int result = 0;
			while (currentReader != null) 
			{
				result = currentReader.Read();
				if (result == -1)
					NextReader();
			}
			if (currentReader == null) 
				result = -1;
			return result;
		}
    
		public override void Close()
		{
			foreach (TextReader reader in readers) 
			{
				try 
				{
					if (reader != null) 
						reader.Close();
				}
				catch 
				{
					// ignore
				}
			}
		}
	}
	#endregion

	#region "Stream filter class reimplementation"

	public class StreamFilter : Stream
	{
		private int maxlength;

		private Stream ips;

		private int readlength = 0;
    
		public StreamFilter(Stream ips, int length)
		{
			this.maxlength = length;
			this.ips = ips;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			if (buffer == null) 
				throw new InvalidOperationException();
			
			if (offset < 0 || offset > buffer.Length || count < 0	|| (offset + count) > buffer.Length || (offset + count) < 0) 
				throw new OverflowException();

			if (readlength >= maxlength) 
				return -1;

			if (readlength + count > maxlength) 
				count = maxlength - readlength;

			if (count <= 0) 
				return 0;

			count = ips.Read(buffer, offset, count);
			readlength += count;
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
			ips.Flush();
		}

		public override long Length
		{
			get
			{
				return ips.Length;
			}
		}
	}

	#endregion

	#region "Text reader filter class reimplementation"

	public class TextReaderFilter : TextReader 
	{
		private int maxlength;
		private TextReader ips;
		private int readlength = 0;
	
		public TextReaderFilter(TextReader ips, int length) 
		{
			this.maxlength = length;
			this.ips = ips;
		}

		public override int Read()  
		{
			if (maxlength <= readlength++)
				return -1;
			else
				return ips.Read();
		}

		public override int Read(char[] b, int off, int len) 
		{
			if (b == null) 
				throw new NullReferenceException();

			if (off < 0 || off > b.Length || len < 0 || (off + len) > b.Length || (off + len) < 0) 
				throw new IndexOutOfRangeException();

			if (readlength >= maxlength) 
				return -1;
			if (readlength + len > maxlength) 
				len = maxlength - readlength;
			if (len <= 0) 
				return 0;

			len = ips.Read(b, off, len);
			readlength += len;
			return len;
		}

		public override void Close()
		{
			ips.Close();
		}
	}

	#endregion

	#region "Raw byte stream reader class implementation"

	public class RawByteReader : StreamReader
	{
		public RawByteReader(Stream stream) : base(stream)
		{
		}

		public override int Read(char[] cbuf, int off, int len)
		{
			try 
			{
				byte[] bbuf = new byte[len];
				int result = BaseStream.Read(bbuf, 0, len);
            
				if (result == -1) 
					return -1; 

				int off_i = off;
				for(int i = 0; i < result; i++, off_i++) 
				{
					int current= bbuf[i] & 0xFF;
					cbuf[off_i] = (char)current;
				}
				return result;
			} 
			catch(IOException ioEx) 
			{
				throw ioEx;
			} 
			catch(Exception ex) 
			{
				throw new IOException(ex.Message);
			}
		}
	}

	#endregion

	#region "Stream class based on reader"

	public class ReaderStream : Stream
	{
		private TextReader reader;
		private char[] charBuf = new char [4096];
		private byte [] byteBuf;
		private int bufPos = 0;
		private int bufExtent = 0;
		private bool atEnd = false;
		private bool sevenbit = false;

		public ReaderStream(TextReader reader, bool sevenbit)
		{
			this.reader = reader;
			this.sevenbit = sevenbit;
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
			bufPos = 0;
			bufExtent = 0;
			reader.Close();
		}

		public override int ReadByte()
		{
			int result;

			if (bufPos >= bufExtent) 
				FillBuffer();

			result = byteBuf[bufPos];
			bufPos++;
			return result;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			int bytesCopied = 0;
			bool atEnd = false;

			while ((count > 0) && !atEnd) 
			{
				if (bufPos >= bufExtent) 
					FillBuffer();
				if (bufPos >= bufExtent) 
					break;

				int copySize = Math.Min(count, bufExtent - bufPos);
				Array.Copy(byteBuf, bufPos, buffer, offset, copySize);
				bufPos += copySize;
				offset += copySize;
				count -= copySize;
				bytesCopied += copySize;
				atEnd = this.atEnd;
			}
			if (bytesCopied == 0) 
				bytesCopied = -1;
			return bytesCopied;
		}

		private void FillBuffer()
		{
			bufPos = 0;
			bufExtent = 0;
			int charsRead = reader.Read(charBuf, 0, charBuf.Length);
			if (charsRead < charBuf.Length) 
				atEnd = true;
			
			if (charsRead < 0) 
				return;

			if (sevenbit)
				byteBuf = Encoding.ASCII.GetBytes(charBuf, 0, charsRead);
			else
				byteBuf = Encoding.GetEncoding(1252).GetBytes(charBuf, 0, charsRead);
			bufExtent = byteBuf.Length;
		}
	}
	#endregion
}
