using System;
using System.IO;
using System.Net.Sockets;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for SocketIntf.
	/// </summary>
	public interface ISocketIntf
	{
		bool ReopenSocketAfterInfoPacket{get;}
		NetworkStream Stream{get;}

		ISocketIntf GetNewInstance();
		void Close();
	}

	public class SocketClass : TcpClient, ISocketIntf
	{
		private string m_host;
		private int m_port;

		public SocketClass(string host, int port) : base(host, port)
		{
			m_host = host;
			m_port = port;
		}

		#region SocketIntf Members

		public bool ReopenSocketAfterInfoPacket
		{
			get
			{
				return true;
			}
		}

		public NetworkStream Stream
		{
			get
			{
				return base.GetStream();
			}
		}

		ISocketIntf ISocketIntf.GetNewInstance()
		{
			return new SocketClass(m_host, m_port);
		}

		void ISocketIntf.Close()
		{
			Close();
		}

		#endregion
	}

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

	#region "Filter steam class reimplementation"

	public class StreamFilter : Stream
	{
		private int maxlength;

		private Stream ips;

		private int readlength = 0;
		private int markedlength = 0;
    
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
}
