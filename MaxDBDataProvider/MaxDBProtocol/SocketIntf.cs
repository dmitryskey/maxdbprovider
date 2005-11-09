using System;
using System.Net.Sockets;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for SocketIntf.
	/// </summary>
	internal interface ISocketIntf
	{
		bool ReopenSocketAfterInfoPacket{get;}
		NetworkStream Stream{get;}

		void Open();
		void Close();
	}

	internal class SocketClass : TcpClient, ISocketIntf
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

		public void Open()
		{
			base.Connect(m_host, m_port);
		}

		void ISocketIntf.Close()
		{
			base.Close();
		}

		#endregion

	}



}
