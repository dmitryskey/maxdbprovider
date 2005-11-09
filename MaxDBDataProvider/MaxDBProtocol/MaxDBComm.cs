using System;
using System.Net.Sockets;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for MaxDBComm.
	/// </summary>
	internal class MaxDBComm
	{
		private ISocketIntf m_socket;
		private string m_dbname;
		private int m_sender = 0;
		private int m_receiver = 0;
		private int m_maxSendLen = 0;
		private int swapMode = Consts.NotSwapped;

		public MaxDBComm(ISocketIntf s)
		{
			m_socket = s;
		}

		public void Connect(string dbname, int port)
		{
			try
			{
				MaxDBPacket request = new MaxDBPacket(new ByteArray(HeaderOffset.END + ConnectPacketOffset.END), dbname, port);
				request.FillHeader(RSQLTypes.INFO_REQUEST, m_sender, m_receiver, m_maxSendLen);
				request.FillPacketLength();
				request.PacketSendLength = ConnectPacketOffset.END;
				m_socket.Stream.Write(request.arrayData, 0, request.arrayData.Length);

				int returnCode = GetConnectReply().readInt16(HeaderOffset.RTEReturnCode);
				if (returnCode != 0)
				{
					Close();
					throw new Exception(CommError.ErrorText[returnCode]);
				}

				if (m_socket.ReopenSocketAfterInfoPacket)
				{
					Close();
					m_socket.Open();
				}

				m_dbname = dbname;
				//m_session = true;

				MaxDBPacket db_request = new MaxDBPacket(new ByteArray(HeaderOffset.END + request.MaxDataLength), dbname, port);
				request.FillPacketLength();
				request.PacketSendLength = ConnectPacketOffset.END;
				m_socket.Stream.Write(request.arrayData, 0, request.arrayData.Length);

				GetConnectReply();
			}
			catch(Exception ex)
			{
				throw new MaxDBException("Can't connect to server.", ex);
			}
		}

		private ByteArray GetConnectReply()
		{
			ByteArray replyPacket = new ByteArray(HeaderOffset.END + ConnectPacketOffset.END);

			int len = m_socket.Stream.Read(replyPacket.arrayData, 0, replyPacket.arrayData.Length);
			if (len <= HeaderOffset.END)
				throw new Exception("Receive line down");

			swapMode = replyPacket.readByte(HeaderOffset.END + 1);
			int actLen = replyPacket.readInt32(HeaderOffset.ActSendLen);
			if (actLen < 0 || actLen > 500 * 1024)
				throw new Exception("Receive garbled reply");

			while (m_socket.Stream.DataAvailable)
				len += m_socket.Stream.Read(replyPacket.arrayData, len, actLen - len);

			m_sender = replyPacket.readInt32(HeaderOffset.SenderRef);

			return replyPacket;
		}

		public void Close()
		{
			MaxDBPacket request = new MaxDBPacket(new ByteArray(HeaderOffset.END));
			request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, m_sender, m_receiver, m_maxSendLen);
			request.PacketSendLength = 0;
			m_socket.Stream.Write(request.arrayData, 0, request.arrayData.Length);
			m_socket.Close();
		}
	}
}
