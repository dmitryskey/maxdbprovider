using System;
using System.Net.Sockets;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for MaxDBComm.
	/// </summary>
	public class MaxDBComm
	{
		private ISocketIntf m_socket;
		private string m_dbname;
		private int m_sender = 0;
		private int m_receiver = 0;
		private int m_maxSendLen = 0;
		private int swapMode = SwapMode.NotSwapped;

		public MaxDBComm(ISocketIntf s)
		{
			m_socket = s;
		}

		public void Connect(string dbname, int port)
		{
			try
			{
				MaxDBPacket request = new MaxDBPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], dbname, port, 1024 * 32, 0, 0, 0);
				request.FillHeader(RSQLTypes.INFO_REQUEST, m_sender, m_receiver, m_maxSendLen);
				request.FillPacketLength();
				request.PacketSendLength = request.PacketLength;
				m_socket.Stream.Write(request.arrayData, 0, request.PacketLength);

				MaxDBPacket reply = GetConnectReply();
				int returnCode = reply.ReturnCode;
				if (returnCode != 0)
				{
					Close();
					throw new Exception(CommError.ErrorText[returnCode]);
				}

				if (m_socket.ReopenSocketAfterInfoPacket)
				{
					Close();
					m_socket = m_socket.GetNewInstance();
				}

				m_dbname = dbname;
				//m_session = true;

				MaxDBPacket db_request = new MaxDBPacket(new byte[HeaderOffset.END + reply.MaxDataLength], dbname, port,
						reply.PacketSize, reply.MaxDataLength, reply.PacketSize, reply.MinReplySize);
				db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, m_sender, m_receiver, m_maxSendLen);
				db_request.FillPacketLength();
				db_request.PacketSendLength = db_request.PacketLength;
				m_socket.Stream.Write(db_request.arrayData, 0, db_request.PacketLength);

				returnCode = GetConnectReply().ReturnCode;
			}
			catch(Exception ex)
			{
				throw new MaxDBException("Can't connect to server.", ex);
			}
		}

		private MaxDBPacket GetConnectReply()
		{
			byte[] replyBuffer = new byte[HeaderOffset.END + ConnectPacketOffset.END];

			int len = m_socket.Stream.Read(replyBuffer, 0, replyBuffer.Length);
			if (len <= HeaderOffset.END)
				throw new Exception("Receive line down");

			swapMode = replyBuffer[HeaderOffset.END + 1];
			
			ByteArray replyPacket = new ByteArray(replyBuffer, swapMode == SwapMode.Swapped);

			int actLen = replyPacket.readInt32(HeaderOffset.ActSendLen);
			if (actLen < 0 || actLen > 500 * 1024)
				throw new Exception("Receive garbled reply");

			while (m_socket.Stream.DataAvailable)
				len += m_socket.Stream.Read(replyPacket.arrayData, len, actLen - len);

			m_sender = replyPacket.readInt32(HeaderOffset.SenderRef);

			return new MaxDBPacket(replyBuffer, swapMode == SwapMode.Swapped);
		}

		public void Close()
		{
			MaxDBPacket request = new MaxDBPacket(new byte[HeaderOffset.END], true);
			request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, m_sender, m_receiver, m_maxSendLen);
			request.PacketSendLength = 0;
			m_socket.Stream.Write(request.arrayData, 0, request.arrayData.Length);
			m_socket.Close();
		}
	}
}
