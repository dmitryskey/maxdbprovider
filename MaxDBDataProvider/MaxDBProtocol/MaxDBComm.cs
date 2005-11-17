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
				ConnectPacketData connData = new ConnectPacketData();
				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegSize = 1024 * 32;
				MaxDBPacket request = new MaxDBPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
				request.FillHeader(RSQLTypes.INFO_REQUEST, m_sender, m_receiver, m_maxSendLen);
				request.FillPacketLength();
				request.SetSendLength(request.PacketLength);
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

				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegSize = reply.PacketSize;
				connData.MaxDataLen = reply.MaxDataLength;
				connData.PacketSize = reply.PacketSize;
				connData.MinReplySize = reply.MinReplySize;

				MaxDBPacket db_request = new MaxDBPacket(new byte[HeaderOffset.END + reply.MaxDataLength], connData);
				db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, m_sender, m_receiver, m_maxSendLen);
				db_request.FillPacketLength();
				db_request.SetSendLength(db_request.PacketLength);
				m_socket.Stream.Write(db_request.arrayData, 0, db_request.PacketLength);

				GetConnectReply();
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

			swapMode = replyBuffer[HeaderOffset.END + ConnectPacketOffset.MessCode + 1];
			
			MaxDBPacket replyPacket = new MaxDBPacket(replyBuffer, swapMode == SwapMode.Swapped);

			int actLen = replyPacket.ActSendLength;
			if (actLen < 0 || actLen > 500 * 1024)
				throw new Exception("Receive garbled reply");

			while (m_socket.Stream.DataAvailable)
			{
				if (len < actLen)
					len += m_socket.Stream.Read(replyPacket.arrayData, len, actLen - len);
				else
					throw new Exception("Chunk overflow in read");
			}

			m_sender = replyPacket.PacketSender;

			bool dd = replyPacket.IsAuthAllowed;

			return replyPacket;
		}

		public void Close()
		{
			MaxDBPacket request = new MaxDBPacket(new byte[HeaderOffset.END], true);
			request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, m_sender, m_receiver, m_maxSendLen);
			request.SetSendLength(0);
			m_socket.Stream.Write(request.arrayData, 0, request.arrayData.Length);
			m_socket.Close();
		}

		public MaxDBPacket Exec(MaxDBPacket userPacket, int len)
		{
			userPacket.FillHeader(RSQLTypes.USER_DATA_REQUEST, m_sender, m_receiver, m_maxSendLen);
			m_socket.Stream.Write(userPacket.arrayData, 0, len);
			byte[] headerBuf = new byte[HeaderOffset.END];

			if (m_socket.Stream.Read(headerBuf, 0, headerBuf.Length) < HeaderOffset.END)
				throw new Exception("Receive line down");

			MaxDBPacket header = new MaxDBPacket(headerBuf, true);
			int returnCode = header.ReturnCode;
			if (returnCode != 0)
				throw new Exception(CommError.ErrorText[returnCode]);

			byte[] packetBuf = new byte[HeaderOffset.END + header.MaxSendLength];
			headerBuf.CopyTo(packetBuf, 0);
			int replyLen = HeaderOffset.END;
			while(m_socket.Stream.DataAvailable)
			{
				if (replyLen < header.ActSendLength)
					replyLen += m_socket.Stream.Read(packetBuf, replyLen, header.ActSendLength - replyLen);
				else
					throw new Exception("Chunk overflow in read");
			}

			return new MaxDBPacket(packetBuf, true);
		}
	}
}
