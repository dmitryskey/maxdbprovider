using System;
using System.Net.Sockets;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE
	/// <summary>
	/// Summary description for MaxDBComm.
	/// </summary>
	internal class MaxDBComm
	{
		private ISocketIntf m_socket;
		private string m_dbname;
		private int m_port;
		private int m_sender = 0;
		private int m_receiver = 0;
		private int m_maxSendLen = 0;
		private bool m_isauthallowed = false;
		private int m_maxcmdsize = 0;
		private bool m_session = false;

		public bool IsAuthAllowed
		{
			get
			{
				return m_isauthallowed;
			}
		}

		public int MaxCmdSize
		{
			get
			{
				return m_maxcmdsize;
			}
		}

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
				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
				request.FillHeader(RSQLTypes.INFO_REQUEST, m_sender, m_receiver, m_maxSendLen);
				request.FillPacketLength();
				request.SetSendLength(request.PacketLength);
				m_socket.Stream.Write(request.arrayData, 0, request.PacketLength);

				MaxDBConnectPacket reply = GetConnectReply();
				int returnCode = reply.ReturnCode;
				if (returnCode != 0)
				{
					Close();
					throw new CommunicationException(returnCode);
				}

				if (dbname.Trim().ToUpper() != reply.ClientDB.Trim().ToUpper())
				{
					Close();
					throw new CommunicationException(RTEReturnCodes.SQLSERVER_DB_UNKNOWN);
				}

				if (m_socket.ReopenSocketAfterInfoPacket)
				{
					ISocketIntf new_socket = m_socket.Clone();
					Close();
					m_socket = new_socket;
				}

				m_session = true;
				m_dbname = dbname;
				m_port = port;

				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegSize = reply.PacketSize;
				connData.MaxDataLen = reply.MaxDataLength;
				connData.PacketSize = reply.PacketSize;
				connData.MinReplySize = reply.MinReplySize;

				MaxDBConnectPacket db_request = new MaxDBConnectPacket(new byte[HeaderOffset.END + reply.MaxDataLength], connData);
				db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, m_sender, m_receiver, m_maxSendLen);
				db_request.FillPacketLength();
				db_request.SetSendLength(db_request.PacketLength);
				m_socket.Stream.Write(db_request.arrayData, 0, db_request.PacketLength);

				reply = GetConnectReply();
				m_isauthallowed = reply.IsAuthAllowed;
				m_maxcmdsize = reply.MaxDataLength - reply.MinReplySize;
			}
			catch(Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_HOST_CONNECT, m_socket.Host, m_socket.Port), ex);
			}
		}

		public void Reconnect()
		{
			ISocketIntf new_socket = m_socket.Clone();
			Close();
			m_socket = new_socket;
			if (m_session)
				Connect(m_dbname, m_port);
			else
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ADMIN_RECONNECT, CommError.ErrorText[RTEReturnCodes.SQLTIMEOUT]));
		}

		private MaxDBConnectPacket GetConnectReply()
		{
			byte[] replyBuffer = new byte[HeaderOffset.END + ConnectPacketOffset.END];

			int len = m_socket.Stream.Read(replyBuffer, 0, replyBuffer.Length);
			if (len <= HeaderOffset.END)
				throw new Exception(MaxDBMessages.Extract(MaxDBMessages.ERROR_RECV_CONNECT));
			
			MaxDBConnectPacket replyPacket = new MaxDBConnectPacket(replyBuffer, 
				replyBuffer[HeaderOffset.END + ConnectPacketOffset.MessCode + 1] == SwapMode.Swapped);

			int actLen = replyPacket.ActSendLength;
			if (actLen < 0 || actLen > 500 * 1024)
				throw new Exception(MaxDBMessages.Extract(MaxDBMessages.ERROR_REPLY_GARBLED));

			while(m_socket.DataAvailable)
			{
				if (len < actLen)
					len += m_socket.Stream.Read(replyPacket.arrayData, len, actLen - len);
				else
					throw new Exception(MaxDBMessages.Extract(MaxDBMessages.ERROR_CHUNKOVERFLOW, actLen, len, replyBuffer.Length));
			};

			m_sender = replyPacket.PacketSender;

			return replyPacket;
		}

		public void Close()
		{
			if (m_socket.Stream != null)
			{
				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END]);
				request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, m_sender, m_receiver, m_maxSendLen);
				request.SetSendLength(0);
				m_socket.Stream.Write(request.arrayData, 0, request.Length);
				m_socket.Close();
			}
		}

		public void Cancel()
		{
			try
			{
				if (m_socket.Stream != null)
				{
					MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END]);
					request.FillHeader(RSQLTypes.USER_CANCEL_REQUEST, m_sender, m_receiver, m_maxSendLen);
					request.WriteInt32(m_sender, HeaderOffset.ReceiverRef);
					request.Close();
					request.SetSendLength(request.Length);
					m_socket.Stream.Write(request.arrayData, 0, request.Length);
					m_socket.Close();
				}
			}
			catch(Exception ex)
			{
				throw new MaxDBException(ex.Message);
			}
		}

		public MaxDBReplyPacket Exec(MaxDBRequestPacket userPacket, int len)
		{
			try
			{
				MaxDBPacket rawPacket = new MaxDBPacket(userPacket.arrayData, 0, userPacket.Swapped); 
				rawPacket.FillHeader(RSQLTypes.USER_DATA_REQUEST, m_sender, m_receiver, m_maxSendLen);
				rawPacket.SetSendLength(len + HeaderOffset.END);
				m_socket.Stream.Write(rawPacket.arrayData, 0, len + HeaderOffset.END);
				byte[] headerBuf = new byte[HeaderOffset.END];

				int headerLength = m_socket.Stream.Read(headerBuf, 0, headerBuf.Length);

				if (headerLength != HeaderOffset.END)
					throw new CommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);

				MaxDBConnectPacket header = new MaxDBConnectPacket(headerBuf, true);
					//headerBuf[HeaderOffset.END + PacketHeaderOffset.MessSwap] == SwapMode.Swapped); //???
				int returnCode = header.ReturnCode;
				if (returnCode != 0)
					throw new CommunicationException(returnCode);

				byte[] packetBuf = new byte[header.MaxSendLength - HeaderOffset.END];
				int replyLen = HeaderOffset.END;
				
				while(m_socket.DataAvailable)
				{
					if (replyLen < header.ActSendLength)
						replyLen += m_socket.Stream.Read(packetBuf, replyLen - HeaderOffset.END, header.ActSendLength - replyLen);
					else
						throw new Exception(MaxDBMessages.Extract(MaxDBMessages.ERROR_CHUNKOVERFLOW, 
							header.ActSendLength, replyLen, packetBuf.Length + HeaderOffset.END));
	
				};

				return new MaxDBReplyPacket(packetBuf);
			}
			catch(Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED), ex);
			}
		}
	}
#endif
}
