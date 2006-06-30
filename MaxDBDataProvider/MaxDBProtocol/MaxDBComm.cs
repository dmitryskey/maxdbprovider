//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.Net.Sockets;
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE
	/// <summary>
	/// Summary description for MaxDBComm.
	/// </summary>
	internal class MaxDBComm
	{
		private IMaxDBSocket mSocket;
		private string strDbName;
		private int iPort;
		private int iSender;
		private bool bIsAuthAllowed;
		private int iMaxCmdSize;
		private bool bSession;
		private TimeSpan ts = new TimeSpan(1);

		public bool IsAuthAllowed
		{
			get
			{
				return bIsAuthAllowed;
			}
		}

		public int MaxCmdSize
		{
			get
			{
				return iMaxCmdSize;
			}
		}

		public MaxDBComm(IMaxDBSocket s)
		{
			mSocket = s;
		}

		public void Connect(string dbname, int port)
		{
			try
			{
				ConnectPacketData connData = new ConnectPacketData();
				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegmentSize = 1024 * 32;
				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
				request.FillHeader(RSQLTypes.INFO_REQUEST, iSender);
				request.FillPacketLength();
				request.SetSendLength(request.PacketLength);
				mSocket.Stream.Write(request.GetArrayData(), 0, request.PacketLength);

				MaxDBConnectPacket reply = GetConnectReply();
				int returnCode = reply.ReturnCode;
				if (returnCode != 0)
				{
					Close();
					throw new MaxDBCommunicationException(returnCode);
				}

				if (string.Compare(dbname.Trim(), reply.ClientDB.Trim(), true, CultureInfo.InvariantCulture) != 0)
				{
					Close();
					throw new MaxDBCommunicationException(RTEReturnCodes.SQLSERVER_DB_UNKNOWN);
				}

				if (mSocket.ReopenSocketAfterInfoPacket)
				{
					IMaxDBSocket new_socket = mSocket.Clone();
					Close();
					mSocket = new_socket;
				}

				bSession = true;
				strDbName = dbname;
				iPort = port;

				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegmentSize = reply.PacketSize;
				connData.MaxDataLen = reply.MaxDataLength;
				connData.PacketSize = reply.PacketSize;
				connData.MinReplySize = reply.MinReplySize;

				MaxDBConnectPacket db_request = new MaxDBConnectPacket(new byte[HeaderOffset.END + reply.MaxDataLength], connData);
				db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, iSender);
				db_request.FillPacketLength();
				db_request.SetSendLength(db_request.PacketLength);
				mSocket.Stream.Write(db_request.GetArrayData(), 0, db_request.PacketLength);

				reply = GetConnectReply();
				bIsAuthAllowed = reply.IsAuthAllowed;
				iMaxCmdSize = reply.MaxDataLength - reply.MinReplySize;
			}
			catch(Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT, mSocket.Host, mSocket.Port), ex);
			}
		}

		public void Reconnect()
		{
			IMaxDBSocket new_socket = mSocket.Clone();
			Close();
			mSocket = new_socket;
			if (bSession)
				Connect(strDbName, iPort);
			else
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADMIN_RECONNECT, CommError.ErrorText[RTEReturnCodes.SQLTIMEOUT]));
		}

		private MaxDBConnectPacket GetConnectReply()
		{
			byte[] replyBuffer = new byte[HeaderOffset.END + ConnectPacketOffset.END];

			int len = mSocket.Stream.Read(replyBuffer, 0, replyBuffer.Length);
			if (len <= HeaderOffset.END)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.RECV_CONNECT));
			
			MaxDBConnectPacket replyPacket = new MaxDBConnectPacket(replyBuffer, 
				replyBuffer[HeaderOffset.END + ConnectPacketOffset.MessCode + 1] == SwapMode.Swapped);

			int actLen = replyPacket.ActSendLength;
			if (actLen < 0 || actLen > 500 * 1024)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.REPLY_GARBLED));

			while(mSocket.DataAvailable)
			{
				if (len < actLen)
					len += mSocket.Stream.Read(replyPacket.GetArrayData(), len, actLen - len);
				else
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW, actLen, len, replyBuffer.Length));
			};

			iSender = replyPacket.PacketSender;

			return replyPacket;
		}

		public void Close()
		{
			if (mSocket.Stream != null)
			{
				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END]);
				request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, iSender);
				request.SetSendLength(HeaderOffset.END);
				mSocket.Stream.Write(request.GetArrayData(), 0, request.Length);
				mSocket.Close();
			}
		}

		public void Cancel()
		{
			try
			{
				if (mSocket.Stream != null)
				{
					IMaxDBSocket cancel_socket = mSocket.Clone();
					ConnectPacketData connData = new ConnectPacketData();
					connData.DBName = strDbName;
					connData.Port = iPort;
					connData.MaxSegmentSize = 1024 * 32;
					MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
					request.FillHeader(RSQLTypes.USER_CANCEL_REQUEST, iSender);
					request.WriteInt32(iSender, HeaderOffset.ReceiverRef);
					request.SetSendLength(request.PacketLength);
					request.Offset = HeaderOffset.END;
					request.Close();
					cancel_socket.Stream.Write(request.GetArrayData(), 0, request.PacketLength);
					cancel_socket.Close();
				}
			}
			catch(Exception ex)
			{
				throw new MaxDBException(ex.Message);
			}
		}

		public MaxDBReplyPacket Execute(MaxDBRequestPacket userPacket, int len)
		{
			MaxDBPacket rawPacket = new MaxDBPacket(userPacket.GetArrayData(), 0, userPacket.Swapped); 
			rawPacket.FillHeader(RSQLTypes.USER_DATA_REQUEST, iSender);
			rawPacket.SetSendLength(len + HeaderOffset.END);

			mSocket.Stream.Write(rawPacket.GetArrayData(), 0, len + HeaderOffset.END);
			byte[] headerBuf = new byte[HeaderOffset.END];

			int headerLength = mSocket.Stream.Read(headerBuf, 0, headerBuf.Length);

			if (headerLength != HeaderOffset.END)
				throw new MaxDBCommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);

			MaxDBConnectPacket header = new MaxDBConnectPacket(headerBuf, true);
				//headerBuf[HeaderOffset.END + PacketHeaderOffset.MessSwap] == SwapMode.Swapped); //???
			int returnCode = header.ReturnCode;
			if (returnCode != 0)
				throw new MaxDBCommunicationException(returnCode);

			byte[] packetBuf = new byte[header.MaxSendLength - HeaderOffset.END];
			int replyLen = HeaderOffset.END;
			
			while(mSocket.DataAvailable)
			{
				if (replyLen < header.ActSendLength)
					replyLen += mSocket.Stream.Read(packetBuf, replyLen - HeaderOffset.END, header.ActSendLength - replyLen);
				else
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW, 
						header.ActSendLength, replyLen, packetBuf.Length + HeaderOffset.END));

				if (!mSocket.DataAvailable) System.Threading.Thread.Sleep(ts); //wait for end of data
			};

			return new MaxDBReplyPacket(packetBuf);
		}
    }
#endif // SAFE
}
