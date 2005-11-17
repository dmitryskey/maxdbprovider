using System;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	public struct ConnectPacketData
	{
		public string DBName;
		public int Port;
		public int MaxSegSize;
		public int MaxDataLen;
		public int PacketSize;
		public int MinReplySize;
	}

	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	public class MaxDBPacket : ByteArray
	{
		private int curPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBPacket(byte[] data, bool SwapMode) : base(data, SwapMode)
		{
		}

		public MaxDBPacket(byte[] data, ConnectPacketData connData) : base(data, false)
		{
			// fill body
			writeByte(Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
			writeByte(SwapMode.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
			writeUInt16(ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
			writeByte(SQLType.USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
			writeByte(Consts.RSQL_JAVA, HeaderOffset.END + ConnectPacketOffset.OSType);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
			writeInt32(connData.MaxSegSize, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			writeInt32(connData.MaxDataLen, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			writeInt32(connData.PacketSize, HeaderOffset.END + ConnectPacketOffset.PacketSize);
			writeInt32(connData.MinReplySize, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			if (connData.DBName.Length > Consts.DBNameSize)
				connData.DBName = connData.DBName.Substring(0, Consts.DBNameSize);
			writeASCII(connData.DBName.PadRight(Consts.DBNameSize, ' '), HeaderOffset.END + ConnectPacketOffset.ServerDB);
			writeASCII("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
			// fill out variable part
			writeByte(4, curPos++);
			writeByte(ArgType.REM_PID, curPos++);
			writeASCII("0", curPos++);
			writeByte(0, curPos++);
			// add port number
			writeByte(4, curPos++);
			writeByte(ArgType.PORT_NO, curPos++);
			writeUInt16((ushort)connData.Port, curPos);
			curPos += 2;
			// add aknowledge flag
			writeByte(3, curPos++);
			writeByte(ArgType.ACKNOWLEDGE, curPos++);
			writeByte(0, curPos++);
			// add omit reply part flag
			writeByte(3, curPos++);
			writeByte(ArgType.OMIT_REPLY_PART, curPos++);
			writeByte(1, curPos++);
		}

		public void FillHeader(byte msgClass, int senderRef, int receiverRef, int maxSendLen)
		{
			// fill out header part
			writeUInt32(0, HeaderOffset.ActSendLen);
			writeByte(3, HeaderOffset.ProtocolID);
			writeByte(msgClass, HeaderOffset.MessClass);
			writeByte(0, HeaderOffset.RTEFlags);
			writeByte(0, HeaderOffset.ResidualPackets);
			writeInt32(senderRef, HeaderOffset.SenderRef);
			writeInt32(receiverRef, HeaderOffset.ReceiverRef);
			writeUInt16(0, HeaderOffset.RTEReturnCode);
			writeUInt16(0, HeaderOffset.Filler);
			writeInt32(maxSendLen, HeaderOffset.MaxSendLen);
		}

		public void FillPacketLength()
		{
			const int packetMinLen = Consts.MinSize;
			if (curPos < packetMinLen) curPos = packetMinLen;
			writeUInt16((ushort)(curPos - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
		}

		public bool IsSwapped
		{
			get
			{
				return (readByte(HeaderOffset.END + ConnectPacketOffset.MessCode + 1) == SwapMode.Swapped);
			}
		}

		public int PacketLength
		{
			get
			{
				return curPos;
			}
		}

		public void SetSendLength(int val)
		{
			writeInt32(val, HeaderOffset.ActSendLen);
			writeInt32(val, HeaderOffset.MaxSendLen);
		}

		public int MaxSendLength
		{
			get
			{
				return readInt32(HeaderOffset.MaxSendLen);
			}
		}

		public int ActSendLength
		{
			get
			{
				return readInt32(HeaderOffset.ActSendLen);
			}
		}

		public int MaxDataLength
		{
			get
			{
				return readInt32(HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			}
		}

		public int PacketSender
		{
			get
			{
				return readInt32(HeaderOffset.SenderRef);
			}
		}

		public int MinReplySize
		{
			get
			{
				return readInt32(HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			}
		}

		public int PacketSize
		{
			get
			{
				return readInt32(HeaderOffset.END + ConnectPacketOffset.PacketSize);
			}
		}

		public int MaxSegmentSize
		{
			get
			{
				return readInt32(HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			}
		}

		public int PortNumber
		{
			get
			{
				int pos = HeaderOffset.END + ConnectPacketOffset.VarPart;
				while(pos < data.Length)
				{
					if (readByte(pos + 1) == ArgType.PORT_NO)
						return readByte(pos + 2) * 0xFF + readByte(pos + 3);//port number is always not swapped
					else
						pos += readByte(pos);
				}

				return Ports.Default;
			}
		}

		public bool IsAuthAllowed
		{
			get
			{
				int pos = HeaderOffset.END + ConnectPacketOffset.VarPart;
				while(pos < data.Length)
				{
					byte len = readByte(pos);

					if (len == 0)
						break;

					if (readByte(pos + 1) == ArgType.AUTH_ALLOW)
						foreach(string authParam in Encoding.ASCII.GetString(data, pos + 2, len - 3).Split(','))
							if (authParam.ToUpper() == "SCRAMMD5")
								return true;
					
					pos += len;
				}

				return false;
			}
		}

		public int ReturnCode
		{
			get
			{
				return readInt16(HeaderOffset.RTEReturnCode);
			}
		}
	}
}
