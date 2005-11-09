using System;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	internal class MaxDBPacket : ByteArray
	{
		private int curPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBPacket(ByteArray array) : base(array.arrayData)
		{
		}

		public MaxDBPacket(ByteArray array, string dbname, int port) : base(array.arrayData)
		{
			// fill body
			writeByte(Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
			writeByte(Consts.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
			writeUInt16(ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
			writeByte(Consts.SQL_USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
			writeByte(Consts.RSQL_JAVA, HeaderOffset.END + ConnectPacketOffset.OSType);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
			writeUInt32(1024 * 32, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.PacketSize);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			writeASCII(dbname.Substring(0, Consts.DBNameSize), HeaderOffset.END + ConnectPacketOffset.ServerDB);
			writeASCII("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
			// fill out variable part
			writeByte(3, curPos++);
			writeByte(Consts.ARGID_REM_PID, curPos++);
			writeByte(0, curPos++);
			writeByte(0, curPos++);
			// add port number
			writeByte(4, curPos++);
			writeByte(Consts.ARGID_PORT_NO, curPos++);
			writeByte((byte)(port / 0xFF), curPos++);
			writeByte((byte)(port % 0xFF), curPos++);
			// add aknowledge flag
			writeByte(3, curPos++);
			writeByte(Consts.ARGID_ACKNOWLEDGE, curPos++);
			writeByte(0, curPos++);
			// add omit reply part flag
			writeByte(3, curPos++);
			writeByte(Consts.ARGID_OMIT_REPLY_PART, curPos++);
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
			const int packetMinLen = ConnectPacketOffset.MinSize;
			if (curPos < packetMinLen) curPos = packetMinLen;
			writeUInt16((ushort)(curPos - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
		}

		public int PacketLength
		{
			get
			{
				return curPos;
			}
		}

		public int PacketSendLength
		{
			set
			{
				writeInt32(value + HeaderOffset.END, HeaderOffset.ActSendLen);
				writeInt32(value + HeaderOffset.END, HeaderOffset.MaxSendLen);
			}
		}

		public int MaxDataLength
		{
			get
			{
				return readInt32(HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
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
					if (readByte(pos + 1) == Consts.ARGID_PORT_NO)
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
					if (readByte(pos + 1) == Consts.ARGID_AUTH_ALLOW)
						foreach(string authParam in Encoding.ASCII.GetString(data, pos + 2, len - 3).Split(','))
							if (authParam.ToUpper() == "SCRAMMD5")
								return true;
							else
								pos += len;
				}

				return false;
			}
		}
	}
}
