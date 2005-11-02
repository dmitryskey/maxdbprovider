using System;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	internal class MaxDBPacket : ByteArray
	{
		private int curPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBPacket(byte[] data, string host, int port, byte msg) : base(data)
		{
			// fill out header part
			writeUInt32(0, HeaderOffset.ActSendLen);
			writeByte(3, HeaderOffset.ProtocolID);
			writeByte(msg, HeaderOffset.MessClass);
			writeByte(0, HeaderOffset.RTEFlags);
			writeByte(0, HeaderOffset.ResidualPackets);
			writeUInt32(senderRef, HeaderOffset.SenderRef);
			writeUInt32(receiverRef, HeaderOffset.ReceiverRef);
			writeUInt16(0, HeaderOffset.RTEReturnCode);
			writeUInt16(0, HeaderOffset.Filler);
			writeUInt32(maxSendLen, HeaderOffset.MaxSendLen);
			// fill body
			writeByte((byte)Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
			writeByte ((byte)Consts.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
			writeUInt16((ushort)ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
			writeByte((byte)Consts.SQL_USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
			writeByte((byte)Consts.RSQL_JAVA, HeaderOffset.END + ConnectPacketOffset.OSType);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
			writeByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
			writeUInt32(1024 * 32, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.PacketSize);
			writeUInt32(0, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			writeASCII(host.Substring(0, (int)Consts.DBNameSize), HeaderOffset.END + ConnectPacketOffset.ServerDB);
			writeASCII("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
			// fill out variable part
			writeByte(3, curPos++);
			writeByte((byte)Consts.ARGID_REM_PID, curPos++);
			writeByte(0, curPos++);
			writeByte(0, curPos++);
			// add port number
			writeByte(4, curPos++);
			writeByte((byte)Consts.ARGID_PORT_NO, curPos++);
			writeByte((byte)(port / 0xFF), curPos++);
			writeByte((byte)(port % 0xFF), curPos++);
			// add aknowledge flag
			writeByte(3, curPos++);
			writeByte((byte)Consts.ARGID_ACKNOWLEDGE, curPos++);
			writeByte(0, curPos++);
			// add omit reply part flag
			writeByte(3, curPos++);
			writeByte((byte)Consts.ARGID_OMIT_REPLY_PART, curPos++);
			writeByte(1, curPos++);
		}
	}
}
