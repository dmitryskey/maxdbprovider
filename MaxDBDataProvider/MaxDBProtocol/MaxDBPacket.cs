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
	public abstract class MaxDBPacket : ByteArray
	{
		public MaxDBPacket(byte[] data, bool SwapMode) : base(data, SwapMode)
		{
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

		public int PacketSender
		{
			get
			{
				return readInt32(HeaderOffset.SenderRef);
			}
		}

		public int ReturnCode
		{
			get
			{
				return readInt16(HeaderOffset.RTEReturnCode);
			}
		}

		protected int alignSize(int val)
		{
			if (val % Consts.AlignValue != 0)
				return val + (Consts.AlignValue - val % Consts.AlignValue);
			else
				return val;
		}
	}

	public class MaxDBConnectPacket : MaxDBPacket
	{
		private int m_curPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBConnectPacket(byte[] data, bool SwapMode) : base(data, SwapMode)
		{
		}

		public MaxDBConnectPacket(byte[] data, ConnectPacketData connData) : base(data, false)
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
			writeByte(4, m_curPos++);
			writeByte(ArgType.REM_PID, m_curPos++);
			writeASCII("0", m_curPos++);
			writeByte(0, m_curPos++);
			// add port number
			writeByte(4, m_curPos++);
			writeByte(ArgType.PORT_NO, m_curPos++);
			writeUInt16((ushort)connData.Port, m_curPos);
			m_curPos += 2;
			// add aknowledge flag
			writeByte(3, m_curPos++);
			writeByte(ArgType.ACKNOWLEDGE, m_curPos++);
			writeByte(0, m_curPos++);
			// add omit reply part flag
			writeByte(3, m_curPos++);
			writeByte(ArgType.OMIT_REPLY_PART, m_curPos++);
			writeByte(1, m_curPos++);
		}

		public void FillPacketLength()
		{
			const int packetMinLen = Consts.MinSize;
			if (m_curPos < packetMinLen) m_curPos = packetMinLen;
			writeUInt16((ushort)(m_curPos - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
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
				return m_curPos;
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
							if (authParam.ToUpper() == Crypt.ScramMD5Name)
								return true;
					
					pos += len;
				}

				return false;
			}
		}
	}

	public class MaxDBRequestPacket : MaxDBPacket
	{
		private int m_length = PacketHeaderOffset.Segment;
		private short m_segments = 0;
		private int m_partOffset = -1, m_partLength = -1, m_segOffset = -1, m_segLength = -1;
		private short m_partArgs = -1, m_segParts = -1;
		private byte currentSqlMode = SqlMode.SessionSqlmode;
		private int replyReserve;
		private int maxNumberOfSeg = Consts.defaultmaxNumberOfSegm;

		public MaxDBRequestPacket(byte[] data, string appID, string appVer) : base(data, false)
		{
			writeByte(Consts.ASCIIClient, PacketHeaderOffset.MessCode);
			writeByte(SwapMode.NotSwapped, PacketHeaderOffset.MessSwap);
			writeASCII(appVer, PacketHeaderOffset.ApplVersion);
			writeASCII(appID, PacketHeaderOffset.Appl);
			writeInt32(data.Length - HeaderOffset.END - PacketHeaderOffset.Segment, PacketHeaderOffset.VarpartSize);
			m_length = PacketHeaderOffset.Segment;
		}

		private int dataPos 
		{
			get
			{
				return m_partOffset + PartHeaderOffset.Data + m_partLength;
			}
		}

		public void addData (byte[] data) 
		{
			writeByte(0, dataPos);
			writeBytes(data, dataPos + 1);
			m_partLength += data.Length + 1;
		}

		public void addASCII(string data)
		{
			writeByte(0x20, dataPos);
			writeASCII(data, dataPos + 1);
			m_partLength += data.Length + 1;
		}

		private void Reset()
		{
			m_length = PacketHeaderOffset.Segment;
			m_segments = 0;
			m_segOffset = -1;
			m_segLength = -1;
			m_segParts = -1;
			maxNumberOfSeg = Consts.defaultmaxNumberOfSegm;
			m_partOffset = -1;
			m_partLength = -1;
			m_partArgs = -1;
			replyReserve = 0;
		}

		public void initDbsCommand(bool autocommit, string cmd) 
		{
			initDbsCommand (cmd, true, autocommit);
		}

		public bool initDbsCommand(string cmd, bool reset, bool autocommit) 
		{
			if (!reset) 
			{
				CloseSegment();
				if (data.Length - HeaderOffset.END - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
					- replyReserve - Consts.ReserveForReply < cmd.Length || m_segments >= maxNumberOfSeg) 
					return false;
			}
			initDbs(reset, autocommit);
			writeASCII(cmd, dataPos);
			m_partLength += cmd.Length;
			m_partArgs = 1;
			return true;
		}

		public void initDbs(bool reset, bool autocommit) 
		{
			if (reset) 
				Reset();
		
			NewSegment(CmdMessType.Dbs, autocommit, false);
			NewPart(PartKind.Command);
			m_partArgs = 1;
		}

		#region "Part operations"

		public void NewPart (byte kind) 
		{
			ClosePart();
			InitPart(kind);
		}

		private void InitPart (byte kind) 
		{
			m_segParts++;
			m_partOffset = m_segOffset + m_segLength;
			m_partLength = 0;
			m_partArgs = 0;
			writeByte(kind, m_partOffset + PartHeaderOffset.PartKind);
			writeByte(0, m_partOffset + PartHeaderOffset.Attributes);
			writeInt16(1, m_partOffset + PartHeaderOffset.ArgCount);
			writeInt32(m_segOffset - PacketHeaderOffset.Segment, m_partOffset + PartHeaderOffset.SegmOffs);
			writeInt32(PartHeaderOffset.Data, m_partOffset + PartHeaderOffset.BufLen);
			writeInt32(data.Length - HeaderOffset.END - m_partOffset, m_partOffset + PartHeaderOffset.BufSize);
		}

		public void AddPartFeature(byte[] features)
		{
			if (features != null)
			{
				NewPart(PartKind.Feature);
				writeBytes(features, dataPos);
				m_partLength += features.Length;
				m_partArgs += (short)(features.Length / 2);
				ClosePart();
			}
		}

		public void addPartAttr(byte attr)
		{
			int attrOffset = m_partOffset + PartHeaderOffset.Attributes;
			writeByte(getByte(attrOffset) | attr, attrOffset);
		}

		public void AddPassword(string passwd, string termID)
		{
			NewPart(PartKind.Data);
			addData(Crypt.Mangle(passwd, false));
			addASCII(termID);
			m_partArgs++;
		}

		private void ClosePart() 
		{
			ClosePart(m_partLength, m_partArgs);
		}

		public void ClosePart(int extent, int args)
		{
			if (m_partOffset == -1) 
				return;

			writeInt32(extent, m_partOffset + PartHeaderOffset.BufLen);
			writeInt16(args, m_partOffset + PartHeaderOffset.ArgCount);
			m_segLength += alignSize(extent + PartHeaderOffset.Data);
			m_partOffset = -1;
			m_partLength = -1;
			m_partArgs = -1;

		}

		#endregion

		#region "Segment operations"

		private void NewSegment(byte kind, bool autocommit, bool parseagain) 
		{
			CloseSegment();

			m_segOffset = m_length;
			m_segLength = SegmentHeaderOffset.Part;
			m_segParts = 0;
			writeInt32(0, m_segOffset + SegmentHeaderOffset.Len);
			writeInt32(m_segOffset - PacketHeaderOffset.Segment, m_segOffset + SegmentHeaderOffset.Offs);
			writeInt16(0, m_segOffset + SegmentHeaderOffset.NoOfParts);
			writeInt16(++m_segments, m_segOffset + SegmentHeaderOffset.OwnIndex);
			writeByte(SegmKind.Cmd, m_segOffset + SegmentHeaderOffset.SegmKind);

			// request segment
			writeByte(kind, m_segOffset + SegmentHeaderOffset.MessType);
			writeByte(currentSqlMode, m_segOffset + SegmentHeaderOffset.SqlMode);
			writeByte(Producer.UserCmd, m_segOffset + SegmentHeaderOffset.Producer);
			writeByte((byte)(autocommit?1:0), m_segOffset + SegmentHeaderOffset.CommitImmediateley);
			writeByte(0, m_segOffset + SegmentHeaderOffset.IgnoreCostwarning);
			writeByte(0, m_segOffset + SegmentHeaderOffset.Prepare);
			writeByte(0, m_segOffset + SegmentHeaderOffset.WithInfo);
			writeByte(0, m_segOffset + SegmentHeaderOffset.MassCmd);
			writeByte((byte)(parseagain?1:0), m_segOffset + SegmentHeaderOffset.ParsingAgain);
			writeByte(0, m_segOffset + SegmentHeaderOffset.CommandOptions);

			replyReserve += (m_segments == 2)?Consts.ReserveFor2ndSegment:Consts.ReserveForReply;
		}

		private void CloseSegment() 
		{
			if (m_segOffset == -1) 
				return;

			ClosePart();
			writeInt32(m_segLength, m_segOffset + SegmentHeaderOffset.Len);
			writeInt16(m_segParts, m_segOffset + SegmentHeaderOffset.NoOfParts);
			m_length += m_segLength;
			m_segOffset = -1;
			m_segLength = -1;
			m_segParts = -1;
			return;
		}

		#endregion
	}
}
