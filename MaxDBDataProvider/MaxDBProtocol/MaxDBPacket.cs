using System;
using System.Text;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if NATIVE
	#region "MaxDB Packet"

	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	public class MaxDBPacket : ByteArray
	{
		public MaxDBPacket(byte[] data) : base(data)
		{
		}

		public MaxDBPacket(byte[] data, int offset) : base(data, offset)
		{
		}

		public MaxDBPacket(byte[] data, int offset, bool swapMode) : base(data, offset, swapMode)
		{
		}

		public void FillHeader(byte msgClass, int senderRef, int receiverRef, int maxSendLen)
		{
			// fill out header part
			WriteUInt32(0, HeaderOffset.ActSendLen);
			WriteByte(3, HeaderOffset.ProtocolID);
			WriteByte(msgClass, HeaderOffset.MessClass);
			WriteByte(0, HeaderOffset.RTEFlags);
			WriteByte(0, HeaderOffset.ResidualPackets);
			WriteInt32(senderRef, HeaderOffset.SenderRef);
			WriteInt32(receiverRef, HeaderOffset.ReceiverRef);
			WriteUInt16(0, HeaderOffset.RTEReturnCode);
			WriteUInt16(0, HeaderOffset.Filler);
			WriteInt32(maxSendLen, HeaderOffset.MaxSendLen);
		}

		public void SetSendLength(int val)
		{
			WriteInt32(val, HeaderOffset.ActSendLen);
			WriteInt32(val, HeaderOffset.MaxSendLen);
		}

		public int MaxSendLength
		{
			get
			{
				return ReadInt32(HeaderOffset.MaxSendLen);
			}
		}

		public int ActSendLength
		{
			get
			{
				return ReadInt32(HeaderOffset.ActSendLen);
			}
		}

		public int PacketSender
		{
			get
			{
				return ReadInt32(HeaderOffset.SenderRef);
			}
		}

		public virtual int ReturnCode
		{
			get
			{
				return ReadInt16(HeaderOffset.RTEReturnCode);
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

	#endregion

	#region "MaxDB Connect Packet"

	public struct ConnectPacketData
	{
		public string DBName;
		public int Port;
		public int MaxSegSize;
		public int MaxDataLen;
		public int PacketSize;
		public int MinReplySize;
	}

	public class MaxDBConnectPacket : MaxDBPacket
	{
		private int m_curPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBConnectPacket(byte[] data) : base(data)
		{
		}

		public MaxDBConnectPacket(byte[] data, bool swapMode) : base(data, 0, swapMode)
		{
		}
			
		public MaxDBConnectPacket(byte[] data, ConnectPacketData connData) : base(data)
		{
			// fill body
			WriteByte(Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
			WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
			WriteUInt16(ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
			WriteByte(SQLType.USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
			WriteByte(Consts.RSQL_DOTNET, HeaderOffset.END + ConnectPacketOffset.OSType);
			WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
			WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
			WriteInt32(connData.MaxSegSize, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			WriteInt32(connData.MaxDataLen, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			WriteInt32(connData.PacketSize, HeaderOffset.END + ConnectPacketOffset.PacketSize);
			WriteInt32(connData.MinReplySize, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			if (connData.DBName.Length > ConnectPacketOffset.DBNameSize)
				connData.DBName = connData.DBName.Substring(0, ConnectPacketOffset.DBNameSize);
			WriteASCII(connData.DBName.PadRight(ConnectPacketOffset.DBNameSize, ' '), HeaderOffset.END + ConnectPacketOffset.ServerDB);
			WriteASCII("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
			// fill out variable part
			WriteByte(4, m_curPos++);
			WriteByte(ArgType.REM_PID, m_curPos++);
			WriteASCII("0", m_curPos++);
			WriteByte(0, m_curPos++);
			// add port number
			WriteByte(4, m_curPos++);
			WriteByte(ArgType.PORT_NO, m_curPos++);
			WriteUInt16((ushort)connData.Port, m_curPos);
			m_curPos += 2;
			// add aknowledge flag
			WriteByte(3, m_curPos++);
			WriteByte(ArgType.ACKNOWLEDGE, m_curPos++);
			WriteByte(0, m_curPos++);
			// add omit reply part flag
			WriteByte(3, m_curPos++);
			WriteByte(ArgType.OMIT_REPLY_PART, m_curPos++);
			WriteByte(1, m_curPos++);
		}

		public void FillPacketLength()
		{
			const int packetMinLen = ConnectPacketOffset.MinSize;
			if (m_curPos < packetMinLen) m_curPos = packetMinLen;
			WriteUInt16((ushort)(m_curPos - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
		}

		public void Close()
		{
			int currentLength = Length;
			int requiredLength = ConnectPacketOffset.MinSize - HeaderOffset.END;

			if (currentLength < requiredLength) 
				m_curPos += requiredLength - currentLength;
			WriteUInt16((ushort)m_curPos, ConnectPacketOffset.ConnectLength);
		}

		public bool IsSwapped
		{
			get
			{
				return (ReadByte(HeaderOffset.END + ConnectPacketOffset.MessCode + 1) == SwapMode.Swapped);
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
				return ReadInt32(HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			}
		}

		public int MinReplySize
		{
			get
			{
				return ReadInt32(HeaderOffset.END + ConnectPacketOffset.MinReplySize);
			}
		}

		public int PacketSize
		{
			get
			{
				return ReadInt32(HeaderOffset.END + ConnectPacketOffset.PacketSize);
			}
		}

		public int MaxSegmentSize
		{
			get
			{
				return ReadInt32(HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			}
		}

		public int PortNumber
		{
			get
			{
				int pos = HeaderOffset.END + ConnectPacketOffset.VarPart;
				while(pos < m_data.Length)
				{
					if (ReadByte(pos + 1) == ArgType.PORT_NO)
						return ReadByte(pos + 2) * 0xFF + ReadByte(pos + 3);//port number is always not swapped
					else
						pos += ReadByte(pos);
				}

				return Ports.Default;
			}
		}

		public bool IsAuthAllowed
		{
			get
			{
				int pos = HeaderOffset.END + ConnectPacketOffset.VarPart;
				while(pos < m_data.Length)
				{
					byte len = ReadByte(pos);

					if (len == 0)
						break;

					if (ReadByte(pos + 1) == ArgType.AUTH_ALLOW)
						foreach(string authParam in Encoding.ASCII.GetString(m_data, pos + 2, len - 3).Split(','))
							if (authParam.ToUpper() == Crypt.ScramMD5Name)
								return true;
					
					pos += len;
				}

				return false;
			}
		}
	}

	#endregion

	#region "MaxDB Request Packet"

	public class MaxDBRequestPacket : MaxDBPacket
	{
		protected int m_length = PacketHeaderOffset.Segment;
		protected short m_segments = 0;
		protected int m_partOffset = -1, m_partLength = -1, m_segOffset = -1, m_segLength = -1;
		protected short m_partArgs = -1, m_segParts = -1;
		private byte m_currentSqlMode = SqlMode.SessionSqlmode;
		private int m_replyReserve;
		private int maxNumberOfSeg = Consts.defaultmaxNumberOfSegm;
		private bool m_isAvailable = false;
		public const int resultCountSize = 6;

		protected MaxDBRequestPacket(byte[] data, byte clientEncoding, string appID, string appVer) : base(data, HeaderOffset.END)
		{
			WriteByte(clientEncoding, PacketHeaderOffset.MessCode);
			WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, PacketHeaderOffset.MessSwap);
			WriteASCII(appVer, PacketHeaderOffset.ApplVersion);
			WriteASCII(appID, PacketHeaderOffset.Appl);
			WriteInt32(data.Length - HeaderOffset.END - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartSize);
			m_length = PacketHeaderOffset.Segment;
		}

		public MaxDBRequestPacket(byte[] data, string appID, string appVer) : this(data, Consts.ASCIIClient, appID, appVer)
		{
		}

		protected int DataPos 
		{
			get
			{
				return m_partOffset + PartHeaderOffset.Data + m_partLength;
			}
		}

		public bool IsAvailable
		{
			get
			{
				return m_isAvailable;
			}
			set
			{
				m_isAvailable = value;
			}
		}

		public int PacketLength
		{
			get
			{
				return m_length;
			}
		}

		public short PartArgs
		{
			get
			{
				return m_partArgs;
			}
			set
			{
				m_partArgs = value;
			}
		}

		public byte SwitchSqlMode(byte newMode) 
		{
			byte result = m_currentSqlMode;

			m_currentSqlMode = newMode;
			return result;
		}

		public void AddData(byte[] data) 
		{
			WriteByte(0, DataPos);
			WriteBytes(data, DataPos + 1);
			m_partLength += data.Length + 1;
		}

		public void AddBytes(byte[] data) 
		{
			WriteBytes(data, DataPos);
			m_partLength += data.Length;
		}

		public virtual void AddDataString(string data)
		{
			WriteByte(0x20, DataPos);
			WriteBytes(Encoding.ASCII.GetBytes(data), DataPos + 1, data.Length, Consts.blankBytes);
			m_partLength += data.Length + 1;
		}

		public virtual void AddString(string data)
		{
			WriteBytes(Encoding.ASCII.GetBytes(data), DataPos, data.Length, Consts.blankBytes);
			m_partLength += data.Length;
		}

		public void AddResultCount(int count) 
		{
			NewPart(PartKind.ResultCount);
			byte[] fullNumber = VDNNumber.Long2Number (count);
			byte[] countNumber = new byte[resultCountSize];
			Array.Copy(fullNumber, 0, countNumber, 0, fullNumber.Length);
			AddData(countNumber);
			m_partArgs++;
		}

		public bool dropPid(byte [] pid, bool reset) 
		{
			if (reset) 
				Reset();
			else 
			{
				if(m_segOffset != -1) 
					CloseSegment();

				int remainingSpace = Length - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
					- m_replyReserve - Consts.reserveForReply	- 12 // length("drop parseid")
					- SegmentHeaderOffset.Part - PartHeaderOffset.Data - 12; // pid.length
				if (remainingSpace <=0 || m_segments >= maxNumberOfSeg) 
					return false;
			}

			NewSegment(CmdMessType.Dbs, false, false);
			NewPart(PartKind.Command);
			m_partArgs = 1;
			AddString("Drop Parseid");
			NewPart(PartKind.Parsid);
			m_partArgs = 1;
			AddBytes(pid);
			return true;
		}

		public bool dropPidAddtoParsidPart(byte[] pid) 
		{
			int remainingSpace = Length - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
				- m_replyReserve - Consts.reserveForReply - m_partLength - 12; // pid.length
			if(remainingSpace <=0) 
				return false;
			AddBytes(pid);
			m_partArgs++;
			return true;
		}

		public void Init(short maxSegment) 
		{
			Reset();
			maxNumberOfSeg = maxSegment;
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
			m_replyReserve = 0;
		}

		public void Close() 
		{
			CloseSegment();
			WriteInt32(m_length - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartLen);
			WriteInt16(m_segments, PacketHeaderOffset.NoOfSegm);
		}

		public void InitDbsCommand(bool autocommit, string cmd) 
		{
			InitDbsCommand(cmd, true, autocommit);
		}

		public bool InitDbsCommand(string cmd, bool reset, bool autocommit) 
		{
			if (!reset) 
			{
				CloseSegment();
				if (m_data.Length - HeaderOffset.END - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
					- m_replyReserve - Consts.ReserveForReply < cmd.Length || m_segments >= maxNumberOfSeg) 
					return false;
			}
			InitDbs(reset, autocommit);
			WriteASCII(cmd, DataPos);
			m_partLength += cmd.Length;
			m_partArgs = 1;
			return true;
		}

		public int InitParseCommand(string cmd, bool reset, bool parseagain) 
		{
			InitParse(reset, parseagain);
			AddString(cmd);
			m_partArgs = 1;
			return this.m_partLength;
		}

		public void InitDbs(bool autocommit) 
		{
			InitDbs(true, autocommit);
		}

		public void InitDbs(bool reset, bool autocommit) 
		{
			if (reset) 
				Reset();
		
			NewSegment(CmdMessType.Dbs, autocommit, false);
			NewPart(PartKind.Command);
			m_partArgs = 1;
		}

		public void InitParse(bool reset, bool parseagain) 
		{
			if (reset) 
				Reset();
			NewSegment(CmdMessType.Parse, false, parseagain);
			NewPart(PartKind.Command);
			return;
		}

		public bool InitChallengeResponse(string user, byte[] challenge)
		{
			InitDbsCommand(false, "CONNECT " + user + "  AUTHENTICATION");
			ClosePart();
			DataPartVariable data = NewVarDataPart();
			data.AddRow(2);
			data.WriteBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.m_extent);
			data.AddArg(data.m_extent, 0);
			data.WriteBytes(challenge, data.m_extent);
			data.AddArg(data.m_extent, 0);
			data.Close();
			return true;
		}

		public DataPart InitGetValue(bool autocommit) 
		{
			Reset();
			NewSegment(CmdMessType.GetValue, autocommit, false);
			return NewDataPart(PartKind.LongData);
		}

		public DataPart InitPutValue(bool autocommit) 
		{
			Reset();
			NewSegment(CmdMessType.PutValue, autocommit, false);
			return NewDataPart(PartKind.LongData);
		}

		public void InitExecute(byte [] parseID, bool autocommit)
		{
			Reset();
			NewSegment(CmdMessType.Execute, autocommit, false);
			AddParseIdPart(parseID);
		}

		private DataPartVariable NewVarDataPart() 
		{
			int partDataOffs;
			NewPart(PartKind.Vardata);
			partDataOffs = m_partOffset + PartHeaderOffset.Data;
			return new DataPartVariable(Clone(partDataOffs, false), this); //??? why DataPartVariable requires BigEndian?
		}

		public DataPart NewDataPart(bool varData) 
		{
			if (varData)
				return NewVarDataPart();
			else
				return NewDataPart();
		}

		private DataPart NewDataPart() 
		{
			return NewDataPart(PartKind.Data);
		}

		public DataPart NewDataPart(byte partKind) 
		{
			NewPart(partKind);
			return new DataPartFixed(Clone(m_partOffset + PartHeaderOffset.Data), this);
		}

		public void SetWithInfo()
		{
			WriteByte(1, m_segOffset + SegmentHeaderOffset.WithInfo);
		}

		public void SetMassCommand() 
		{
			WriteByte(1, m_segOffset + SegmentHeaderOffset.MassCmd);
		}

		#region "Part operations"

		public void NewPart(byte kind) 
		{
			ClosePart();
			InitPart(kind);
		}

		private void InitPart(byte kind) 
		{
			m_segParts++;
			m_partOffset = m_segOffset + m_segLength;
			m_partLength = 0;
			m_partArgs = 0;
			WriteByte(kind, m_partOffset + PartHeaderOffset.PartKind);
			WriteByte(0, m_partOffset + PartHeaderOffset.Attributes);
			WriteInt16(1, m_partOffset + PartHeaderOffset.ArgCount);
			WriteInt32(m_segOffset - PacketHeaderOffset.Segment, m_partOffset + PartHeaderOffset.SegmOffs);
			WriteInt32(PartHeaderOffset.Data, m_partOffset + PartHeaderOffset.BufLen);
			WriteInt32(m_data.Length - HeaderOffset.END - m_partOffset, m_partOffset + PartHeaderOffset.BufSize);
		}

		public void AddPartFeature(byte[] features)
		{
			if (features != null)
			{
				NewPart(PartKind.Feature);
				WriteBytes(features, DataPos);
				m_partLength += features.Length;
				m_partArgs += (short)(features.Length / 2);
				ClosePart();
			}
		}

		public void AddPartAttr(byte attr)
		{
			int attrOffset = m_partOffset + PartHeaderOffset.Attributes;
			WriteByte((byte)(ReadByte(attrOffset) | attr), attrOffset);
		}

		public void AddClientProofPart(byte[] clientProof)
		{
			DataPartVariable data = NewVarDataPart();
			data.AddRow(2);
			data.WriteBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.m_extent);
			data.AddArg(data.m_extent,0);
			data.WriteBytes(clientProof, data.m_extent);
			data.AddArg(data.m_extent, 0);
			data.Close();
		}

		public void AddClientIDPart(string clientID)
		{
			NewPart(PartKind.Clientid);
			AddDataString(clientID);
			m_partArgs = 1;
		}

		public void AddFeatureRequestPart(byte[] features) 
		{
			if ((features != null) && (features.Length != 0)) 
			{
				NewPart (PartKind.Feature);
				AddBytes(features);
				PartArgs += (short)(features.Length/2);
				ClosePart();
			}
		}

		public void AddParseIdPart(byte[] parseID) 
		{
			if (parseID != null) 
			{
				NewPart(PartKind.Parsid);
				AddBytes(parseID);
				m_partArgs = 1;
				ClosePart();
			}
			else
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_INTERNAL_INVALIDPARSEID));
			}
		}

		public void AddCursorPart(string cursorName) 
		{
			if ((cursorName != null) && (cursorName.Length != 0)) 
			{
				NewPart(PartKind.ResultTableName);
				AddString(cursorName);
				m_partArgs++;
				ClosePart();
			}
		}

		private void ClosePart() 
		{
			ClosePart(m_partLength, m_partArgs);
		}

		public void ClosePart(int extent, short args)
		{
			if (m_partOffset == -1) 
				return;

			WriteInt32(extent, m_partOffset + PartHeaderOffset.BufLen);
			WriteInt16(args, m_partOffset + PartHeaderOffset.ArgCount);
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
			WriteInt32(0, m_segOffset + SegmentHeaderOffset.Len);
			WriteInt32(m_segOffset - PacketHeaderOffset.Segment, m_segOffset + SegmentHeaderOffset.Offs);
			WriteInt16(0, m_segOffset + SegmentHeaderOffset.NoOfParts);
			WriteInt16(++m_segments, m_segOffset + SegmentHeaderOffset.OwnIndex);
			WriteByte(SegmKind.Cmd, m_segOffset + SegmentHeaderOffset.SegmKind);

			// request segment
			WriteByte(kind, m_segOffset + SegmentHeaderOffset.MessType);
			WriteByte(m_currentSqlMode, m_segOffset + SegmentHeaderOffset.SqlMode);
			WriteByte(Producer.UserCmd, m_segOffset + SegmentHeaderOffset.Producer);
			WriteByte((byte)(autocommit?1:0), m_segOffset + SegmentHeaderOffset.CommitImmediateley);
			WriteByte(0, m_segOffset + SegmentHeaderOffset.IgnoreCostwarning);
			WriteByte(0, m_segOffset + SegmentHeaderOffset.Prepare);
			WriteByte(0, m_segOffset + SegmentHeaderOffset.WithInfo);
			WriteByte(0, m_segOffset + SegmentHeaderOffset.MassCmd);
			WriteByte((byte)(parseagain?1:0), m_segOffset + SegmentHeaderOffset.ParsingAgain);
			WriteByte(0, m_segOffset + SegmentHeaderOffset.CommandOptions);

			m_replyReserve += (m_segments == 2) ? Consts.ReserveFor2ndSegment : Consts.ReserveForReply;
		}

		private void CloseSegment() 
		{
			if (m_segOffset == -1) 
				return;

			ClosePart();
			WriteInt32(m_segLength, m_segOffset + SegmentHeaderOffset.Len);
			WriteInt16(m_segParts, m_segOffset + SegmentHeaderOffset.NoOfParts);
			m_length += m_segLength;
			m_segOffset = -1;
			m_segLength = -1;
			m_segParts = -1;
			return;
		}

		#endregion
	}

	#endregion

	#region "MaxDB Unicode Request Packet"

	public class MaxDBRequestPacketUnicode : MaxDBRequestPacket
	{
		public MaxDBRequestPacketUnicode(byte[] data, String applID, String applVers) : base(data, Consts.UnicodeSwapClient, applID, applVers) 
		{
		}

		public override void AddDataString(string data) 
		{
			WriteByte(Vsp00Consts.DefinedUnicode, DataPos);
			m_partLength++;
			AddString(data);
		}

		public override void AddString(string data) 
		{
			int lenInBytes = data.Length * Consts.unicodeWidth;
			WriteBytes(Encoding.Unicode.GetBytes(data), DataPos, lenInBytes, Consts.blankUnicodeBytes);
			m_partLength += lenInBytes;
		}
	}

	#endregion

	#region "MaxDB Reply Packet"

	internal class MaxDBReplyPacket : MaxDBPacket
	{
		private int m_segmOffset;
		private int m_partOffset;
		private int m_cachedResultCount = int.MinValue;
		private int m_cachedPartCount = int.MinValue;
		private int m_currentSegment;
		private int[] m_partIndices;
		private int m_partIdx = -1;

		public MaxDBReplyPacket(byte[] data) : base(data, 0, data[PacketHeaderOffset.MessSwap] == SwapMode.Swapped) 
		{
			m_segmOffset = PacketHeaderOffset.Segment;
			m_currentSegment = 1;
			ClearPartCache();
		}

		#region "Part Operations"

		private void ClearPartCache()
		{
			m_cachedResultCount = int.MinValue;
			m_cachedPartCount = int.MinValue;

			int pc = ReadInt16(m_segmOffset + SegmentHeaderOffset.NoOfParts);
			m_partIndices = new int[pc];
			int partofs = 0;
			for(int i = 0; i < pc; i++) 
			{
				if(i == 0) 
					partofs = m_partIndices[i] = m_segmOffset + SegmentHeaderOffset.Part;
				else 
				{
					int partlen = ReadInt32(partofs + PartHeaderOffset.BufLen);
					partofs = m_partIndices[i] = partofs + alignSize(partlen + PartHeaderOffset.Data);
				}
			}
		}

		public int FindPart(int requestedKind)
		{
			m_partOffset = -1;
			m_partIdx  = -1;
			int partsLeft = PartCount;
			while(partsLeft > 0) 
			{
				NextPart();
				--partsLeft;
				if(PartType == requestedKind) 
					return PartPos;
			}
			throw new PartNotFound();
		}

		public bool ExistsPart(int requestedKind) 
		{
			try 
			{
				FindPart(requestedKind);
				return true;
			}
			catch (PartNotFound) 
			{
				return false;
			}
		}

		public void ClearPartOffset()
		{
			m_partOffset = -1;
			m_partIdx = -1;
		}

		public int PartCount
		{
			get
			{
				if (m_cachedPartCount == int.MinValue)
					return m_cachedPartCount = ReadInt16(m_segmOffset + SegmentHeaderOffset.NoOfParts);
				else 
					return m_cachedPartCount;
			}
		}

		public int NextPart()
		{
			return m_partOffset = m_partIndices[++m_partIdx];
		}

		public int PartArgs 
		{
			get
			{
				return ReadInt16(m_partOffset + PartHeaderOffset.ArgCount);
			}
		}

		public int PartType 
		{
			get
			{
				return ReadByte(m_partOffset + PartHeaderOffset.PartKind);
			}
		}

		public int PartLength
		{
			get
			{
				return ReadInt32(m_partOffset + PartHeaderOffset.BufLen);
			}
		}

		public int PartPos 
		{
			get
			{
				return m_partOffset;
			}
		}

		public int PartDataPos 
		{
			get
			{
				return m_partOffset + PartHeaderOffset.Data;
			}
		}

		public DataPartVariable VarDataPart
		{
			get
			{
				try 
				{
					FindPart(PartKind.Vardata);
					return new DataPartVariable(new ByteArray(ReadBytes(PartDataPos , PartLength), 0, false), 1);//??? why DataPartVariable requires BigEndian
				}
				catch (PartNotFound)
				{
					return null;
				}
			}
		}

		public bool WasLastPart 
		{
			get
			{
				int partAttributes;
				bool result;

				partAttributes = this.ReadByte(m_partOffset + PartHeaderOffset.Attributes);
				result = (partAttributes & PartAttributes.LastPacket_Ext) != 0;
				return result;
			}
		}

		#endregion

		#region "Segment Operations"

		private int SegmLength 
		{
			get
			{
				return ReadInt32(m_segmOffset + SegmentHeaderOffset.Len);
			}
		}

		public int SegmCount 
		{
			get
			{
				return ReadInt16(PacketHeaderOffset.NoOfSegm);
			}
		}

		public int FirstSegment()
		{
			int result;

			if (SegmCount > 0) 
				result = PacketHeaderOffset.Segment;
			else 
				result = -1;

			m_segmOffset = result;
			m_currentSegment = 1;
			ClearPartCache();
			return result;
		}

		public int NextSegment() 
		{
			if (SegmCount <= m_currentSegment++)
				return -1;
			m_segmOffset += SegmLength;
			ClearPartCache();
			return m_segmOffset;
		}

		#endregion

		#region "Kernel Properties"

		public int KernelMajorVersion
		{
			get
			{
				try 
				{
					FindPart(PartKind.SessionInfoReturned);
					//offset 2200 is taken from order interface manual
					return int.Parse(ReadASCII(PartDataPos + 2200, 1));
				}
				catch(PartNotFound)  
				{
					return -1;
				}
			}
		}

		public int KernelMinorVersion
		{
			get
			{
				try 
				{
					FindPart(PartKind.SessionInfoReturned);
					//offset 2202 is taken from order interface manual
					return int.Parse(ReadASCII(PartDataPos + 2201, 2));
				}
				catch(PartNotFound)  
				{
					return -1;
				}
			}
		}

		public int KernelCorrectionLevel
		{
			get
			{
				try 
				{
					FindPart(PartKind.SessionInfoReturned);
					//offset 2204 is taken from order interface manual
					return int.Parse(ReadASCII(PartDataPos + 2203, 2));
				}
				catch(PartNotFound) 
				{
					return -1;
				}
			}
		}

		#endregion

		#region "Field and Column Operations"

		public string[] ParseColumnNames() 
		{
			int columnCount = PartArgs;
			string[] result = new string[columnCount];
			int nameLen;
			int pos = PartDataPos;

			for (int i = 0; i < columnCount; ++i) 
			{
				nameLen = ReadByte(pos);
				result[i] = ReadASCII(pos + 1, nameLen);
				pos += nameLen + 1;
			}
			return result;
		}

		// Extracts the short field info, and creates translator instances.
		public DBTechTranslator[] ParseShortFields(bool spaceoption, bool isDBProcedure, DBProcParameterInfo[] procParameters, bool isVardata)
		{
			int columnCount;
			DBTechTranslator[] result;
			int pos;
			int mode;
			int ioType;
			int dataType;
			int frac;
			int len;
			int ioLen;
			int bufpos;
        
			columnCount = PartArgs;
			result = new DBTechTranslator[columnCount];
			pos = PartDataPos;
			// byte[] info;
			for (int i = 0; i < columnCount; ++i) 
			{
				DBProcParameterInfo info = null;
				if (procParameters != null && procParameters.Length > i) 
					info = procParameters[i];

				mode = ReadByte(pos + ParamInfo.ModeOffset);
				ioType = ReadByte(pos + ParamInfo.IOTypeOffset);
				dataType = ReadByte(pos + ParamInfo.DataTypeOffset);
				frac = ReadByte(pos + ParamInfo.FracOffset);
				len = ReadInt16(pos + ParamInfo.LengthOffset);
				ioLen = ReadInt16(pos + ParamInfo.InOutLenOffset);
				if (isVardata && mode == ParamInfo.Input)
					bufpos =  ReadInt16(pos + ParamInfo.ParamNoOffset); 
				else
					bufpos = ReadInt32(pos + ParamInfo.BufPosOffset);

				result[i] = GetTranslator(mode, ioType, dataType, frac, len, ioLen, bufpos, spaceoption, isDBProcedure, info);

				pos += ParamInfo.END;
			}
			return result;
		}

		protected virtual DBTechTranslator GetTranslator(int mode, int ioType, int dataType, int frac, int len, 
			int ioLen, int bufpos, bool spaceoption, bool isDBProcedure, DBProcParameterInfo procParamInfo)
		{
			switch (dataType) 
			{
				case DataType.CHA:
				case DataType.CHE:
				case DataType.VARCHARA:
				case DataType.VARCHARE: 
					if (spaceoption)
						return new SpaceOptionStringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else
						return new StringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.CHB: 
					if(procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE) 
						return new StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, false);
					else 
						return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.VARCHARB:
					return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.BOOLEAN:
					return new BooleanTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.TIME:
					return new TimeTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.DATE:
					return new DateTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.TIMESTAMP:
					return new TimestampTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.FIXED:
				case DataType.FLOAT:
				case DataType.VFLOAT:
				case DataType.SMALLINT:
				case DataType.INTEGER:
					return new NumericTranslator(mode, ioType, dataType, len, frac, ioLen, bufpos);
				case DataType.STRA:
				case DataType.STRE:
				case DataType.LONGA:
				case DataType.LONGE:
					if(isDBProcedure) 
						return new ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else 
						return new ASCIIStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.STRB:
				case DataType.LONGB:
					if(isDBProcedure) 
						return new ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else 
						return new BinaryStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.UNICODE:
				case DataType.VARCHARUNI:
					if (spaceoption)
						return new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, m_swapMode);
					else
						return new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, m_swapMode);
				case DataType.LONGUNI:
				case DataType.STRUNI:
					if(isDBProcedure) 
						return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else 
						return new UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				default:
					return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
			}
		}

		#endregion

		public override int ReturnCode
		{
			get
			{
				return ReadInt16(m_segmOffset + SegmentHeaderOffset.ReturnCode); 
			}
		}

		public string SqlState 
		{
			get
			{
				return ReadASCII(m_segmOffset + SegmentHeaderOffset.SqlState, 5);
			}
		}

		public string ErrorMsg 
		{
			get
			{
				try 
				{
					FindPart(PartKind.ErrorText);
					return ReadASCII(PartDataPos, PartLength).Trim();
				}
				catch(PartNotFound) 
				{
					return MessageTranslator.Translate(MessageKey.ERROR);
				}
			}
		}

		public int ResultCount(bool positionedAtPart)
		{
			if(m_cachedResultCount == int.MinValue) 
			{
				if (!positionedAtPart) 
				{
					try 
					{
						FindPart(PartKind.ResultCount);
					}
					catch (PartNotFound) 
					{
						return m_cachedResultCount--;
					}
				}
				
				return m_cachedResultCount = VDNNumber.Number2Int(ReadDataBytes(m_partOffset + PartHeaderOffset.Data, PartLength));
			} 
			else 
				return m_cachedResultCount;
		}

		public int SessionID
		{
			get
			{
				try 
				{
					FindPart(PartKind.SessionInfoReturned);
					return Clone(0, false).ReadInt32(PartDataPos + 1);//??? session ID is always BigEndian
				}
				catch(PartNotFound) 
				{
					return -1;
				}
			}
		}

		public bool IsUnicode
		{
			get
			{
				try 
				{
					FindPart(PartKind.SessionInfoReturned);
					return (ReadByte(PartDataPos) == 1);
				}
				catch(PartNotFound) 
				{
					return false;
				}
			}
		}

		public int FuncCode
		{
			get
			{
				return ReadInt16(m_segmOffset + SegmentHeaderOffset.FunctionCode);
			}
		}

		public int ErrorPos 
		{
			get
			{
				return ReadInt32(m_segmOffset + SegmentHeaderOffset.ErrorPos);
			}
		}

		public byte[] Features
		{
			get
			{
				try 
				{
					FindPart(PartKind.Feature);
					return ReadBytes(PartDataPos, PartLength);
				}
				catch(PartNotFound)  
				{
					return null;
				}
			}
		}

		public int WeakReturnCode
		{
			get
			{
				int result = ReturnCode;
				if (result == 100) 
				{
					switch(FuncCode) 
					{
						case FunctionCode.DBProcExecute:
						case FunctionCode.DBProcWithResultSetExecute:
						case FunctionCode.MFetchFirst:
						case FunctionCode.MFetchLast:
						case FunctionCode.MFetchNext:
						case FunctionCode.MFetchPrev:
						case FunctionCode.MFetchPos:
						case FunctionCode.MFetchSame:
						case FunctionCode.MFetchRelative:
						case FunctionCode.FetchFirst:
						case FunctionCode.FetchLast:
						case FunctionCode.FetchNext:
						case FunctionCode.FetchPrev:
						case FunctionCode.FetchPos:
						case FunctionCode.FetchSame:
						case FunctionCode.FetchRelative:
							/* keep result */
							break;
						default:
							result = 0;
							break;
					}
				}
				return result;
			}
		}

		public MaxDBSQLException CreateException() 
		{
			string state = SqlState;
			int rc = ReturnCode;
			string errmsg = ErrorMsg;
			int errorPos = ErrorPos;

			if (rc == -8000) 
				errmsg = "RESTART REQUIRED";

			return new MaxDBSQLException(errmsg, state, rc, errorPos);
		}

		public byte[] ReadDataBytes(int pos, int len) 
		{
			int defByte;

			defByte = ReadByte(pos);
			if (defByte == 0xff) 
				return null;
			
			return ReadBytes(pos + 1, len - 1);
		}

		public string ReadString(int offset, int len) 
		{
			return ReadASCII(offset, len);
		}

		public byte[][] ParseLongDescriptors() 
		{
			if (!ExistsPart(PartKind.LongData)) 
				return null;

			int argCount = PartArgs;
			byte[][] result = new byte[argCount][];
			for (int i = 0; i < argCount; i++) 
			{
				int pos = (i * (LongDesc.Size + 1)) + 1;
				result[i] = ReadBytes(PartDataPos +  pos, LongDesc.Size);
			}
			return result;
		}
	}

	#endregion

	#region "MaxDB Unicode Reply Packet"

	internal class MaxDBReplyPacketUnicode : MaxDBReplyPacket
	{
		public MaxDBReplyPacketUnicode(byte[] data) : base(data)
		{
		}

		protected override DBTechTranslator GetTranslator(int mode, int ioType, int dataType, int frac, int len, int ioLen,
			int bufpos, bool spaceoption, bool isDBProcedure, DBProcParameterInfo procParamInfo)
		{
			switch (dataType) 
			{
				case DataType.CHA:
				case DataType.CHE:
				case DataType.VARCHARA:
				case DataType.VARCHARE:
					if (spaceoption)
						return new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, m_swapMode);
					else
						return new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, m_swapMode);
				case DataType.CHB: 
					if(procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE) 
						return new StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, true);
					else 
						return new BytesTranslator (mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.STRA:
				case DataType.STRE:
				case DataType.LONGA:
				case DataType.LONGE:
					if(isDBProcedure) 
						return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else 
						return new UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.TIME:
					return new UnicodeTimeTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.TIMESTAMP:
					return new UnicodeTimestampTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.DATE:
					return new UnicodeDateTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				default:
					return base.GetTranslator(mode, ioType, dataType, frac, len, ioLen, bufpos, spaceoption, isDBProcedure, procParamInfo);
			}

		}
	}

	#endregion
#endif
}
