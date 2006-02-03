using System;
using System.Text;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "MaxDB Packet"

	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	public class MaxDBPacket : ByteArray
	{
		public MaxDBPacket(byte[] data, bool SwapMode) : base(data, SwapMode)
		{
		}

		public MaxDBPacket(byte[] data, bool SwapMode, int offset) : base(data, SwapMode, offset)
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

		public virtual int ReturnCode
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
			writeByte(Consts.RSQL_DOTNET, HeaderOffset.END + ConnectPacketOffset.OSType);
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

	#endregion

	#region "MaxDB Request Packet"

	public class MaxDBRequestPacket : MaxDBPacket
	{
		protected int m_length = PacketHeaderOffset.Segment;
		protected short m_segments = 0;
		protected int m_partOffset = -1, m_partLength = -1, m_segOffset = -1, m_segLength = -1;
		protected short m_partArgs = -1, m_segParts = -1;
		private byte currentSqlMode = SqlMode.SessionSqlmode;
		private int replyReserve;
		private int maxNumberOfSeg = Consts.defaultmaxNumberOfSegm;
		private bool m_isAvailable = false;

		protected MaxDBRequestPacket(byte[] data, byte clientEncoding, string appID, string appVer) : base(data, false, HeaderOffset.END)
		{
			writeByte(clientEncoding, PacketHeaderOffset.MessCode);
			writeByte(SwapMode.NotSwapped, PacketHeaderOffset.MessSwap);
			writeASCII(appVer, PacketHeaderOffset.ApplVersion);
			writeASCII(appID, PacketHeaderOffset.Appl);
			writeInt32(data.Length - HeaderOffset.END - PacketHeaderOffset.Segment, PacketHeaderOffset.VarpartSize);
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

		public void AddData(byte[] data) 
		{
			writeByte(0, DataPos);
			writeBytes(data, DataPos + 1);
			m_partLength += data.Length + 1;
		}

		public void AddBytes(byte[] data) 
		{
			writeBytes(data, DataPos);
			m_partLength += data.Length;
		}

		public virtual void AddDataString(string data)
		{
			writeByte(0x20, DataPos);
			writeBytes(Encoding.ASCII.GetBytes(data), DataPos + 1, data.Length, Consts.blankBytes);
			m_partLength += data.Length + 1;
		}

		public virtual void AddString(string data)
		{
			writeBytes(Encoding.ASCII.GetBytes(data), DataPos, data.Length, Consts.blankBytes);
			m_partLength += data.Length;
		}

		public bool dropPid(byte [] pid, bool reset) 
		{
			if (reset) 
				Reset();
			else 
			{
				if(m_segOffset != -1) 
					closeSegment();

				int remainingSpace = Length - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
					- replyReserve - Consts.reserveForReply	- 12 // length("drop parseid")
					- SegmentHeaderOffset.Part - PartHeaderOffset.Data - 12; // pid.length
				if (remainingSpace <=0 || m_segments >= maxNumberOfSeg) 
					return false;
			}

			newSegment(CmdMessType.Dbs, false, false);
			newPart(PartKind.Command);
			m_partArgs = 1;
			AddString("Drop Parseid");
			newPart(PartKind.Parsid);
			m_partArgs = 1;
			AddBytes(pid);
			return true;
		}

		public bool dropPidAddtoParsidPart(byte[] pid) 
		{
			int remainingSpace = Length - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
				- replyReserve - Consts.reserveForReply - m_partLength - 12; // pid.length
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
			replyReserve = 0;
		}

		public void Close() 
		{
			closeSegment();
			writeInt32(m_length - PacketHeaderOffset.Segment, PacketHeaderOffset.VarpartLen);
			writeInt16(m_segments, PacketHeaderOffset.NoOfSegm);
		}

		public void initDbsCommand(bool autocommit, string cmd) 
		{
			initDbsCommand(cmd, true, autocommit);
		}

		public bool initDbsCommand(string cmd, bool reset, bool autocommit) 
		{
			if (!reset) 
			{
				closeSegment();
				if (data.Length - HeaderOffset.END - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
					- replyReserve - Consts.ReserveForReply < cmd.Length || m_segments >= maxNumberOfSeg) 
					return false;
			}
			initDbs(reset, autocommit);
			writeASCII(cmd, DataPos);
			m_partLength += cmd.Length;
			m_partArgs = 1;
			return true;
		}

		public void initDbs(bool autocommit) 
		{
			initDbs(true, autocommit);
		}

		public void initDbs(bool reset, bool autocommit) 
		{
			if (reset) 
				Reset();
		
			newSegment(CmdMessType.Dbs, autocommit, false);
			newPart(PartKind.Command);
			m_partArgs = 1;
		}

		public bool initChallengeResponse(string user, byte[] challenge)
		{
			initDbsCommand(false, "CONNECT " + user + "  AUTHENTICATION");
			closePart();
			DataPartVariable data = this.newVarDataPart();
			data.AddRow(2);
			data.writeBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.extent);
			data.AddArg(data.extent, 0);
			data.writeBytes(challenge, data.extent);
			data.AddArg(data.extent, 0);
			data.Close();
			return true;
		}

		public DataPart initGetValue(bool autocommit) 
		{
			Reset();
			newSegment(CmdMessType.Getval, autocommit, false);
			return newDataPart(PartKind.Longdata);
		}

		private DataPartVariable newVarDataPart() 
		{
			int partDataOffs;
			newPart(PartKind.Vardata);
			partDataOffs = m_partOffset + PartHeaderOffset.Data;
			return new DataPartVariable(new ByteArray(data, swapMode, partDataOffs), this);
		}

		public DataPart newDataPart(bool varData) 
		{
			if (varData)
				return newVarDataPart();
			else
				return newDataPart();
		}

		private DataPart newDataPart() 
		{
			return newDataPart(PartKind.Data);
		}

		public DataPart newDataPart(byte partKind) 
		{
			newPart(partKind);
			return new DataPartFixed(Clone(m_partOffset + PartHeaderOffset.Data), this);
		}

		public void incrPartArguments () 
		{
			m_partArgs++;
		}

		public void incrPartArguments (short count) 
		{
			m_partArgs += count;
		}

		public void setWithInfo()
		{
			writeByte(1, m_segOffset + SegmentHeaderOffset.WithInfo);
		}

		#region "Part operations"

		public void newPart (byte kind) 
		{
			closePart();
			initPart(kind);
		}

		private void initPart (byte kind) 
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

		public void addPartFeature(byte[] features)
		{
			if (features != null)
			{
				newPart(PartKind.Feature);
				writeBytes(features, DataPos);
				m_partLength += features.Length;
				m_partArgs += (short)(features.Length / 2);
				closePart();
			}
		}

		public void addPartAttr(byte attr)
		{
			int attrOffset = m_partOffset + PartHeaderOffset.Attributes;
			writeByte((byte)(readByte(attrOffset) | attr), attrOffset);
		}

		public void addClientProofPart(byte[] clientProof)
		{
			DataPartVariable data = this.newVarDataPart();
			data.AddRow(2);
			data.writeBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.extent);
			data.AddArg(data.extent,0);
			data.writeBytes(clientProof,data.extent);
			data.AddArg(data.extent,0);
			data.Close();
		}

		public void addClientIDPart(string clientID)
		{
			newPart(PartKind.Clientid);
			AddDataString(clientID);
			m_partArgs = 1;
		}

		public void addFeatureRequestPart(byte[] features) 
		{
			if ((features != null) && (features.Length != 0)) 
			{
				newPart (PartKind.Feature);
				AddBytes(features);
				incrPartArguments((short)(features.Length/2));
				closePart();
			}
		}

		public void addCursorPart(string cursorName) 
		{
			if (cursorName != null && cursorName.Length != 0) 
			{
				newPart(PartKind.ResultTableName);
				AddString(cursorName);
				m_partArgs++;
				closePart();
			}
		}

		private void closePart() 
		{
			closePart(m_partLength, m_partArgs);
		}

		public void closePart(int extent, short args)
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

		private void newSegment(byte kind, bool autocommit, bool parseagain) 
		{
			closeSegment();

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

		private void closeSegment() 
		{
			if (m_segOffset == -1) 
				return;

			closePart();
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

	#endregion

	#region "MaxDB Unicode Request Packet"

	public class MaxDBRequestPacketUnicode : MaxDBRequestPacket
	{
		public MaxDBRequestPacketUnicode(byte[] data, String applID, String applVers) : base(data, Consts.UnicodeSwapClient, applID, applVers) 
		{
		}

		public override void AddDataString(string data) 
		{
			writeByte(Vsp00Consts.DefinedUnicode, DataPos);
			m_partLength++;
			AddString(data);
		}

		public override void AddString(string data) 
		{
			int lenInBytes = data.Length * Consts.unicodeWidth;
			writeBytes(Encoding.Unicode.GetBytes(data), DataPos, lenInBytes, Consts.blankUnicodeBytes);
			m_partLength += lenInBytes;
		}
	}

	#endregion

	#region "MaxDB Reply Packet"

	public class MaxDBReplyPacket : MaxDBPacket
	{
		private int m_segmOffset;
		private int m_partOffset;
		private int m_cachedResultCount = int.MinValue;
		private int m_cachedPartCount = int.MinValue;
		private int m_currentSegment;
		private int[] m_partIndices;
		private int m_partIdx = -1;

		public MaxDBReplyPacket(byte[] data, bool swapMode) : base(data, swapMode) 
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

			int pc = readInt16(m_segmOffset + SegmentHeaderOffset.NoOfParts);
			m_partIndices = new int[pc];
			int partofs = 0;
			for(int i = 0; i < pc; i++) 
			{
				if(i == 0) 
					partofs = m_partIndices[i] = m_segmOffset + SegmentHeaderOffset.Part;
				else 
				{
					int partlen = readInt32(partofs + PartHeaderOffset.BufLen);
					partofs = m_partIndices[i] = partofs + alignSize(partlen + PartHeaderOffset.Data);
				}
			}
		}

		public int findPart(int requestedKind)
		{
			m_partOffset = -1;
			m_partIdx  = -1;
			int partsLeft = partCount;
			while(partsLeft > 0) 
			{
				nextPart();
				--partsLeft;
				if(partKind == requestedKind) 
				{
					return PartPos;
				}
			}
			throw new PartNotFound();
		}

		public void ClearPartOffset()
		{
			m_partOffset = -1;
			m_partIdx = -1;
		}

		public int partCount
		{
			get
			{
				if (m_cachedPartCount == int.MinValue)
					return m_cachedPartCount = readInt16(m_segmOffset + SegmentHeaderOffset.NoOfParts);
				else 
					return m_cachedPartCount;
			}
		}

		public int nextPart()
		{
			return m_partOffset = m_partIndices[++m_partIdx];
		}

		public int partArgs 
		{
			get
			{
				return readInt16(m_partOffset + PartHeaderOffset.ArgCount);
			}
		}

		public int partKind 
		{
			get
			{
				return readByte(m_partOffset + PartHeaderOffset.PartKind);
			}
		}

		public int partLength
		{
			get
			{
				return readInt32(m_partOffset + PartHeaderOffset.BufLen);
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
					findPart(PartKind.Vardata);
					return new DataPartVariable(new ByteArray(readBytes(PartDataPos , partLength)), 1);
				}
				catch (PartNotFound)
				{
					return null;
				}
			}
		}

		#endregion

		#region "Segment Operations"

		private int segmLength 
		{
			get
			{
				return readInt32(m_segmOffset + SegmentHeaderOffset.Len);
			}
		}

		public int segmCount 
		{
			get
			{
				return readInt16(PacketHeaderOffset.NoOfSegm);
			}
		}

		public int firstSegment
		{
			get
			{
				int result;

				if (segmCount > 0) 
					result = PacketHeaderOffset.Segment;
				else 
					result = -1;

				m_segmOffset = result;
				m_currentSegment = 1;
				ClearPartCache();
				return result;
			}
		}

		public int nextSegment () 
		{
			if (segmCount <= m_currentSegment++)
				return -1;
			m_segmOffset += segmLength;
			ClearPartCache();
			return m_segmOffset;
		}

		#endregion

		#region "Kernel Properties"

		public int KernelMajorVersion
		{
			get
			{
				int result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					//offset 2200 taken from order interface manual
					result = int.Parse(readASCII(PartDataPos + 2200, 1));
				}
				catch(PartNotFound)  
				{
					result = -1;
				}
				return result;
			}
		}

		public int KernelMinorVersion
		{
			get
			{
				int result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					//offset 2202 taken from order interface manual
					result = int.Parse(readASCII(PartDataPos + 2201, 2));
				}
				catch(PartNotFound)  
				{
					result = -1;
				}
				return result;
			}
		}

		public int KernelCorrectionLevel
		{
			get
			{
				int result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					//offset 2204 taken from order interface manual
					result = int.Parse(readASCII(PartDataPos + 2203, 2));
				}
				catch(PartNotFound) 
				{
					result = -1;
				}
				return result;
			}
		}

		#endregion

		#region "Field and Column Operations"

		public string[] parseColumnNames() 
		{
			int columnCount = partArgs;
			string[] result = new string[columnCount];
			int nameLen;
			int pos = PartDataPos;

			for (int i = 0; i < columnCount; ++i) 
			{
				nameLen = readByte(pos);
				result[i] = readASCII(pos + 1, nameLen);
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
        
			columnCount = partArgs;
			result = new DBTechTranslator[columnCount];
			pos = PartDataPos;
			// byte[] info;
			for (int i = 0; i < columnCount; ++i) 
			{
				DBProcParameterInfo info = null;
				if (procParameters != null && procParameters.Length > i) 
					info = procParameters[i];

				mode = readByte(pos + ParamInfo.ModeOffset);
				ioType = readByte(pos + ParamInfo.IOTypeOffset);
				dataType = readByte(pos + ParamInfo.DataTypeOffset);
				frac = readByte(pos + ParamInfo.FracOffset);
				len = readInt16(pos + ParamInfo.LengthOffset);
				ioLen = readInt16(pos + ParamInfo.InOutLenOffset);
				if (isVardata && mode == ParamInfo.Input)
					bufpos =  readInt16(pos + ParamInfo.ParamNoOffset); 
				else
					bufpos = readInt32(pos + ParamInfo.BufPosOffset);

				result[i] = GetTranslator(mode, ioType, dataType, frac, len, ioLen, bufpos, spaceoption, isDBProcedure, info);

				pos += ParamInfo.ParamInfo_END;
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
						return new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else
						return new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
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
				return readInt16(m_segmOffset + SegmentHeaderOffset.ReturnCode); 
			}
		}

		public string SqlState 
		{
			get
			{
				return readASCII(m_segmOffset + SegmentHeaderOffset.SqlState, 5);
			}
		}

		public string ErrorMsg 
		{
			get
			{
				string result;

				try 
				{
					findPart(PartKind.ErrorText);
					result = readASCII(PartDataPos, partLength).Trim();
				}
				catch(PartNotFound) 
				{
					result = MessageTranslator.Translate(MessageKey.ERROR);
				}
				return result;
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
						findPart(PartKind.ResultCount);
					}
					catch (PartNotFound) 
					{
						return m_cachedResultCount--;
					}
				}
				return m_cachedResultCount = VDNNumber.Number2Int(readBytes(m_partOffset + PartHeaderOffset.Data, partLength));
			} 
			else 
				return m_cachedResultCount;
		}

		public int SessionID
		{
			get
			{
				int result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					result = readInt32(PartDataPos + 1);
				}
				catch(PartNotFound) 
				{
					result = -1;
				}
				return result;
			}
		}

		public bool IsUnicode
		{
			get
			{
				bool result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					result = (readByte(PartDataPos) == 1);
				}
				catch(PartNotFound) 
				{
					result = false;
				}
				return result;
			}
		}

		public int funcCode
		{
			get
			{
				return readInt16(m_segmOffset + SegmentHeaderOffset.FunctionCode);
			}
		}

		public int ErrorPos 
		{
			get
			{
				return readInt32(m_segmOffset + SegmentHeaderOffset.ErrorPos);
			}
		}

		public byte[] Features
		{
			get
			{
				try 
				{
					findPart(PartKind.Feature);
					return readBytes(PartDataPos, partLength);
				}
				catch(PartNotFound)  
				{
					return null;
				}
			}
		}

		public int weakReturnCode
		{
			get
			{
				int result = ReturnCode;
				if (result == 100) 
				{
					switch (funcCode) 
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

		public MaxDBSQLException createException() 
		{
			string state = SqlState;
			int rc = ReturnCode;
			string errmsg = ErrorMsg;
			int errorPos = ErrorPos;

			if (rc == -8000) 
				errmsg = "RESTART REQUIRED";

			return new MaxDBSQLException(errmsg, state, rc, errorPos);
		}

		public byte[] GetDataBytes (int pos, int len) 
		{
			int defByte;

			defByte = readByte(pos);
			if (defByte == 0xff) 
				return null;
			
			return readBytes(pos + 1, len - 1);
		}
	}

	#endregion

	#region "MaxDB Unicode Reply Packet"

	public class MaxDBReplyPacketUnicode : MaxDBReplyPacket
	{
		public MaxDBReplyPacketUnicode(byte[] data, bool swapMode) : base(data, swapMode)
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
						return new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
					else
						return new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
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
}
