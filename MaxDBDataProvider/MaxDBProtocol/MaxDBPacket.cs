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
		private bool m_isAvailable = false;

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

		public void addData(byte[] data) 
		{
			writeByte(0, dataPos);
			writeBytes(data, dataPos + 1);
			m_partLength += data.Length + 1;
		}

		public void addBytes(byte[] data) 
		{
			writeBytes(data, dataPos);
			m_partLength += data.Length;
		}

		public void addASCII(string data)
		{
			writeByte(0x20, dataPos);
			writeASCII(data, dataPos + 1);
			m_partLength += data.Length + 1;
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
			addASCII("Drop Parseid");
			newPart(PartKind.Parsid);
			m_partArgs = 1;
			addBytes(pid);
			return true;
		}

		public bool dropPidAddtoParsidPart(byte[] pid) 
		{
			int remainingSpace = Length - m_length - SegmentHeaderOffset.Part - PartHeaderOffset.Data
				- replyReserve - Consts.reserveForReply - m_partLength - 12; // pid.length
			if(remainingSpace <=0) 
				return false;
			addBytes(pid);
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
			writeASCII(cmd, dataPos);
			m_partLength += cmd.Length;
			m_partArgs = 1;
			return true;
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
			data.addRow(2);
			data.writeBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.extent);
			data.addArg(data.extent, 0);
			data.writeBytes(challenge, data.extent);
			data.addArg(data.extent, 0);
			data.Close();
			return true;
		}

		private DataPartVariable newVarDataPart() 
		{
			int partDataOffs;
			newPart(PartKind.Vardata);
			partDataOffs = m_partOffset + PartHeaderOffset.Data;
			return new DataPartVariable(new ByteArray(data, swapMode, partDataOffs), this);
		}

		public void incrPartArguments () 
		{
			m_partArgs++;
		}

		public void incrPartArguments (short count) 
		{
			m_partArgs += count;
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
				writeBytes(features, dataPos);
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

		public void addPassword(string passwd, string termID)
		{
			newPart(PartKind.Data);
			addData(Crypt.Mangle(passwd, false));
			addASCII(termID);
			m_partArgs++;
		}

		public void addClientProofPart(byte[] clientProof)
		{
			DataPartVariable data = this.newVarDataPart();
			data.addRow(2);
			data.writeBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.extent);
			data.addArg(data.extent,0);
			data.writeBytes(clientProof,data.extent);
			data.addArg(data.extent,0);
			data.Close();
		}

		public void addClientIDPart(string clientID)
		{
			newPart(PartKind.Clientid);
			addASCII(clientID);
			m_partArgs = 1;
		}

		public void addFeatureRequestPart(byte[] features) 
		{
			if ((features != null) && (features.Length != 0)) 
			{
				newPart (PartKind.Feature);
				addBytes(features);
				incrPartArguments((short)(features.Length/2));
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

		private void ClearPartCache()
		{
			m_cachedResultCount = int.MinValue;
			m_cachedPartCount = int.MinValue;

			int pc = readInt16(m_segmOffset + SegmentHeaderOffset.NoOfParts);
			m_partIndices = new int[pc];
			int partofs = 0;
			for(int i=0; i< pc; ++i) 
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

		public override int ReturnCode
		{
			get
			{
				return readInt16(m_segmOffset + SegmentHeaderOffset.Returncode); 
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
					findPart(PartKind.Errortext);
					result = readASCII(PartDataPos, partLength).Trim();
				}
				catch(PartNotFound) 
				{
					result = MessageTranslator.Translate(MessageKey.ERROR);
				}
				return result;
			}
		}

//		public int resultCount(bool positionedAtPart)
//		{
//			if(m_cachedResultCount == int.MinValue) 
//			{
//				if (!positionedAtPart) 
//				{
//					try 
//					{
//						findPart(PartKind.ResultCount);
//					}
//					catch (PartNotFound) 
//					{
//						return --m_cachedResultCount;
//					}
//				}
//				byte[] rawNumber = readBytes(m_partOffset + PartHeaderOffset.Data, partLength);
//				return m_cachedResultCount = VDNNumber.number2int (rawNumber);
//			} 
//			else 
//			{
//				return m_cachedResultCount;
//			}
//		}

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

		int partKind 
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

		public int funcCode
		{
			get
			{
				return readInt16(m_segmOffset + SegmentHeaderOffset.FunctionCode);
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

		public int ErrorPos 
		{
			get
			{
				return readInt32(m_segmOffset + SegmentHeaderOffset.ErrorPos);
			}
		}

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
					result = int.Parse(readASCII(PartDataPos + 2201, 1));
				}
				catch(PartNotFound)  
				{
					result = -1;
				}
				return result;
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

		public int KernelCorrectionLevel
		{
			get
			{
				int result;

				try 
				{
					findPart(PartKind.SessionInfoReturned);
					//offset 2204 taken from order interface manual
					result = int.Parse(readASCII(PartDataPos + 2204, 1));
				}
				catch(PartNotFound) 
				{
					result = -1;
				}
				return result;
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
	}
}
