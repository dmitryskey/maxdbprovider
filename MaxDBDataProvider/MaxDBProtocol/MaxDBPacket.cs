//	Copyright © 2005-2006 Dmitry S. Kataev
//	Copyright © 2002-2003 SAP AG
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
using System.Text;
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data.MaxDBProtocol
{
	#region "MaxDB Packet"

	/// <summary>
	/// Summary description for MaxDBPacket.
	/// </summary>
	internal class MaxDBPacket : ByteArray
	{
		protected int iSegmentOffset = -1;
		protected int iCurrentSegment;
		protected int iPartOffset;
		protected int iCachedResultCount = int.MinValue;
		protected int iCachedPartCount = int.MinValue;
		protected int[] iPartIndices;
		protected int iPartIndex = -1;

		public MaxDBPacket(byte[] data)
			: base(data)
		{
		}

		public MaxDBPacket(byte[] data, int offset)
			: base(data, offset)
		{
		}

		public MaxDBPacket(byte[] data, int offset, bool swapMode)
			: base(data, offset, swapMode)
		{
		}

		public void FillHeader(byte msgClass, int senderRef)
		{
			// fill out header part
			WriteUInt32(0, HeaderOffset.ActSendLen);
			WriteByte(3, HeaderOffset.ProtocolID);
			WriteByte(msgClass, HeaderOffset.MessClass);
			WriteByte(0, HeaderOffset.RTEFlags);
			WriteByte(0, HeaderOffset.ResidualPackets);
			WriteInt32(senderRef, HeaderOffset.SenderRef);
			WriteInt32(0, HeaderOffset.ReceiverRef);
			WriteUInt16(0, HeaderOffset.RTEReturnCode);
			WriteUInt16(0, HeaderOffset.Filler);
			WriteInt32(0, HeaderOffset.MaxSendLen);
		}

		public void SetSendLength(int value)
		{
			WriteInt32(value, HeaderOffset.ActSendLen);
			WriteInt32(value, HeaderOffset.MaxSendLen);
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

		protected static int AlignSize(int value)
		{
            return value + (value % Consts.AlignValue != 0 ? (Consts.AlignValue - value % Consts.AlignValue) : 0);
		}

		#region "Segment operations"

		protected int SegmentLength
		{
			get
			{
				return ReadInt32(iSegmentOffset + SegmentHeaderOffset.Len);
			}
		}

		protected int SegmentCount
		{
			get
			{
				return ReadInt16(PacketHeaderOffset.NoOfSegm);
			}
		}

		protected void ClearPartCache()
		{
			iCachedResultCount = int.MinValue;
			iCachedPartCount = int.MinValue;

			int pc = ReadInt16(iSegmentOffset + SegmentHeaderOffset.NoOfParts);
			iPartIndices = new int[pc];
			int partofs = 0;
			for (int i = 0; i < pc; i++)
			{
                if (i == 0)
                {
                    partofs = iPartIndices[i] = iSegmentOffset + SegmentHeaderOffset.Part;
                }
                else
                {
                    int partlen = ReadInt32(partofs + PartHeaderOffset.BufLen);
                    partofs = iPartIndices[i] = partofs + AlignSize(partlen + PartHeaderOffset.Data);
                }
			}
		}

		public int FirstSegment()
		{
			int result = SegmentCount > 0 ? PacketHeaderOffset.Segment : -1;

			iSegmentOffset = result;
			iCurrentSegment = 1;
			ClearPartCache();
			return result;
		}

		public int NextSegment()
		{
            if (SegmentCount <= iCurrentSegment++)
            {
                return -1;
            }

			iSegmentOffset += SegmentLength;
			ClearPartCache();
			return iSegmentOffset;
		}

		public string DumpSegment(DateTime dt)
		{
			var dump = new StringBuilder();

			dump.Append("   ").Append(ReadByte(iSegmentOffset + SegmentHeaderOffset.SegmKind).ToString(CultureInfo.InvariantCulture));
			dump.Append(" Segment ").Append(ReadInt16(iSegmentOffset + SegmentHeaderOffset.OwnIndex).ToString(CultureInfo.InvariantCulture));
			dump.Append(" at ").Append(ReadInt32(iSegmentOffset + SegmentHeaderOffset.Offset).ToString(CultureInfo.InvariantCulture));
			dump.Append("(").Append(ReadInt32(iSegmentOffset + SegmentHeaderOffset.Len).ToString(CultureInfo.InvariantCulture));
			dump.Append(" of ").Append((ReadInt32(PacketHeaderOffset.VarPartSize) - iSegmentOffset).ToString(CultureInfo.InvariantCulture));
			dump.Append(" bytes)\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

			switch (ReadByte(iSegmentOffset + SegmentHeaderOffset.SegmKind))
			{
				case SegmKind.Cmd:
				case SegmKind.Proccall:
					dump.Append("        messtype: ").Append(CmdMessType.Name[ReadByte(iSegmentOffset + SegmentHeaderOffset.MessType)]);
					dump.Append("  sqlmode: ").Append(SqlModeName.Value[ReadByte(iSegmentOffset +
						SegmentHeaderOffset.SqlMode)].ToLower(CultureInfo.InvariantCulture));
					dump.Append("  producer: ").Append(ProducerType.Name[ReadByte(iSegmentOffset + SegmentHeaderOffset.Producer)]);

					dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture)).Append("        Options: ");
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.CommitImmediately) == 1 ? "commit " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning) == 1 ? "ignore costwarning " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.Prepare) == 1 ? "prepare " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.WithInfo) == 1 ? "with info " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.MassCmd) == 1 ? "mass cmd " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.ParsingAgain) == 1 ? "parsing again " : ""));
					break;
				case SegmKind.Return:
				case SegmKind.Procreply:
					dump.Append("        RC: ").Append(ReadInt16(iSegmentOffset +
						SegmentHeaderOffset.ReturnCode).ToString(CultureInfo.InvariantCulture));
					dump.Append("  ").Append(ReadAscii(iSegmentOffset + SegmentHeaderOffset.SqlState,
						SegmentHeaderOffset.ReturnCode - SegmentHeaderOffset.SqlState));
					dump.Append("  (Pos ").Append(ReadInt32(iSegmentOffset +
						SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
					dump.Append(") Function ").Append(ReadInt16(iSegmentOffset +
						SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
					dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));
					break;
				default:
					dump.Append("unknown segment kind");
					dump.Append("        messtype: ").Append(CmdMessType.Name[ReadByte(iSegmentOffset + SegmentHeaderOffset.MessType)]);
					dump.Append("  sqlmode: ").Append(SqlModeName.Value[ReadByte(iSegmentOffset + SegmentHeaderOffset.SqlMode)]);
					dump.Append("  producer: ").Append(ProducerType.Name[ReadByte(iSegmentOffset + SegmentHeaderOffset.Producer)]);

					dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture)).Append("        Options: ");
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.CommitImmediately) == 1 ? "commit " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning) == 1 ? "ignore costwarning " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.Prepare) == 1 ? "prepare " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.WithInfo) == 1 ? "with info " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.MassCmd) == 1 ? "mass cmd " : ""));
					dump.Append((ReadByte(iSegmentOffset + SegmentHeaderOffset.ParsingAgain) == 1 ? "parsing again " : ""));

					dump.Append("        RC: ").Append(ReadInt16(iSegmentOffset +
						SegmentHeaderOffset.ReturnCode).ToString(CultureInfo.InvariantCulture));
					dump.Append("  ").Append(ReadAscii(iSegmentOffset + SegmentHeaderOffset.SqlState,
						SegmentHeaderOffset.ReturnCode - SegmentHeaderOffset.SqlState));
					dump.Append("  (Pos ").Append(ReadInt32(iSegmentOffset +
						SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
					dump.Append(") Function ").Append(ReadInt16(iSegmentOffset +
						SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
					dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));
					break;
			}

			dump.Append("        ").Append(ReadInt16(iSegmentOffset +
				SegmentHeaderOffset.NoOfParts).ToString(CultureInfo.InvariantCulture));
			dump.Append(" parts:\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

			ClearPartOffset();
			for (int i = 0; i < PartCount; i++)
			{
				NextPart();
				dump.Append(DumpPart(dt));
			}

			return dump.ToString();
		}

		#endregion

		#region "Part operations"

		public int PartLength
		{
			get
			{
				return ReadInt32(iPartOffset + PartHeaderOffset.BufLen);
			}
		}

		public int PartSize
		{
			get
			{
				return ReadInt32(iPartOffset + PartHeaderOffset.BufSize);
			}
		}

		public int PartPos
		{
			get
			{
				return iPartOffset;
			}
		}

		public int PartDataPos
		{
			get
			{
				return iPartOffset + PartHeaderOffset.Data;
			}
		}

		public int PartArgsCount
		{
			get
			{
				return ReadInt16(iPartOffset + PartHeaderOffset.ArgCount);
			}
		}

		public int PartType
		{
			get
			{
				return ReadByte(iPartOffset + PartHeaderOffset.PartKind);
			}
		}

		public int PartSegmentOffset
		{
			get
			{
				return ReadInt32(iPartOffset + PartHeaderOffset.SegmOffset);
			}
		}

		public void ClearPartOffset()
		{
			iPartOffset = -1;
			iPartIndex = -1;
		}

		public int PartCount
		{
			get
			{
                if (iCachedPartCount == int.MinValue)
                {
                    return iCachedPartCount = ReadInt16(iSegmentOffset + SegmentHeaderOffset.NoOfParts);
                }
                else
                {
                    return iCachedPartCount;
                }
			}
		}

		public int NextPart()
		{
			return iPartOffset = iPartIndices[++iPartIndex];
		}

		public string DumpPart(DateTime dt)
		{
			var dump = new StringBuilder();

			string partkindname = "Unknown Part " + PartType.ToString(CultureInfo.InvariantCulture);
            if (PartType < PartKind.Name.Length)
            {
                partkindname = PartKind.Name[PartType] + " Part";
            }

			dump.Append("        ").Append(partkindname).Append(" ");
			dump.Append(PartArgsCount.ToString(CultureInfo.InvariantCulture)).Append(" Arguments (");
			dump.Append(PartLength.ToString(CultureInfo.InvariantCulture)).Append(" of ");
			dump.Append(PartSize.ToString(CultureInfo.InvariantCulture)).Append(") (Segment at ");
			dump.Append(PartSegmentOffset.ToString(CultureInfo.InvariantCulture)).Append(")\n")
                .Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

			byte[] data = ReadBytes(PartDataPos, PartLength);

			for (int i = 0; i <= data.Length / 0x10; i++)
			{
				dump.Append((i * 0x10).ToString("x", CultureInfo.InvariantCulture).PadLeft(8)).Append("  ");

				int tailLen = Math.Min(0x10, 0x10 - ((i + 1) * 0x10 - data.Length));

                for (int k = 0; k < tailLen; k++)
                {
                    dump.Append(data[i * 0x10 + k].ToString("x2", CultureInfo.InvariantCulture)).Append(" ");
                }

				dump.Append("  |".PadLeft((0x10 - tailLen + 1) * 3));
				string dumpStr = Encoding.ASCII.GetString(data, i * 0x10, tailLen).PadRight(0x10);
                foreach (char ch in dumpStr)
                {
                    dump.Append(!char.IsControl(ch) ? ch : '?');
                }

				dump.Append("|\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));
			}

			return dump.ToString();
		}

		#endregion

		public string DumpPacket()
		{
			StringBuilder dump = new StringBuilder();

			dump.Append(Consts.MessageCode[ReadByte(PacketHeaderOffset.MessCode)]).Append(" ");
			dump.Append(SwapMode.SwapType[ReadByte(PacketHeaderOffset.MessSwap)]).Append(" swap ");
			dump.Append(ReadAscii(PacketHeaderOffset.Appl,
				PacketHeaderOffset.VarPartSize - PacketHeaderOffset.Appl).ToString(CultureInfo.InvariantCulture)).Append("-");
			dump.Append(ReadAscii(PacketHeaderOffset.ApplVersion,
				PacketHeaderOffset.Appl - PacketHeaderOffset.ApplVersion).ToString(CultureInfo.InvariantCulture));
			dump.Append(" (transfer len ").Append((ReadInt32(PacketHeaderOffset.VarPartLen) +
				PacketHeaderOffset.Segment).ToString(CultureInfo.InvariantCulture)).Append(")");

			return dump.ToString();
		}
	}

	#endregion

	#region "MaxDB Connect Packet"

	internal struct ConnectPacketData
	{
		public string DBName;
		public int Port;
		public int MaxSegmentSize;
		public int MaxDataLen;
		public int PacketSize;
		public int MinReplySize;
	}

	internal class MaxDBConnectPacket : MaxDBPacket
	{
		private int iCurrentPos = HeaderOffset.END + ConnectPacketOffset.VarPart;

		public MaxDBConnectPacket(byte[] data)
			: base(data)
		{
		}

		public MaxDBConnectPacket(byte[] data, bool swapMode)
			: base(data, 0, swapMode)
		{
		}

		public MaxDBConnectPacket(byte[] data, ConnectPacketData packetData)
			: base(data)
		{
			// fill body
			WriteByte(Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
			WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
			WriteUInt16(ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
			WriteByte(SqlType.USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
			WriteByte(Consts.RSQL_DOTNET, HeaderOffset.END + ConnectPacketOffset.OSType);
			WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
			WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
			WriteInt32(packetData.MaxSegmentSize, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
			WriteInt32(packetData.MaxDataLen, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
			WriteInt32(packetData.PacketSize, HeaderOffset.END + ConnectPacketOffset.PacketSize);
			WriteInt32(packetData.MinReplySize, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
            if (packetData.DBName.Length > ConnectPacketOffset.DBNameSize)
            {
                packetData.DBName = packetData.DBName.Substring(0, ConnectPacketOffset.DBNameSize);
            }
			WriteAscii(packetData.DBName.PadRight(ConnectPacketOffset.DBNameSize), HeaderOffset.END + ConnectPacketOffset.ServerDB);
			WriteAscii("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
			// fill out variable part
			WriteByte(4, iCurrentPos++);
			WriteByte(ArgType.REM_PID, iCurrentPos++);
			WriteAscii("0", iCurrentPos++);
			WriteByte(0, iCurrentPos++);
			// add port number
			WriteByte(4, iCurrentPos++);
			WriteByte(ArgType.PORT_NO, iCurrentPos++);
			WriteUInt16((ushort)packetData.Port, iCurrentPos);
			iCurrentPos += 2;
			// add aknowledge flag
			WriteByte(3, iCurrentPos++);
			WriteByte(ArgType.ACKNOWLEDGE, iCurrentPos++);
			WriteByte(0, iCurrentPos++);
			// add omit reply part flag
			WriteByte(3, iCurrentPos++);
			WriteByte(ArgType.OMIT_REPLY_PART, iCurrentPos++);
			WriteByte(1, iCurrentPos++);
		}

		public void FillPacketLength()
		{
			const int packetMinLen = ConnectPacketOffset.MinSize;
            if (iCurrentPos < packetMinLen)
            {
                iCurrentPos = packetMinLen;
            }

			WriteUInt16((ushort)(iCurrentPos - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
		}

		public void Close()
		{
			int currentLength = PacketLength;
			int requiredLength = ConnectPacketOffset.MinSize - HeaderOffset.END;

            if (currentLength < requiredLength)
            {
                iCurrentPos += requiredLength - currentLength;
            }

			WriteUInt16((ushort)(iCurrentPos - HeaderOffset.END), ConnectPacketOffset.ConnectLength);
		}

		public int PacketLength
		{
			get
			{
				return iCurrentPos;
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

		public string ClientDB
		{
			get
			{
				string dbname = ReadAscii(HeaderOffset.END + ConnectPacketOffset.ClientDB, 8);
                if (dbname.IndexOf("\0") >= 0)
                {
                    dbname = dbname.Substring(0, dbname.IndexOf("\0"));
                }

				return dbname;
			}
		}

		public bool IsAuthAllowed
		{
			get
			{
				int pos = HeaderOffset.END + ConnectPacketOffset.VarPart;
				while (pos < byData.Length)
				{
					byte len = ReadByte(pos);

                    if (len == 0)
                    {
                        break;
                    }

                    if (ReadByte(pos + 1) == ArgType.AUTH_ALLOW)
                    {
                        foreach (string authParam in Encoding.ASCII.GetString(byData, pos + 2, len - 3).Split(','))
                        {
                            if (string.Compare(authParam, Crypt.ScramMD5Name, true, CultureInfo.InvariantCulture) == 0)
                            {
                                return true;
                            }
                        }
                    }

					pos += len;
				}

				return false;
			}
		}

		public bool IsLittleEndian
		{
			get
			{
				return ReadByte(HeaderOffset.END + ConnectPacketOffset.MessCode + 1) == SwapMode.Swapped;
			}
		}
	}

	#endregion

	#region "MaxDB Request Packet"

	internal class MaxDBRequestPacket : MaxDBPacket
	{
		protected int iLength = PacketHeaderOffset.Segment;
		protected short sSegments;
		protected int iPartLength = -1, iSegmentLength = -1;
		protected short sPartArguments = -1, sSegmentParts = -1;
		private byte byCurrentSqlMode = (byte)SqlMode.SessionSqlMode;
		private int iReplyReserve;
		private int iMaxNumberOfSegment = Consts.DefaultMaxNumberOfSegm;
		private const string strDropCmd = "Drop Parseid";
		private const int iResultCountSize = 6;

		protected MaxDBRequestPacket(byte[] data, byte clientEncoding, string appID, string appVersion)
			: base(data, HeaderOffset.END)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteByte(clientEncoding, PacketHeaderOffset.MessCode);
			WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, PacketHeaderOffset.MessSwap);
			WriteAscii(appVersion, PacketHeaderOffset.ApplVersion);
			WriteAscii(appID, PacketHeaderOffset.Appl);
			WriteInt32(data.Length - HeaderOffset.END - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartSize);
			iLength = PacketHeaderOffset.Segment;
		}

		public MaxDBRequestPacket(byte[] data, string appID, string appVersion)
			: this(data, Consts.ASCIIClient, appID, appVersion)
		{
		}

		protected int DataPos
		{
			get
			{
				return iPartOffset + PartHeaderOffset.Data + iPartLength;
			}
		}

		public int PacketLength
		{
			get
			{
				return iLength;
			}
		}

		public short PartArguments
		{
			get
			{
				return sPartArguments;
			}
			set
			{
				sPartArguments = value;
			}
		}

		public static int ResultCountPartSize
		{
			get
			{
				return PartHeaderOffset.Data + Consts.ResultCountSize + 8; // alignment
			}
		}

		public void AddNullData(int len)
		{
			WriteByte(255, DataPos);
			iPartLength += len + 1;
		}

		public void AddUndefinedResultCount()
		{
			NewPart(PartKind.ResultCount);
			AddNullData(iResultCountSize);
			sPartArguments++;
		}

		public byte SwitchSqlMode(byte newMode)
		{
			byte oldMode = byCurrentSqlMode;
			byCurrentSqlMode = newMode;
			return oldMode;
		}

		public void AddData(byte[] data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteByte(0, DataPos);
			WriteBytes(data, DataPos + 1);
			iPartLength += data.Length + 1;
		}

		public void AddBytes(byte[] data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteBytes(data, DataPos);
			iPartLength += data.Length;
		}

		public virtual void AddDataString(string data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteByte(Consts.DefinedAscii, DataPos);
			WriteBytes(Encoding.ASCII.GetBytes(data), DataPos + 1, data.Length, Consts.BlankBytes);
			iPartLength += data.Length + 1;
		}

		public virtual void AddString(string data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteBytes(Encoding.ASCII.GetBytes(data), DataPos, data.Length, Consts.BlankBytes);
			iPartLength += data.Length;
		}

		public void AddResultCount(int count)
		{
			NewPart(PartKind.ResultCount);
			byte[] fullNumber = VDNNumber.Long2Number(count);
			byte[] countNumber = new byte[iResultCountSize];
            Buffer.BlockCopy(fullNumber, 0, countNumber, 0, fullNumber.Length);
			AddData(countNumber);
			sPartArguments++;
		}

		public bool DropParseId(byte[] parseId, bool reset)
		{
            if (parseId == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "pid"));
            }

            if (reset)
            {
                Reset();
            }
            else
            {
                if (iSegmentOffset != -1)
                {
                    CloseSegment();
                }

                int remainingSpace = Length - iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
                    - iReplyReserve - Consts.ReserveForReply - strDropCmd.Length
                    - SegmentHeaderOffset.Part - PartHeaderOffset.Data - parseId.Length;
                if (remainingSpace <= 0 || sSegments >= iMaxNumberOfSegment)
                {
                    return false;
                }
            }

			NewSegment(CmdMessType.Dbs, false, false);
			NewPart(PartKind.Command);
			sPartArguments = 1;
			AddString(strDropCmd);
			NewPart(PartKind.ParseId);
			sPartArguments = 1;
			AddBytes(parseId);
			return true;
		}

		public bool DropParseIdAddToParseIdPart(byte[] parseId)
		{
            if (parseId == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "pid"));
            }

			int remainingSpace = Length - iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
				- iReplyReserve - Consts.ReserveForReply - iPartLength - parseId.Length;
            if (remainingSpace <= 0)
            {
                return false;
            }

			AddBytes(parseId);
			sPartArguments++;
			return true;
		}

		public void Init(short maxSegment)
		{
			Reset();
			iMaxNumberOfSegment = maxSegment;
		}

		private void Reset()
		{
			iLength = PacketHeaderOffset.Segment;
			sSegments = 0;
			iSegmentOffset = -1;
			iSegmentLength = -1;
			sSegmentParts = -1;
			iPartOffset = -1;
			iPartLength = -1;
			sPartArguments = -1;
			iReplyReserve = 0;
		}

		public void Close()
		{
			CloseSegment();
			WriteInt32(iLength - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartLen);
			WriteInt16(sSegments, PacketHeaderOffset.NoOfSegm);
		}

		public virtual void InitDbsCommand(bool autoCommit, string cmd)
		{
			InitDbsCommand(cmd, true, autoCommit, false);
		}

		public virtual bool InitDbsCommand(string command, bool reset, bool autoCommit)
		{
			return InitDbsCommand(command, reset, autoCommit, false);
		}

		public bool InitDbsCommand(string command, bool reset, bool autoCommit, bool unicode)
		{
            if (command == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "command"));
            }

			if (!reset)
			{
				CloseSegment();
                if (byData.Length - HeaderOffset.END - iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
                    - iReplyReserve - Consts.ReserveForReply < command.Length || sSegments >= iMaxNumberOfSegment)
                {
                    return false;
                }
			}

			InitDbs(reset, autoCommit);
			if (unicode)
			{
				WriteUnicode(command, DataPos);
				iPartLength += command.Length * Consts.UnicodeWidth;
			}
			else
			{
				WriteAscii(command, DataPos);
				iPartLength += command.Length;
			}

			sPartArguments = 1;
			return true;
		}

		public int InitParseCommand(string cmd, bool reset, bool parseAgain)
		{
			InitParse(reset, parseAgain);
			AddString(cmd);
			sPartArguments = 1;
			return this.iPartLength;
		}

		public void InitDbs(bool autoCommit)
		{
			InitDbs(true, autoCommit);
		}

		public void InitDbs(bool reset, bool autoCommit)
		{
            if (reset)
            {
                Reset();
            }

			NewSegment(CmdMessType.Dbs, autoCommit, false);
			NewPart(PartKind.Command);
			sPartArguments = 1;
		}

		public void InitParse(bool reset, bool parseAgain)
		{
            if (reset)
            {
                Reset();
            }

			NewSegment(CmdMessType.Parse, false, parseAgain);
			NewPart(PartKind.Command);
			return;
		}

		public bool InitChallengeResponse(string user, byte[] challenge)
		{
			InitDbsCommand(false, "CONNECT " + user + "  AUTHENTICATION");
			ClosePart();
			DataPartVariable data = NewVarDataPart();
			data.AddRow(2);
			data.WriteBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.Extent);
			data.AddArg(data.Extent, 0);
			data.WriteBytes(challenge, data.Extent);
			data.AddArg(data.Extent, 0);
			data.Close();
			return true;
		}

		public void InitExecute(byte[] parseId, bool autoCommit)
		{
			Reset();
			NewSegment(CmdMessType.Execute, autoCommit, false);
			AddParseIdPart(parseId);
		}

		public void SetWithInfo()
		{
			WriteByte(1, iSegmentOffset + SegmentHeaderOffset.WithInfo);
		}

		public void SetMassCommand()
		{
			WriteByte(1, iSegmentOffset + SegmentHeaderOffset.MassCmd);
		}

		#region "Part operations"

		public DataPart InitGetValue(bool autoCommit)
		{
			Reset();
			NewSegment(CmdMessType.GetValue, autoCommit, false);
			return NewDataPart(PartKind.LongData);
		}

		public DataPart InitPutValue(bool autoCommit)
		{
			Reset();
			NewSegment(CmdMessType.PutValue, autoCommit, false);
			return NewDataPart(PartKind.LongData);
		}

		private DataPartVariable NewVarDataPart()
		{
			int partDataOffs;
			NewPart(PartKind.Vardata);
			partDataOffs = iPartOffset + PartHeaderOffset.Data;
			return new DataPartVariable(Clone(partDataOffs, false), this); //??? why DataPartVariable requires BigEndian?
		}

		public DataPart NewDataPart(bool varData)
		{
            return varData ? NewVarDataPart() : NewDataPart();
		}

		private DataPart NewDataPart()
		{
			return NewDataPart(PartKind.Data);
		}

		public DataPart NewDataPart(byte partKind)
		{
			NewPart(partKind);
			return new DataPartFixed(Clone(iPartOffset + PartHeaderOffset.Data), this);
		}

		public void NewPart(byte kind)
		{
			ClosePart();
			InitPart(kind);
		}

		private void InitPart(byte kind)
		{
			sSegmentParts++;
			iPartOffset = iSegmentOffset + iSegmentLength;
			iPartLength = 0;
			sPartArguments = 0;
			WriteByte(kind, iPartOffset + PartHeaderOffset.PartKind);
			WriteByte(0, iPartOffset + PartHeaderOffset.Attributes);
			WriteInt16(1, iPartOffset + PartHeaderOffset.ArgCount);
			WriteInt32(iSegmentOffset - PacketHeaderOffset.Segment, iPartOffset + PartHeaderOffset.SegmOffset);
			WriteInt32(PartHeaderOffset.Data, iPartOffset + PartHeaderOffset.BufLen);
			WriteInt32(byData.Length - HeaderOffset.END - iPartOffset, iPartOffset + PartHeaderOffset.BufSize);
		}

		public void AddPartAttr(byte attr)
		{
			int attrOffset = iPartOffset + PartHeaderOffset.Attributes;
			WriteByte((byte)(ReadByte(attrOffset) | attr), attrOffset);
		}

		public void AddClientProofPart(byte[] clientProof)
		{
			var data = NewVarDataPart();
			data.AddRow(2);
			data.WriteBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.Extent);
			data.AddArg(data.Extent, 0);
			data.WriteBytes(clientProof, data.Extent);
			data.AddArg(data.Extent, 0);
			data.Close();
		}

		public void AddClientIdPart(string clientId)
		{
			NewPart(PartKind.ClientId);
			AddDataString(clientId);
			sPartArguments = 1;
		}

		public void AddFeatureRequestPart(byte[] features)
		{
            if (features == null)
            {
                return;
            }

			if (features.Length > 0)
			{
				NewPart(PartKind.Feature);
				AddBytes(features);
				PartArguments += (short)(features.Length / 2);
				ClosePart();
			}
		}

		public void AddParseIdPart(byte[] parseId)
		{
            if (parseId != null)
            {
                NewPart(PartKind.ParseId);
                AddBytes(parseId);
                sPartArguments = 1;
                ClosePart();
            }
            else
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INTERNAL_INVALIDPARSEID));
            }
		}

		public void AddCursorPart(string cursorName)
		{
			if (cursorName != null && cursorName.Length != 0)
			{
				NewPart(PartKind.ResultTableName);
				AddString(cursorName);
				sPartArguments++;
				ClosePart();
			}
		}

		private void ClosePart()
		{
			ClosePart(iPartLength, sPartArguments);
		}

		public void ClosePart(int extent, short args)
		{
            if (iPartOffset == -1)
            {
                return;
            }

			WriteInt32(extent, iPartOffset + PartHeaderOffset.BufLen);
			WriteInt16(args, iPartOffset + PartHeaderOffset.ArgCount);
			iSegmentLength += AlignSize(extent + PartHeaderOffset.Data);
			iPartOffset = -1;
			iPartLength = -1;
			sPartArguments = -1;
		}

		#endregion

		#region "Segment operations"

		private void NewSegment(byte kind, bool autocommit, bool parseagain)
		{
			CloseSegment();

			iSegmentOffset = iLength;
			iSegmentLength = SegmentHeaderOffset.Part;
			sSegmentParts = 0;
			WriteInt32(0, iSegmentOffset + SegmentHeaderOffset.Len);
			WriteInt32(iSegmentOffset - PacketHeaderOffset.Segment, iSegmentOffset + SegmentHeaderOffset.Offset);
			WriteInt16(0, iSegmentOffset + SegmentHeaderOffset.NoOfParts);
			WriteInt16(++sSegments, iSegmentOffset + SegmentHeaderOffset.OwnIndex);
			WriteByte(SegmKind.Cmd, iSegmentOffset + SegmentHeaderOffset.SegmKind);

			// request segment
			WriteByte(kind, iSegmentOffset + SegmentHeaderOffset.MessType);
			WriteByte(byCurrentSqlMode, iSegmentOffset + SegmentHeaderOffset.SqlMode);
			WriteByte(Producer.UserCmd, iSegmentOffset + SegmentHeaderOffset.Producer);
			WriteByte((byte)(autocommit ? 1 : 0), iSegmentOffset + SegmentHeaderOffset.CommitImmediately);
			WriteByte(0, iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning);
			WriteByte(0, iSegmentOffset + SegmentHeaderOffset.Prepare);
			WriteByte(0, iSegmentOffset + SegmentHeaderOffset.WithInfo);
			WriteByte(0, iSegmentOffset + SegmentHeaderOffset.MassCmd);
			WriteByte((byte)(parseagain ? 1 : 0), iSegmentOffset + SegmentHeaderOffset.ParsingAgain);
			WriteByte(0, iSegmentOffset + SegmentHeaderOffset.CommandOptions);

			iReplyReserve += (sSegments == 2) ? Consts.ReserveFor2ndSegment : Consts.ReserveForReply;
		}

		private void CloseSegment()
		{
            if (iSegmentOffset == -1)
            {
                return;
            }

			ClosePart();
			WriteInt32(iSegmentLength, iSegmentOffset + SegmentHeaderOffset.Len);
			WriteInt16(sSegmentParts, iSegmentOffset + SegmentHeaderOffset.NoOfParts);
			iLength += iSegmentLength;
			iSegmentOffset = -1;
			iSegmentLength = -1;
			sSegmentParts = -1;
			return;
		}

		#endregion
	}

	#endregion

	#region "MaxDB Unicode Request Packet"
	internal class MaxDBUnicodeRequestPacket : MaxDBRequestPacket
	{
		public MaxDBUnicodeRequestPacket(byte[] data, string appID, string appVersion)
			: base(data, Consts.IsLittleEndian ? Consts.UnicodeSwapClient : Consts.UnicodeClient, appID, appVersion)
		{
		}

		public override void AddDataString(string data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

			WriteByte(Consts.DefinedUnicode, DataPos);
            if (Consts.IsLittleEndian)
            {
                WriteBytes(Encoding.Unicode.GetBytes(data), DataPos + 1, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
            else
            {
                WriteBytes(Encoding.BigEndianUnicode.GetBytes(data), DataPos + 1, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }

			iPartLength += data.Length * Consts.UnicodeWidth + 1;
		}

		public override void AddString(string data)
		{
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            if (Consts.IsLittleEndian)
            {
                WriteBytes(Encoding.Unicode.GetBytes(data), DataPos, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
            else
            {
                WriteBytes(Encoding.BigEndianUnicode.GetBytes(data), DataPos, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }

			iPartLength += data.Length * Consts.UnicodeWidth;
		}

		public override void InitDbsCommand(bool autoCommit, string cmd)
		{
			InitDbsCommand(cmd, true, autoCommit, true);
		}

		public override bool InitDbsCommand(string command, bool reset, bool autoCommit)
		{
			return InitDbsCommand(command, reset, autoCommit, true);
		}
	}
	#endregion

	#region "MaxDB Reply Packet"

	internal class MaxDBReplyPacket : MaxDBPacket
	{
		public MaxDBReplyPacket(byte[] data)
			: base(data, 0, data[PacketHeaderOffset.MessSwap] == SwapMode.Swapped)
		{
			iSegmentOffset = PacketHeaderOffset.Segment;
			iCurrentSegment = 1;
			ClearPartCache();
		}

		#region "Part Operations"

		public int FindPart(int requestedKind)
		{
			iPartOffset = -1;
			iPartIndex = -1;
			int partsLeft = PartCount;
			while (partsLeft > 0)
			{
				NextPart();
				--partsLeft;
                if (PartType == requestedKind)
                {
                    return PartPos;
                }
			}

			throw new PartNotFoundException();
		}

		public bool ExistsPart(int requestedKind)
		{
			try
			{
				FindPart(requestedKind);
				return true;
			}
			catch (PartNotFoundException)
			{
				return false;
			}
		}

		public DataPartVariable VarDataPart
		{
			get
			{
				try
				{
					FindPart(PartKind.Vardata);
					return new DataPartVariable(new ByteArray(ReadBytes(PartDataPos, PartLength), 0, false), 1);//??? why DataPartVariable requires BigEndian
				}
				catch (PartNotFoundException)
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

				partAttributes = this.ReadByte(iPartOffset + PartHeaderOffset.Attributes);
				result = (partAttributes & PartAttributes.LastPacket_Ext) != 0;
				return result;
			}
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
					return int.Parse(ReadAscii(PartDataPos + 2200, 1), CultureInfo.InvariantCulture);
				}
				catch (PartNotFoundException)
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
					return int.Parse(ReadAscii(PartDataPos + 2201, 2), CultureInfo.InvariantCulture);
				}
				catch (PartNotFoundException)
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
					return int.Parse(ReadAscii(PartDataPos + 2203, 2), CultureInfo.InvariantCulture);
				}
				catch (PartNotFoundException)
				{
					return -1;
				}
			}
		}

		#endregion

		#region "Field and Column Operations"

		public virtual string[] ParseColumnNames()
		{
			int columnCount = PartArgsCount;
			string[] result = new string[columnCount];
			int nameLen;
			int pos = PartDataPos;

			for (int i = 0; i < columnCount; ++i)
			{
				nameLen = ReadByte(pos);
				result[i] = ReadAscii(pos + 1, nameLen);
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

			columnCount = PartArgsCount;
			result = new DBTechTranslator[columnCount];
			pos = PartDataPos;

			for (int i = 0; i < columnCount; ++i)
			{
				DBProcParameterInfo info = null;
                if (procParameters != null && procParameters.Length > i)
                {
                    info = procParameters[i];
                }

				mode = ReadByte(pos + ParamInfo.ModeOffset);
				ioType = ReadByte(pos + ParamInfo.IOTypeOffset);
				dataType = ReadByte(pos + ParamInfo.DataTypeOffset);
				frac = ReadByte(pos + ParamInfo.FracOffset);
				len = ReadInt16(pos + ParamInfo.LengthOffset);
				ioLen = ReadInt16(pos + ParamInfo.InOutLenOffset);
                if (isVardata && mode == ParamInfo.Input)
                {
                    bufpos = ReadInt16(pos + ParamInfo.ParamNoOffset);
                }
                else
                {
                    bufpos = ReadInt32(pos + ParamInfo.BufPosOffset);
                }

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
                    return spaceoption ? new SpaceOptionStringTranslator(mode, ioType, dataType, len, ioLen, bufpos) : new StringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.CHB:
                    if (procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE)
                    {
                        return new StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, false);
                    }
                    else
                    {
                        return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
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
                    if (isDBProcedure)
                    {
                        return new ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new ASCIIStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				case DataType.STRB:
				case DataType.LONGB:
                    if (isDBProcedure)
                    {
                        return new ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new BinaryStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				case DataType.UNICODE:
				case DataType.VARCHARUNI:
                    return spaceoption ? new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode) :
                        new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
				case DataType.LONGUNI:
				case DataType.STRUNI:
                    if (isDBProcedure)
                    {
                        return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				default:
					return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
			}
		}

		#endregion

		public override int ReturnCode
		{
			get
			{
				return ReadInt16(iSegmentOffset + SegmentHeaderOffset.ReturnCode);
			}
		}

		public string SqlState
		{
			get
			{
				return ReadAscii(iSegmentOffset + SegmentHeaderOffset.SqlState, 5);
			}
		}

		public virtual string ErrorMsg
		{
			get
			{
				try
				{
					FindPart(PartKind.ErrorText);
					return ReadAscii(PartDataPos, PartLength).Trim();
				}
				catch (PartNotFoundException)
				{
					return MaxDBMessages.Extract(MaxDBError.ERROR);
				}
			}
		}

		public int ResultCount(bool positionedAtPart)
		{
            if (iCachedResultCount == int.MinValue)
            {
                if (!positionedAtPart)
                {
                    try
                    {
                        FindPart(PartKind.ResultCount);
                    }
                    catch (PartNotFoundException)
                    {
                        return iCachedResultCount--;
                    }
                }

                return iCachedResultCount = VDNNumber.Number2Int(ReadDataBytes(iPartOffset + PartHeaderOffset.Data, PartLength));
            }
            else
            {
                return iCachedResultCount;
            }
		}

		public int SessionID
		{
			get
			{
				try
				{
					FindPart(PartKind.SessionInfoReturned);
					return Clone(Offset, false).ReadInt32(PartDataPos + 1);//session ID is always BigEndian
				}
				catch (PartNotFoundException)
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
				catch (PartNotFoundException)
				{
					return false;
				}
			}
		}

		public int FuncCode
		{
			get
			{
				return ReadInt16(iSegmentOffset + SegmentHeaderOffset.FunctionCode);
			}
		}

		public int ErrorPos
		{
			get
			{
				return ReadInt32(iSegmentOffset + SegmentHeaderOffset.ErrorPos);
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
				catch (PartNotFoundException)
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
					switch (FuncCode)
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
							// keep result 
							break;
						default:
							result = 0;
							break;
					}
				}
				return result;
			}
		}

		public MaxDBException CreateException()
		{
			string state = SqlState;
			int rc = ReturnCode;
			string errmsg = ErrorMsg;
			int errorPos = ErrorPos;

            if (rc == -8000)
            {
                errmsg = MaxDBMessages.Extract(MaxDBError.COMMRESTARTREQUIRED);
            }

			return new MaxDBException(errmsg, state, rc, errorPos);
		}

		public byte[] ReadDataBytes(int pos, int len)
		{
			int defByte;

			defByte = ReadByte(pos);
            if (defByte == 0xFF)
            {
                return null;
            }

			return ReadBytes(pos + 1, len - 1);
		}

		public virtual string ReadString(int offset, int len)
		{
			return ReadAscii(offset, len);
		}

		public byte[][] ParseLongDescriptors()
		{
            if (!ExistsPart(PartKind.LongData))
            {
                return null;
            }

			int argCount = PartArgsCount;
			byte[][] result = new byte[argCount][];
			for (int i = 0; i < argCount; i++)
			{
				int pos = (i * (LongDesc.Size + 1)) + 1;
				result[i] = ReadBytes(PartDataPos + pos, LongDesc.Size);
			}

			return result;
		}
	}

	#endregion

	#region "MaxDB Unicode Reply Packet"

	internal class MaxDBUnicodeReplyPacket : MaxDBReplyPacket
	{
		public MaxDBUnicodeReplyPacket(byte[] data)
			: base(data)
		{
		}

		public override string ReadString(int offset, int len)
		{
			return ReadUnicode(offset, len);
		}

		public override string[] ParseColumnNames()
		{
			int columnCount = PartArgsCount;
			string[] result = new string[columnCount];
			int nameLen;
			int pos = PartDataPos;

			for (int i = 0; i < columnCount; ++i)
			{
				nameLen = ReadByte(pos);
				result[i] = ReadUnicode(pos + 1, nameLen);
				pos += nameLen + 1;
			}

			return result;
		}

		public override string ErrorMsg
		{
			get
			{
				try
				{
					FindPart(PartKind.ErrorText);
					return ReadUnicode(PartDataPos, PartLength).Trim();
				}
				catch (PartNotFoundException)
				{
					return MaxDBMessages.Extract(MaxDBError.ERROR);
				}
			}
		}

		protected override DBTechTranslator GetTranslator(int mode, int ioType, int dataType, int frac, int len,
				  int ioLen, int bufpos, bool spaceoption, bool isDBProcedure, DBProcParameterInfo procParamInfo)
		{
			switch (dataType)
			{
				case DataType.CHA:
				case DataType.CHE:
				case DataType.VARCHARA:
				case DataType.VARCHARE:
                    return spaceoption ? new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode) :
                        new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
				case DataType.CHB:
                    if (procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE)
                    {
                        return new StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, false);
                    }
                    else
                    {
                        return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				case DataType.VARCHARB:
					return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.BOOLEAN:
					return new BooleanTranslator(mode, ioType, dataType, len, ioLen, bufpos);
				case DataType.TIME:
					return new UnicodeTimeTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
				case DataType.DATE:
					return new UnicodeDateTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
				case DataType.TIMESTAMP:
					return new UnicodeTimestampTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
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
                    if (isDBProcedure)
                    {
                        return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				case DataType.STRB:
				case DataType.LONGB:
                    if (isDBProcedure)
                    {
                        return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new BinaryStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				case DataType.UNICODE:
				case DataType.VARCHARUNI:
                    return spaceoption ? new SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode) :
                        new UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, bSwapMode);
				case DataType.LONGUNI:
				case DataType.STRUNI:
                    if (isDBProcedure)
                    {
                        return new UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
                    else
                    {
                        return new UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                    }
				default:
					return new BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
			}
		}

	}

	#endregion
}
