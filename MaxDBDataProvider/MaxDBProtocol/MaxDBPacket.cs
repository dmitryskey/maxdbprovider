// Copyright © 2005-2006 Dmitry S. Kataev
// Copyright © 2002-2003 SAP AG
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.MaxDBProtocol
{
    using System;
    using System.Globalization;
    using System.Text;
    using MaxDB.Data.Utilities;

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
            this.WriteUInt32(0, HeaderOffset.ActSendLen);
            this.WriteByte(3, HeaderOffset.ProtocolID);
            this.WriteByte(msgClass, HeaderOffset.MessClass);
            this.WriteByte(0, HeaderOffset.RTEFlags);
            this.WriteByte(0, HeaderOffset.ResidualPackets);
            this.WriteInt32(senderRef, HeaderOffset.SenderRef);
            this.WriteInt32(0, HeaderOffset.ReceiverRef);
            this.WriteUInt16(0, HeaderOffset.RTEReturnCode);
            this.WriteUInt16(0, HeaderOffset.Filler);
            this.WriteInt32(0, HeaderOffset.MaxSendLen);
        }

        public void SetSendLength(int value)
        {
            this.WriteInt32(value, HeaderOffset.ActSendLen);
            this.WriteInt32(value, HeaderOffset.MaxSendLen);
        }

        public int MaxSendLength => this.ReadInt32(HeaderOffset.MaxSendLen);

        public int ActSendLength => this.ReadInt32(HeaderOffset.ActSendLen);

        public int PacketSender => this.ReadInt32(HeaderOffset.SenderRef);

        public virtual int ReturnCode => this.ReadInt16(HeaderOffset.RTEReturnCode);

        protected static int AlignSize(int value) => value + (value % Consts.AlignValue != 0 ? (Consts.AlignValue - (value % Consts.AlignValue)) : 0);

        #region "Segment operations"

        protected int SegmentLength => this.ReadInt32(this.iSegmentOffset + SegmentHeaderOffset.Len);

        protected int SegmentCount => this.ReadInt16(PacketHeaderOffset.NoOfSegm);

        protected void ClearPartCache()
        {
            this.iCachedResultCount = int.MinValue;
            this.iCachedPartCount = int.MinValue;

            int pc = this.ReadInt16(this.iSegmentOffset + SegmentHeaderOffset.NoOfParts);
            this.iPartIndices = new int[pc];
            int partofs = 0;
            for (int i = 0; i < pc; i++)
            {
                if (i == 0)
                {
                    partofs = this.iPartIndices[i] = this.iSegmentOffset + SegmentHeaderOffset.Part;
                }
                else
                {
                    int partlen = this.ReadInt32(partofs + PartHeaderOffset.BufLen);
                    partofs = this.iPartIndices[i] = partofs + AlignSize(partlen + PartHeaderOffset.Data);
                }
            }
        }

        public int FirstSegment()
        {
            int result = this.SegmentCount > 0 ? PacketHeaderOffset.Segment : -1;

            this.iSegmentOffset = result;
            this.iCurrentSegment = 1;
            this.ClearPartCache();
            return result;
        }

        public int NextSegment()
        {
            if (this.SegmentCount <= this.iCurrentSegment++)
            {
                return -1;
            }

            this.iSegmentOffset += this.SegmentLength;
            this.ClearPartCache();
            return this.iSegmentOffset;
        }

        public string DumpSegment(DateTime dt)
        {
            var dump = new StringBuilder();

            dump.Append("   ").Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.SegmKind).ToString(CultureInfo.InvariantCulture));
            dump.Append(" Segment ").Append(this.ReadInt16(this.iSegmentOffset + SegmentHeaderOffset.OwnIndex).ToString(CultureInfo.InvariantCulture));
            dump.Append(" at ").Append(this.ReadInt32(this.iSegmentOffset + SegmentHeaderOffset.Offset).ToString(CultureInfo.InvariantCulture));
            dump.Append("(").Append(this.ReadInt32(this.iSegmentOffset + SegmentHeaderOffset.Len).ToString(CultureInfo.InvariantCulture));
            dump.Append(" of ").Append((this.ReadInt32(PacketHeaderOffset.VarPartSize) - this.iSegmentOffset).ToString(CultureInfo.InvariantCulture));
            dump.Append(" bytes)\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

            switch (this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.SegmKind))
            {
                case SegmKind.Cmd:
                case SegmKind.Proccall:
                    dump.Append("        messtype: ").Append(CmdMessType.Name[this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.MessType)]);
                    dump.Append("  sqlmode: ").Append(SqlModeName.Value[this.ReadByte(this.iSegmentOffset +
                        SegmentHeaderOffset.SqlMode)].ToLower(CultureInfo.InvariantCulture));
                    dump.Append("  producer: ").Append(ProducerType.Name[this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.Producer)]);

                    dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture)).Append("        Options: ");
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.CommitImmediately) == 1 ? "commit " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning) == 1 ? "ignore costwarning " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.Prepare) == 1 ? "prepare " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.WithInfo) == 1 ? "with info " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.MassCmd) == 1 ? "mass cmd " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.ParsingAgain) == 1 ? "parsing again " : string.Empty);
                    break;
                case SegmKind.Return:
                case SegmKind.Procreply:
                    dump.Append("        RC: ").Append(this.ReadInt16(this.iSegmentOffset +
                        SegmentHeaderOffset.ReturnCode).ToString(CultureInfo.InvariantCulture));
                    dump.Append("  ").Append(this.ReadAscii(this.iSegmentOffset + SegmentHeaderOffset.SqlState,
                        SegmentHeaderOffset.ReturnCode - SegmentHeaderOffset.SqlState));
                    dump.Append("  (Pos ").Append(this.ReadInt32(this.iSegmentOffset +
                        SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
                    dump.Append(") Function ").Append(this.ReadInt16(this.iSegmentOffset +
                        SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
                    dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));
                    break;
                default:
                    dump.Append("unknown segment kind");
                    dump.Append("        messtype: ").Append(CmdMessType.Name[this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.MessType)]);
                    dump.Append("  sqlmode: ").Append(SqlModeName.Value[this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.SqlMode)]);
                    dump.Append("  producer: ").Append(ProducerType.Name[this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.Producer)]);

                    dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture)).Append("        Options: ");
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.CommitImmediately) == 1 ? "commit " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning) == 1 ? "ignore costwarning " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.Prepare) == 1 ? "prepare " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.WithInfo) == 1 ? "with info " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.MassCmd) == 1 ? "mass cmd " : string.Empty);
                    dump.Append(this.ReadByte(this.iSegmentOffset + SegmentHeaderOffset.ParsingAgain) == 1 ? "parsing again " : string.Empty);

                    dump.Append("        RC: ").Append(this.ReadInt16(this.iSegmentOffset +
                        SegmentHeaderOffset.ReturnCode).ToString(CultureInfo.InvariantCulture));
                    dump.Append("  ").Append(this.ReadAscii(this.iSegmentOffset + SegmentHeaderOffset.SqlState,
                        SegmentHeaderOffset.ReturnCode - SegmentHeaderOffset.SqlState));
                    dump.Append("  (Pos ").Append(this.ReadInt32(this.iSegmentOffset +
                        SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
                    dump.Append(") Function ").Append(this.ReadInt16(this.iSegmentOffset +
                        SegmentHeaderOffset.ErrorPos).ToString(CultureInfo.InvariantCulture));
                    dump.Append("\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));
                    break;
            }

            dump.Append("        ").Append(this.ReadInt16(this.iSegmentOffset +
                SegmentHeaderOffset.NoOfParts).ToString(CultureInfo.InvariantCulture));
            dump.Append(" parts:\n").Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

            this.ClearPartOffset();
            for (int i = 0; i < this.PartCount; i++)
            {
                this.NextPart();
                dump.Append(this.DumpPart(dt));
            }

            return dump.ToString();
        }

        #endregion

        #region "Part operations"

        public int PartLength => this.ReadInt32(this.iPartOffset + PartHeaderOffset.BufLen);

        public int PartSize => this.ReadInt32(this.iPartOffset + PartHeaderOffset.BufSize);

        public int PartPos => this.iPartOffset;

        public int PartDataPos => this.iPartOffset + PartHeaderOffset.Data;

        public int PartArgsCount => this.ReadInt16(this.iPartOffset + PartHeaderOffset.ArgCount);

        public int PartType => this.ReadByte(this.iPartOffset + PartHeaderOffset.PartKind);

        public int PartSegmentOffset => this.ReadInt32(this.iPartOffset + PartHeaderOffset.SegmOffset);

        public void ClearPartOffset()
        {
            this.iPartOffset = -1;
            this.iPartIndex = -1;
        }

        public int PartCount => this.iCachedPartCount == int.MinValue
                    ? (this.iCachedPartCount = this.ReadInt16(this.iSegmentOffset + SegmentHeaderOffset.NoOfParts))
                    : this.iCachedPartCount;

        public int NextPart() => this.iPartOffset = this.iPartIndices[++this.iPartIndex];

        public string DumpPart(DateTime dt)
        {
            var dump = new StringBuilder();

            string partkindname = "Unknown Part " + this.PartType.ToString(CultureInfo.InvariantCulture);
            if (this.PartType < PartKind.Name.Length)
            {
                partkindname = PartKind.Name[this.PartType] + " Part";
            }

            dump.Append("        ").Append(partkindname).Append(" ");
            dump.Append(this.PartArgsCount.ToString(CultureInfo.InvariantCulture)).Append(" Arguments (");
            dump.Append(this.PartLength.ToString(CultureInfo.InvariantCulture)).Append(" of ");
            dump.Append(this.PartSize.ToString(CultureInfo.InvariantCulture)).Append(") (Segment at ");
            dump.Append(this.PartSegmentOffset.ToString(CultureInfo.InvariantCulture)).Append(")\n")
                .Append(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture));

            byte[] data = this.ReadBytes(this.PartDataPos, this.PartLength);

            for (int i = 0; i <= data.Length / 0x10; i++)
            {
                dump.Append((i * 0x10).ToString("x", CultureInfo.InvariantCulture).PadLeft(8)).Append("  ");

                int tailLen = Math.Min(0x10, 0x10 - (((i + 1) * 0x10) - data.Length));

                for (int k = 0; k < tailLen; k++)
                {
                    dump.Append(data[(i * 0x10) + k].ToString("x2", CultureInfo.InvariantCulture)).Append(" ");
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
            var dump = new StringBuilder();

            dump.Append(Consts.MessageCode[this.ReadByte(PacketHeaderOffset.MessCode)]).Append(" ");
            dump.Append(SwapMode.SwapType[this.ReadByte(PacketHeaderOffset.MessSwap)]).Append(" swap ");
            dump.Append(this.ReadAscii(PacketHeaderOffset.Appl,
                PacketHeaderOffset.VarPartSize - PacketHeaderOffset.Appl).ToString(CultureInfo.InvariantCulture)).Append("-");
            dump.Append(this.ReadAscii(PacketHeaderOffset.ApplVersion,
                PacketHeaderOffset.Appl - PacketHeaderOffset.ApplVersion).ToString(CultureInfo.InvariantCulture));
            dump.Append(" (transfer len ").Append((this.ReadInt32(PacketHeaderOffset.VarPartLen) +
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
            this.WriteByte(Consts.ASCIIClient, HeaderOffset.END + ConnectPacketOffset.MessCode);
            this.WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, HeaderOffset.END + ConnectPacketOffset.MessCode + 1);
            this.WriteUInt16(ConnectPacketOffset.END, HeaderOffset.END + ConnectPacketOffset.ConnectLength);
            this.WriteByte(SqlType.USER, HeaderOffset.END + ConnectPacketOffset.ServiceType);
            this.WriteByte(Consts.RSQL_DOTNET, HeaderOffset.END + ConnectPacketOffset.OSType);
            this.WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler1);
            this.WriteByte(0, HeaderOffset.END + ConnectPacketOffset.Filler2);
            this.WriteInt32(packetData.MaxSegmentSize, HeaderOffset.END + ConnectPacketOffset.MaxSegmentSize);
            this.WriteInt32(packetData.MaxDataLen, HeaderOffset.END + ConnectPacketOffset.MaxDataLen);
            this.WriteInt32(packetData.PacketSize, HeaderOffset.END + ConnectPacketOffset.PacketSize);
            this.WriteInt32(packetData.MinReplySize, HeaderOffset.END + ConnectPacketOffset.MinReplySize);
            if (packetData.DBName.Length > ConnectPacketOffset.DBNameSize)
            {
                packetData.DBName = packetData.DBName.Substring(0, ConnectPacketOffset.DBNameSize);
            }

            this.WriteAscii(packetData.DBName.PadRight(ConnectPacketOffset.DBNameSize), HeaderOffset.END + ConnectPacketOffset.ServerDB);
            this.WriteAscii("        ", HeaderOffset.END + ConnectPacketOffset.ClientDB);
            // fill out variable part
            this.WriteByte(4, this.PacketLength++);
            this.WriteByte(ArgType.REM_PID, this.PacketLength++);
            this.WriteAscii("0", this.PacketLength++);
            this.WriteByte(0, this.PacketLength++);
            // add port number
            this.WriteByte(4, this.PacketLength++);
            this.WriteByte(ArgType.PORT_NO, this.PacketLength++);
            this.WriteUInt16((ushort)packetData.Port, this.PacketLength);
            this.PacketLength += 2;
            // add aknowledge flag
            this.WriteByte(3, this.PacketLength++);
            this.WriteByte(ArgType.ACKNOWLEDGE, this.PacketLength++);
            this.WriteByte(0, this.PacketLength++);
            // add omit reply part flag
            this.WriteByte(3, this.PacketLength++);
            this.WriteByte(ArgType.OMIT_REPLY_PART, this.PacketLength++);
            this.WriteByte(1, this.PacketLength++);
        }

        public void FillPacketLength()
        {
            const int packetMinLen = ConnectPacketOffset.MinSize;
            if (this.PacketLength < packetMinLen)
            {
                this.PacketLength = packetMinLen;
            }

            this.WriteUInt16((ushort)(this.PacketLength - HeaderOffset.END), HeaderOffset.END + ConnectPacketOffset.ConnectLength);
        }

        public void Close()
        {
            int currentLength = this.PacketLength;
            int requiredLength = ConnectPacketOffset.MinSize - HeaderOffset.END;

            if (currentLength < requiredLength)
            {
                this.PacketLength += requiredLength - currentLength;
            }

            this.WriteUInt16((ushort)(this.PacketLength - HeaderOffset.END), ConnectPacketOffset.ConnectLength);
        }

        public int PacketLength { get; private set; } = HeaderOffset.END + ConnectPacketOffset.VarPart;

        public int MaxDataLength => this.ReadInt32(HeaderOffset.END + ConnectPacketOffset.MaxDataLen);

        public int MinReplySize => this.ReadInt32(HeaderOffset.END + ConnectPacketOffset.MinReplySize);

        public int PacketSize => this.ReadInt32(HeaderOffset.END + ConnectPacketOffset.PacketSize);

        public string ClientDB
        {
            get
            {
                string dbname = this.ReadAscii(HeaderOffset.END + ConnectPacketOffset.ClientDB, 8);
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
                while (pos < this.byData.Length)
                {
                    byte len = this.ReadByte(pos);

                    if (len == 0)
                    {
                        break;
                    }

                    if (this.ReadByte(pos + 1) == ArgType.AUTH_ALLOW)
                    {
                        foreach (string authParam in Encoding.ASCII.GetString(this.byData, pos + 2, len - 3).Split(','))
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

        public bool IsLittleEndian => this.ReadByte(HeaderOffset.END + ConnectPacketOffset.MessCode + 1) == SwapMode.Swapped;
    }

    #endregion

    #region "MaxDB Request Packet"

    internal class MaxDBRequestPacket : MaxDBPacket
    {
        protected int iLength = PacketHeaderOffset.Segment;
        protected short sSegments;
        protected int iPartLength = -1;
        protected int iSegmentLength = -1;
        protected short sPartArguments = -1;
        protected short sSegmentParts = -1;
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

            this.WriteByte(clientEncoding, PacketHeaderOffset.MessCode);
            this.WriteByte(Consts.IsLittleEndian ? SwapMode.Swapped : SwapMode.NotSwapped, PacketHeaderOffset.MessSwap);
            this.WriteAscii(appVersion, PacketHeaderOffset.ApplVersion);
            this.WriteAscii(appID, PacketHeaderOffset.Appl);
            this.WriteInt32(data.Length - HeaderOffset.END - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartSize);
            this.iLength = PacketHeaderOffset.Segment;
        }

        public MaxDBRequestPacket(byte[] data, string appID, string appVersion)
            : this(data, Consts.ASCIIClient, appID, appVersion)
        {
        }

        protected int DataPos => this.iPartOffset + PartHeaderOffset.Data + this.iPartLength;

        public int PacketLength => this.iLength;

        public short PartArguments
        {
            get => this.sPartArguments;
            set => this.sPartArguments = value;
        }

        public static int ResultCountPartSize => PartHeaderOffset.Data + Consts.ResultCountSize + 8; // alignment

        public void AddNullData(int len)
        {
            this.WriteByte(255, this.DataPos);
            this.iPartLength += len + 1;
        }

        public void AddUndefinedResultCount()
        {
            this.NewPart(PartKind.ResultCount);
            this.AddNullData(iResultCountSize);
            this.sPartArguments++;
        }

        public byte SwitchSqlMode(byte newMode)
        {
            byte oldMode = this.byCurrentSqlMode;
            this.byCurrentSqlMode = newMode;
            return oldMode;
        }

        public void AddData(byte[] data)
        {
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            this.WriteByte(0, this.DataPos);
            this.WriteBytes(data, this.DataPos + 1);
            this.iPartLength += data.Length + 1;
        }

        public void AddBytes(byte[] data)
        {
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            this.WriteBytes(data, this.DataPos);
            this.iPartLength += data.Length;
        }

        public virtual void AddDataString(string data)
        {
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            this.WriteByte(Consts.DefinedAscii, this.DataPos);
            this.WriteBytes(Encoding.ASCII.GetBytes(data), this.DataPos + 1, data.Length, Consts.BlankBytes);
            this.iPartLength += data.Length + 1;
        }

        public virtual void AddString(string data)
        {
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            this.WriteBytes(Encoding.ASCII.GetBytes(data), this.DataPos, data.Length, Consts.BlankBytes);
            this.iPartLength += data.Length;
        }

        public void AddResultCount(int count)
        {
            this.NewPart(PartKind.ResultCount);
            byte[] fullNumber = VDNNumber.Long2Number(count);
            byte[] countNumber = new byte[iResultCountSize];
            Buffer.BlockCopy(fullNumber, 0, countNumber, 0, fullNumber.Length);
            this.AddData(countNumber);
            this.sPartArguments++;
        }

        public bool DropParseId(byte[] parseId, bool reset)
        {
            if (parseId == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "pid"));
            }

            if (reset)
            {
                this.Reset();
            }
            else
            {
                if (this.iSegmentOffset != -1)
                {
                    this.CloseSegment();
                }

                int remainingSpace = this.Length - this.iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
                    - this.iReplyReserve - Consts.ReserveForReply - strDropCmd.Length
                    - SegmentHeaderOffset.Part - PartHeaderOffset.Data - parseId.Length;
                if (remainingSpace <= 0 || this.sSegments >= this.iMaxNumberOfSegment)
                {
                    return false;
                }
            }

            this.NewSegment(CmdMessType.Dbs, false, false);
            this.NewPart(PartKind.Command);
            this.sPartArguments = 1;
            this.AddString(strDropCmd);
            this.NewPart(PartKind.ParseId);
            this.sPartArguments = 1;
            this.AddBytes(parseId);
            return true;
        }

        public bool DropParseIdAddToParseIdPart(byte[] parseId)
        {
            if (parseId == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "pid"));
            }

            int remainingSpace = this.Length - this.iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
                - this.iReplyReserve - Consts.ReserveForReply - this.iPartLength - parseId.Length;
            if (remainingSpace <= 0)
            {
                return false;
            }

            this.AddBytes(parseId);
            this.sPartArguments++;
            return true;
        }

        public void Init(short maxSegment)
        {
            this.Reset();
            this.iMaxNumberOfSegment = maxSegment;
        }

        private void Reset()
        {
            this.iLength = PacketHeaderOffset.Segment;
            this.sSegments = 0;
            this.iSegmentOffset = -1;
            this.iSegmentLength = -1;
            this.sSegmentParts = -1;
            this.iPartOffset = -1;
            this.iPartLength = -1;
            this.sPartArguments = -1;
            this.iReplyReserve = 0;
        }

        public void Close()
        {
            this.CloseSegment();
            this.WriteInt32(this.iLength - PacketHeaderOffset.Segment, PacketHeaderOffset.VarPartLen);
            this.WriteInt16(this.sSegments, PacketHeaderOffset.NoOfSegm);
        }

        public virtual void InitDbsCommand(bool autoCommit, string cmd) => this.InitDbsCommand(cmd, true, autoCommit, false);

        public virtual bool InitDbsCommand(string command, bool reset, bool autoCommit) => this.InitDbsCommand(command, reset, autoCommit, false);

        public bool InitDbsCommand(string command, bool reset, bool autoCommit, bool unicode)
        {
            if (command == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "command"));
            }

            if (!reset)
            {
                this.CloseSegment();
                if (this.byData.Length - HeaderOffset.END - this.iLength - SegmentHeaderOffset.Part - PartHeaderOffset.Data
                    - this.iReplyReserve - Consts.ReserveForReply < command.Length || this.sSegments >= this.iMaxNumberOfSegment)
                {
                    return false;
                }
            }

            this.InitDbs(reset, autoCommit);
            if (unicode)
            {
                this.WriteUnicode(command, this.DataPos);
                this.iPartLength += command.Length * Consts.UnicodeWidth;
            }
            else
            {
                this.WriteAscii(command, this.DataPos);
                this.iPartLength += command.Length;
            }

            this.sPartArguments = 1;
            return true;
        }

        public int InitParseCommand(string cmd, bool reset, bool parseAgain)
        {
            this.InitParse(reset, parseAgain);
            this.AddString(cmd);
            this.sPartArguments = 1;
            return this.iPartLength;
        }

        public void InitDbs(bool autoCommit) => this.InitDbs(true, autoCommit);

        public void InitDbs(bool reset, bool autoCommit)
        {
            if (reset)
            {
                this.Reset();
            }

            this.NewSegment(CmdMessType.Dbs, autoCommit, false);
            this.NewPart(PartKind.Command);
            this.sPartArguments = 1;
        }

        public void InitParse(bool reset, bool parseAgain)
        {
            if (reset)
            {
                this.Reset();
            }

            this.NewSegment(CmdMessType.Parse, false, parseAgain);
            this.NewPart(PartKind.Command);
            return;
        }

        public bool InitChallengeResponse(string user, byte[] challenge)
        {
            this.InitDbsCommand(false, "CONNECT " + user + "  AUTHENTICATION");
            this.ClosePart();
            DataPartVariable data = this.NewVarDataPart();
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
            this.Reset();
            this.NewSegment(CmdMessType.Execute, autoCommit, false);
            this.AddParseIdPart(parseId);
        }

        public void SetWithInfo() => this.WriteByte(1, this.iSegmentOffset + SegmentHeaderOffset.WithInfo);

        public void SetMassCommand() => this.WriteByte(1, this.iSegmentOffset + SegmentHeaderOffset.MassCmd);

        #region "Part operations"

        public DataPart InitGetValue(bool autoCommit)
        {
            this.Reset();
            this.NewSegment(CmdMessType.GetValue, autoCommit, false);
            return this.NewDataPart(PartKind.LongData);
        }

        public DataPart InitPutValue(bool autoCommit)
        {
            this.Reset();
            this.NewSegment(CmdMessType.PutValue, autoCommit, false);
            return this.NewDataPart(PartKind.LongData);
        }

        private DataPartVariable NewVarDataPart()
        {
            int partDataOffs;
            this.NewPart(PartKind.Vardata);
            partDataOffs = this.iPartOffset + PartHeaderOffset.Data;
            return new DataPartVariable(this.Clone(partDataOffs, false), this); // ??? why DataPartVariable requires BigEndian?
        }

        public DataPart NewDataPart(bool varData) => varData ? this.NewVarDataPart() : this.NewDataPart();

        private DataPart NewDataPart() => this.NewDataPart(PartKind.Data);

        public DataPart NewDataPart(byte partKind)
        {
            this.NewPart(partKind);
            return new DataPartFixed(this.Clone(this.iPartOffset + PartHeaderOffset.Data), this);
        }

        public void NewPart(byte kind)
        {
            this.ClosePart();
            this.InitPart(kind);
        }

        private void InitPart(byte kind)
        {
            this.sSegmentParts++;
            this.iPartOffset = this.iSegmentOffset + this.iSegmentLength;
            this.iPartLength = 0;
            this.sPartArguments = 0;
            this.WriteByte(kind, this.iPartOffset + PartHeaderOffset.PartKind);
            this.WriteByte(0, this.iPartOffset + PartHeaderOffset.Attributes);
            this.WriteInt16(1, this.iPartOffset + PartHeaderOffset.ArgCount);
            this.WriteInt32(this.iSegmentOffset - PacketHeaderOffset.Segment, this.iPartOffset + PartHeaderOffset.SegmOffset);
            this.WriteInt32(PartHeaderOffset.Data, this.iPartOffset + PartHeaderOffset.BufLen);
            this.WriteInt32(this.byData.Length - HeaderOffset.END - this.iPartOffset, this.iPartOffset + PartHeaderOffset.BufSize);
        }

        public void AddPartAttr(byte attr)
        {
            int attrOffset = this.iPartOffset + PartHeaderOffset.Attributes;
            this.WriteByte((byte)(this.ReadByte(attrOffset) | attr), attrOffset);
        }

        public void AddClientProofPart(byte[] clientProof)
        {
            var data = this.NewVarDataPart();
            data.AddRow(2);
            data.WriteBytes(Encoding.ASCII.GetBytes(Crypt.ScramMD5Name), data.Extent);
            data.AddArg(data.Extent, 0);
            data.WriteBytes(clientProof, data.Extent);
            data.AddArg(data.Extent, 0);
            data.Close();
        }

        public void AddClientIdPart(string clientId)
        {
            this.NewPart(PartKind.ClientId);
            this.AddDataString(clientId);
            this.sPartArguments = 1;
        }

        public void AddFeatureRequestPart(byte[] features)
        {
            if (features == null)
            {
                return;
            }

            if (features.Length > 0)
            {
                this.NewPart(PartKind.Feature);
                this.AddBytes(features);
                this.PartArguments += (short)(features.Length / 2);
                this.ClosePart();
            }
        }

        public void AddParseIdPart(byte[] parseId)
        {
            if (parseId != null)
            {
                this.NewPart(PartKind.ParseId);
                this.AddBytes(parseId);
                this.sPartArguments = 1;
                this.ClosePart();
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
                this.NewPart(PartKind.ResultTableName);
                this.AddString(cursorName);
                this.sPartArguments++;
                this.ClosePart();
            }
        }

        private void ClosePart() => this.ClosePart(this.iPartLength, this.sPartArguments);

        public void ClosePart(int extent, short args)
        {
            if (this.iPartOffset == -1)
            {
                return;
            }

            this.WriteInt32(extent, this.iPartOffset + PartHeaderOffset.BufLen);
            this.WriteInt16(args, this.iPartOffset + PartHeaderOffset.ArgCount);
            this.iSegmentLength += AlignSize(extent + PartHeaderOffset.Data);
            this.iPartOffset = -1;
            this.iPartLength = -1;
            this.sPartArguments = -1;
        }

        #endregion

        #region "Segment operations"

        private void NewSegment(byte kind, bool autocommit, bool parseagain)
        {
            this.CloseSegment();

            this.iSegmentOffset = this.iLength;
            this.iSegmentLength = SegmentHeaderOffset.Part;
            this.sSegmentParts = 0;
            this.WriteInt32(0, this.iSegmentOffset + SegmentHeaderOffset.Len);
            this.WriteInt32(this.iSegmentOffset - PacketHeaderOffset.Segment, this.iSegmentOffset + SegmentHeaderOffset.Offset);
            this.WriteInt16(0, this.iSegmentOffset + SegmentHeaderOffset.NoOfParts);
            this.WriteInt16(++this.sSegments, this.iSegmentOffset + SegmentHeaderOffset.OwnIndex);
            this.WriteByte(SegmKind.Cmd, this.iSegmentOffset + SegmentHeaderOffset.SegmKind);

            // request segment
            this.WriteByte(kind, this.iSegmentOffset + SegmentHeaderOffset.MessType);
            this.WriteByte(this.byCurrentSqlMode, this.iSegmentOffset + SegmentHeaderOffset.SqlMode);
            this.WriteByte(Producer.UserCmd, this.iSegmentOffset + SegmentHeaderOffset.Producer);
            this.WriteByte((byte)(autocommit ? 1 : 0), this.iSegmentOffset + SegmentHeaderOffset.CommitImmediately);
            this.WriteByte(0, this.iSegmentOffset + SegmentHeaderOffset.IgnoreCostwarning);
            this.WriteByte(0, this.iSegmentOffset + SegmentHeaderOffset.Prepare);
            this.WriteByte(0, this.iSegmentOffset + SegmentHeaderOffset.WithInfo);
            this.WriteByte(0, this.iSegmentOffset + SegmentHeaderOffset.MassCmd);
            this.WriteByte((byte)(parseagain ? 1 : 0), this.iSegmentOffset + SegmentHeaderOffset.ParsingAgain);
            this.WriteByte(0, this.iSegmentOffset + SegmentHeaderOffset.CommandOptions);

            this.iReplyReserve += (this.sSegments == 2) ? Consts.ReserveFor2ndSegment : Consts.ReserveForReply;
        }

        private void CloseSegment()
        {
            if (this.iSegmentOffset == -1)
            {
                return;
            }

            this.ClosePart();
            this.WriteInt32(this.iSegmentLength, this.iSegmentOffset + SegmentHeaderOffset.Len);
            this.WriteInt16(this.sSegmentParts, this.iSegmentOffset + SegmentHeaderOffset.NoOfParts);
            this.iLength += this.iSegmentLength;
            this.iSegmentOffset = -1;
            this.iSegmentLength = -1;
            this.sSegmentParts = -1;
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

            this.WriteByte(Consts.DefinedUnicode, this.DataPos);
            if (Consts.IsLittleEndian)
            {
                this.WriteBytes(Encoding.Unicode.GetBytes(data), this.DataPos + 1, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
            else
            {
                this.WriteBytes(Encoding.BigEndianUnicode.GetBytes(data), this.DataPos + 1, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }

            this.iPartLength += (data.Length * Consts.UnicodeWidth) + 1;
        }

        public override void AddString(string data)
        {
            if (data == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "data"));
            }

            if (Consts.IsLittleEndian)
            {
                this.WriteBytes(Encoding.Unicode.GetBytes(data), this.DataPos, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }
            else
            {
                this.WriteBytes(Encoding.BigEndianUnicode.GetBytes(data), this.DataPos, data.Length * Consts.UnicodeWidth, Consts.BlankUnicodeBytes);
            }

            this.iPartLength += data.Length * Consts.UnicodeWidth;
        }

        public override void InitDbsCommand(bool autoCommit, string cmd) => this.InitDbsCommand(cmd, true, autoCommit, true);

        public override bool InitDbsCommand(string command, bool reset, bool autoCommit) => this.InitDbsCommand(command, reset, autoCommit, true);
    }
    #endregion

    #region "MaxDB Reply Packet"

    internal class MaxDBReplyPacket : MaxDBPacket
    {
        public MaxDBReplyPacket(byte[] data)
            : base(data, 0, data[PacketHeaderOffset.MessSwap] == SwapMode.Swapped)
        {
            this.iSegmentOffset = PacketHeaderOffset.Segment;
            this.iCurrentSegment = 1;
            this.ClearPartCache();
        }

        #region "Part Operations"

        public int FindPart(int requestedKind)
        {
            this.iPartOffset = -1;
            this.iPartIndex = -1;
            int partsLeft = this.PartCount;
            while (partsLeft > 0)
            {
                this.NextPart();
                --partsLeft;
                if (this.PartType == requestedKind)
                {
                    return this.PartPos;
                }
            }

            throw new PartNotFoundException();
        }

        public bool ExistsPart(int requestedKind)
        {
            try
            {
                this.FindPart(requestedKind);
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
                    this.FindPart(PartKind.Vardata);
                    return new DataPartVariable(new ByteArray(this.ReadBytes(this.PartDataPos, this.PartLength), 0, false), 1);// ??? why DataPartVariable requires BigEndian
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

                partAttributes = this.ReadByte(this.iPartOffset + PartHeaderOffset.Attributes);
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
                    this.FindPart(PartKind.SessionInfoReturned);

                    // offset 2200 is taken from order interface manual
                    return int.Parse(this.ReadAscii(this.PartDataPos + 2200, 1), CultureInfo.InvariantCulture);
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
                    this.FindPart(PartKind.SessionInfoReturned);

                    // offset 2202 is taken from order interface manual
                    return int.Parse(this.ReadAscii(this.PartDataPos + 2201, 2), CultureInfo.InvariantCulture);
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
                    this.FindPart(PartKind.SessionInfoReturned);

                    // offset 2204 is taken from order interface manual
                    return int.Parse(this.ReadAscii(this.PartDataPos + 2203, 2), CultureInfo.InvariantCulture);
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
            int columnCount = this.PartArgsCount;
            string[] result = new string[columnCount];
            int nameLen;
            int pos = this.PartDataPos;

            for (int i = 0; i < columnCount; ++i)
            {
                nameLen = this.ReadByte(pos);
                result[i] = this.ReadAscii(pos + 1, nameLen);
                pos += nameLen + 1;
            }

            return result;
        }

        // Extracts the short field info, and creates translator instances.
        public MaxDBTranslators.DBTechTranslator[] ParseShortFields(bool spaceoption, bool isDBProcedure, DBProcParameterInfo[] procParameters, bool isVardata)
        {
            int columnCount;
            MaxDBTranslators.DBTechTranslator[] result;
            int pos, mode, ioType, dataType, frac, len, ioLen, bufpos;

            columnCount = this.PartArgsCount;
            result = new MaxDBTranslators.DBTechTranslator[columnCount];
            pos = this.PartDataPos;

            for (int i = 0; i < columnCount; ++i)
            {
                DBProcParameterInfo info = null;
                if (procParameters != null && procParameters.Length > i)
                {
                    info = procParameters[i];
                }

                mode = this.ReadByte(pos + ParamInfo.ModeOffset);
                ioType = this.ReadByte(pos + ParamInfo.IOTypeOffset);
                dataType = this.ReadByte(pos + ParamInfo.DataTypeOffset);
                frac = this.ReadByte(pos + ParamInfo.FracOffset);
                len = this.ReadInt16(pos + ParamInfo.LengthOffset);
                ioLen = this.ReadInt16(pos + ParamInfo.InOutLenOffset);
                if (isVardata && mode == ParamInfo.Input)
                {
                    bufpos = this.ReadInt16(pos + ParamInfo.ParamNoOffset);
                }
                else
                {
                    bufpos = this.ReadInt32(pos + ParamInfo.BufPosOffset);
                }

                result[i] = this.GetTranslator(mode, ioType, dataType, frac, len, ioLen, bufpos, spaceoption, isDBProcedure, info);

                pos += ParamInfo.END;
            }

            return result;
        }

        protected virtual MaxDBTranslators.DBTechTranslator GetTranslator(int mode, int ioType, int dataType, int frac, int len,
            int ioLen, int bufpos, bool spaceoption, bool isDBProcedure, DBProcParameterInfo procParamInfo)
        {
            switch (dataType)
            {
                case DataType.CHA:
                case DataType.CHE:
                case DataType.VARCHARA:
                case DataType.VARCHARE:
                    return spaceoption ?
                        new MaxDBTranslators.SpaceOptionStringTranslator(mode, ioType, dataType, len, ioLen, bufpos) :
                        new MaxDBTranslators.StringTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.CHB:
                    return procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE
                        ? new MaxDBTranslators.StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, false)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.VARCHARB:
                    return new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.BOOLEAN:
                    return new MaxDBTranslators.BooleanTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.TIME:
                    return new MaxDBTranslators.TimeTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.DATE:
                    return new MaxDBTranslators.DateTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.TIMESTAMP:
                    return new MaxDBTranslators.TimestampTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.FIXED:
                case DataType.FLOAT:
                case DataType.VFLOAT:
                case DataType.SMALLINT:
                case DataType.INTEGER:
                    return new MaxDBTranslators.NumericTranslator(mode, ioType, dataType, len, frac, ioLen, bufpos);
                case DataType.STRA:
                case DataType.STRE:
                case DataType.LONGA:
                case DataType.LONGE:
                    return isDBProcedure
                        ? new MaxDBTranslators.ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.ASCIIStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.STRB:
                case DataType.LONGB:
                    return isDBProcedure
                        ? new MaxDBTranslators.ProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.BinaryStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.UNICODE:
                case DataType.VARCHARUNI:
                    return spaceoption ? new MaxDBTranslators.SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode) :
                        new MaxDBTranslators.UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.LONGUNI:
                case DataType.STRUNI:
                    return isDBProcedure
                        ? new MaxDBTranslators.UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                default:
                    return new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
            }
        }

        #endregion

        public override int ReturnCode => this.ReadInt16(this.iSegmentOffset + SegmentHeaderOffset.ReturnCode);

        public string SqlState => this.ReadAscii(this.iSegmentOffset + SegmentHeaderOffset.SqlState, 5);

        public virtual string ErrorMsg
        {
            get
            {
                try
                {
                    this.FindPart(PartKind.ErrorText);
                    return this.ReadAscii(this.PartDataPos, this.PartLength).Trim();
                }
                catch (PartNotFoundException)
                {
                    return MaxDBMessages.Extract(MaxDBError.ERROR);
                }
            }
        }

        public int ResultCount(bool positionedAtPart)
        {
            if (this.iCachedResultCount == int.MinValue)
            {
                if (!positionedAtPart)
                {
                    try
                    {
                        this.FindPart(PartKind.ResultCount);
                    }
                    catch (PartNotFoundException)
                    {
                        return this.iCachedResultCount--;
                    }
                }

                return this.iCachedResultCount = VDNNumber.Number2Int(this.ReadDataBytes(this.iPartOffset + PartHeaderOffset.Data, this.PartLength));
            }
            else
            {
                return this.iCachedResultCount;
            }
        }

        public int SessionID
        {
            get
            {
                try
                {
                    this.FindPart(PartKind.SessionInfoReturned);
                    return this.Clone(this.Offset, false).ReadInt32(this.PartDataPos + 1);// session ID is always BigEndian
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
                    this.FindPart(PartKind.SessionInfoReturned);
                    return this.ReadByte(this.PartDataPos) == 1;
                }
                catch (PartNotFoundException)
                {
                    return false;
                }
            }
        }

        public int FuncCode => this.ReadInt16(this.iSegmentOffset + SegmentHeaderOffset.FunctionCode);

        public int ErrorPos => this.ReadInt32(this.iSegmentOffset + SegmentHeaderOffset.ErrorPos);

        public byte[] Features
        {
            get
            {
                try
                {
                    this.FindPart(PartKind.Feature);
                    return this.ReadBytes(this.PartDataPos, this.PartLength);
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
                int result = this.ReturnCode;
                if (result == 100)
                {
                    switch (this.FuncCode)
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
            string state = this.SqlState;
            int rc = this.ReturnCode;
            string errmsg = this.ErrorMsg;
            int errorPos = this.ErrorPos;

            if (rc == -8000)
            {
                errmsg = MaxDBMessages.Extract(MaxDBError.COMMRESTARTREQUIRED);
            }

            return new MaxDBException(errmsg, state, rc, errorPos);
        }

        public byte[] ReadDataBytes(int pos, int len)
        {
            int defByte;

            defByte = this.ReadByte(pos);
            if (defByte == 0xFF)
            {
                return null;
            }

            return this.ReadBytes(pos + 1, len - 1);
        }

        public virtual string ReadString(int offset, int len) => this.ReadAscii(offset, len);

        public byte[][] ParseLongDescriptors()
        {
            if (!this.ExistsPart(PartKind.LongData))
            {
                return null;
            }

            int argCount = this.PartArgsCount;
            byte[][] result = new byte[argCount][];
            for (int i = 0; i < argCount; i++)
            {
                int pos = (i * (LongDesc.Size + 1)) + 1;
                result[i] = this.ReadBytes(this.PartDataPos + pos, LongDesc.Size);
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

        public override string ReadString(int offset, int len) => this.ReadUnicode(offset, len);

        public override string[] ParseColumnNames()
        {
            int columnCount = this.PartArgsCount;
            string[] result = new string[columnCount];
            int nameLen;
            int pos = this.PartDataPos;

            for (int i = 0; i < columnCount; ++i)
            {
                nameLen = this.ReadByte(pos);
                result[i] = this.ReadUnicode(pos + 1, nameLen);
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
                    this.FindPart(PartKind.ErrorText);
                    return this.ReadUnicode(this.PartDataPos, this.PartLength).Trim();
                }
                catch (PartNotFoundException)
                {
                    return MaxDBMessages.Extract(MaxDBError.ERROR);
                }
            }
        }

        protected override MaxDBTranslators.DBTechTranslator GetTranslator(int mode, int ioType, int dataType, int frac, int len,
                  int ioLen, int bufpos, bool spaceoption, bool isDBProcedure, DBProcParameterInfo procParamInfo)
        {
            switch (dataType)
            {
                case DataType.CHA:
                case DataType.CHE:
                case DataType.VARCHARA:
                case DataType.VARCHARE:
                    return spaceoption ? new MaxDBTranslators.SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode) :
                        new MaxDBTranslators.UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.CHB:
                    return procParamInfo != null && procParamInfo.ElementType == DBProcParameterInfo.STRUCTURE
                        ? new MaxDBTranslators.StructureTranslator(mode, ioType, dataType, len, ioLen, bufpos, false)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.VARCHARB:
                    return new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.BOOLEAN:
                    return new MaxDBTranslators.BooleanTranslator(mode, ioType, dataType, len, ioLen, bufpos);
                case DataType.TIME:
                    return new MaxDBTranslators.UnicodeTimeTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.DATE:
                    return new MaxDBTranslators.UnicodeDateTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.TIMESTAMP:
                    return new MaxDBTranslators.UnicodeTimestampTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.FIXED:
                case DataType.FLOAT:
                case DataType.VFLOAT:
                case DataType.SMALLINT:
                case DataType.INTEGER:
                    return new MaxDBTranslators.NumericTranslator(mode, ioType, dataType, len, frac, ioLen, bufpos);
                case DataType.STRA:
                case DataType.STRE:
                case DataType.LONGA:
                case DataType.LONGE:
                    return isDBProcedure
                        ? new MaxDBTranslators.UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.STRB:
                case DataType.LONGB:
                    return isDBProcedure
                        ? new MaxDBTranslators.UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.BinaryStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                case DataType.UNICODE:
                case DataType.VARCHARUNI:
                    return spaceoption ? new MaxDBTranslators.SpaceOptionUnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode) :
                        new MaxDBTranslators.UnicodeStringTranslator(mode, ioType, dataType, len, ioLen, bufpos, this.bSwapMode);
                case DataType.LONGUNI:
                case DataType.STRUNI:
                    return isDBProcedure
                        ? new MaxDBTranslators.UnicodeProcedureStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos)
                        : (MaxDBTranslators.DBTechTranslator)new MaxDBTranslators.UnicodeStreamTranslator(mode, ioType, dataType, len, ioLen, bufpos);

                default:
                    return new MaxDBTranslators.BytesTranslator(mode, ioType, dataType, len, ioLen, bufpos);
            }
        }

    }

    #endregion
}
