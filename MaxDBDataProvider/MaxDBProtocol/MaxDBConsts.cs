//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConsts.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
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

    #region "Offsets"

    /// <summary>
    /// Package header offsets.
    /// </summary>
    internal struct HeaderOffset
    {
        /// <summary>
        /// Actual package length.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const byte ActSendLen = 0;

        /// <summary>
        /// Protocol ID (always set to 3).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const byte ProtocolID = 4;

        /// <summary>
        /// Message class (<see cref="RSQLTypes"/>).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const byte MessClass = 5;

        /// <summary>
        /// RTE flags (always set to 0).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const byte RTEFlags = 6;

        /// <summary>
        /// Residual packets (always set to 0).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const byte ResidualPackets = 7;

        /// <summary>
        /// Sender reference.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const byte SenderRef = 8;

        /// <summary>
        /// Receiver reference.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const byte ReceiverRef = 12;

        /// <summary>
        /// RTE return code.
        /// </summary>
        /// <remarks>INT2.</remarks>
        public const byte RTEReturnCode = 16;

        /// <summary>
        /// Filler (always set to 0).
        /// </summary>
        /// <remarks>INT2.</remarks>
        public const byte Filler = 18;

        /// <summary>
        /// Max length of data to send.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const byte MaxSendLen = 20;

        /// <summary>
        /// End of header.
        /// </summary>
        public const byte END = 24;
    }

    /// <summary>
    /// Connection packet offsets.
    /// </summary>
    internal struct ConnectPacketOffset
    {
        /// <summary>
        /// LE/BE message code (<see cref="SwapMode"/>).
        /// </summary>
        /// <remarks>C2.</remarks>
        public const int MessCode = 0;

        /// <summary>
        /// Connction packet length.
        /// </summary>
        /// <remarks>INT2.</remarks>
        public const int ConnectLength = 2;

        /// <summary>
        /// Service type (<see cref="SqlType"/>).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const int ServiceType = 4;

        /// <summary>
        /// OS type (always set to RSQL_DOTNET = 13).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const int OSType = 5;

        /// <summary>
        /// First filler (always set to 0).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const int Filler1 = 6;

        /// <summary>
        /// Second filler (always set to 0).
        /// </summary>
        /// <remarks>INT1.</remarks>
        public const int Filler2 = 7;

        /// <summary>
        /// Max segment size.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const int MaxSegmentSize = 8;

        /// <summary>
        /// Max packet size.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const int MaxDataLen = 12;

        /// <summary>
        /// Packet size.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const int PacketSize = 16;

        /// <summary>
        /// Min reply size.
        /// </summary>
        /// <remarks>INT4.</remarks>
        public const int MinReplySize = 20;

        /// <summary>
        /// Server Database Name.
        /// </summary>
        /// <remarks>C8.</remarks>
        public const int ServerDB = 24;

        /// <summary>
        /// Client Database Name.
        /// </summary>
        /// <remarks>C8.</remarks>
        public const int ClientDB = 32;

        /// <summary>
        /// Variable part of the connection packet used for the auxiliary info.
        /// </summary>
        /// <remarks>C256.</remarks>
        public const int VarPart = 40;

        /// <summary>
        /// End of header offset.
        /// </summary>
        public const int END = 296;

        /// <summary>
        /// Other connect header constants for Unix vserver.
        /// </summary>
        public const int DBNameSize = 8;

        /// <summary>
        /// Minimal size.
        /// </summary>
        public const int MinSize = 64;
    }

    /// <summary>
    /// Offsets of vsp001::tsp1_packet.
    /// </summary>
    internal struct PacketHeaderOffset
    {
        public const byte MessCode = 0;  // enum1
        public const byte MessSwap = 1;  // enum1
        public const byte Filler1 = 2;  // int2
        public const byte ApplVersion = 4;  // c5
        public const byte Appl = 9;  // c3
        public const byte VarPartSize = 12; // int4
        public const byte VarPartLen = 16; // int4
        public const byte Filler2 = 20; // int2
        public const byte NoOfSegm = 22; // int2
        public const byte Filler3 = 24; // c8
        public const byte Segment = 32;
    }

    //
    // offsets of vsp001::tsp1_segment
    //
    internal struct SegmentHeaderOffset
    {
        // common header
        public const byte Len = 0;  // int4

        // common header
        public const byte Offset = 4;  // int4

        // common header
        public const byte NoOfParts = 8;  // int2

        // common header
        public const byte OwnIndex = 10; // int2

        // common header
        public const byte SegmKind = 12; // enum1

        // common header
        public const byte // request segment
MessType = 13; // enum1

        // common header
        public const byte SqlMode = 14; // enum1

        // common header
        public const byte Producer = 15; // enum1

        // common header
        public const byte CommitImmediately = 16; // bool

        // common header
        public const byte IgnoreCostwarning = 17; // bool

        // common header
        public const byte Prepare = 18; // bool

        // common header
        public const byte WithInfo = 19; // bool

        // common header
        public const byte MassCmd = 20; // bool

        // common header
        public const byte ParsingAgain = 21; // bool

        // common header
        public const byte CommandOptions = 22; // enum1

        // common header
        public const byte // reply segment
SqlState = 13; // c5

        // common header
        public const byte ReturnCode = 18; // int2

        // common header
        public const byte ErrorPos = 20; // int4

        // common header
        public const byte ExternWarning = 24; // set2

        // common header
        public const byte InternWarning = 26; // set2

        // common header
        public const byte FunctionCode = 28; // int2

        // common header
        public const byte TraceLevel = 30; // int1

        // common header
        public const byte Part = 40;
    }

    //
    // offsets of vsp001::tsp1_part
    //
    internal struct PartHeaderOffset
    {
        public const byte PartKind = 0;   // enum1
        public const byte Attributes = 1;   // set1
        public const byte ArgCount = 2;   // int2
        public const byte SegmOffset = 4;   // int4
        public const byte BufLen = 8;   // int4
        public const byte BufSize = 12;  // int4
        public const byte Data = 16;
    }

    #endregion

    //
    // copy of vsp001::tsp1_segment_kind
    //
    internal struct SegmKind
    {
        public const byte Nil = 0;
        public const byte Cmd = 1;
        public const byte Return = 2;
        public const byte Proccall = 3;
        public const byte Procreply = 4;
        public const byte LastSegmentKind = 5;
    }

    internal struct SegmentCmdOption
    {
        public const byte selfetchOff = 1;
        public const byte scrollableCursorOn = 2;
    }

    //
    // copy of vsp001::tsp1_producer
    //
    internal struct Producer
    {
        public const byte Nil = 0;
        public const byte UserCmd = 1;
        public const byte InternalCmd = 2;
        public const byte Kernel = 3;
        public const byte Installation = 4;
    }

    //
    // copy of vsp001::tsp1_cmd_mess_type.
    //
    internal struct CmdMessType
    {
        public const byte Nil = 0;
        public const byte CmdLowerBound = 1;
        public const byte Dbs = 2;
        public const byte Parse = 3;
        public const byte Getparse = 4;
        public const byte Syntax = 5;
        public const byte Cfill1 = 6;
        public const byte Cfill2 = 7;
        public const byte Cfill3 = 8;
        public const byte Cfill4 = 9;
        public const byte Cfill5 = 10;
        public const byte CmdUpperBound = 11;
        public const byte NoCmdLowerBound = 12;
        public const byte Execute = 13;
        public const byte GetExecute = 14;
        public const byte PutValue = 15;
        public const byte GetValue = 16;
        public const byte Load = 17;
        public const byte Unload = 18;
        public const byte Ncfill1 = 19;
        public const byte Ncfill2 = 20;
        public const byte Ncfill3 = 21;
        public const byte Ncfill4 = 22;
        public const byte Ncfill5 = 23;
        public const byte NoCmdUpperBound = 24;
        public const byte Hello = 25;
        public const byte UtilLowerBound = 26;
        public const byte Utility = 27;
        public const byte Incopy = 28;
        public const byte Ufill1 = 29;
        public const byte Outcopy = 30;
        public const byte DiagOutcopy = 31;
        public const byte Ufill3 = 32;
        public const byte Ufill4 = 33;
        public const byte Ufill5 = 34;
        public const byte Ufill6 = 35;
        public const byte Ufill7 = 36;
        public const byte UtilUpperBound = 37;
        public const byte SpecialsLowerBound = 38;
        public const byte Switch = 39;
        public const byte Switchlimit = 40;
        public const byte Buflength = 41;
        public const byte Minbuf = 42;
        public const byte Maxbuf = 43;
        public const byte StateUtility = 44;
        public const byte Sfill2 = 45;
        public const byte Sfill3 = 46;
        public const byte Sfill4 = 47;
        public const byte Sfill5 = 48;
        public const byte SpecialsUpperBound = 49;
        public const byte WaitForEvent = 50;
        public const byte ProcservLowerBound = 51;
        public const byte ProcservCall = 52;
        public const byte ProcservReply = 53;
        public const byte ProcservFill1 = 54;
        public const byte ProcservFill2 = 55;
        public const byte ProcservFill3 = 56;
        public const byte ProcservFill4 = 57;
        public const byte ProcservFill5 = 58;
        public const byte ProcservUpperBound = 59;
        public const byte LastCmdMessType = 60;

        public static readonly string[] Name =
        {
            "nil",
            "cmd_lower_bound",
            "dbs",
            "parse",
            "getparse",
            "syntax",
            "cfill1",
            "cfill2",
            "cfill3",
            "cfill4",
            "cfill5",
            "cmd_upper_bound",
            "no_cmd_lower_bound",
            "execute",
            "getexecute",
            "putval",
            "getval",
            "load",
            "unload",
            "ncfill1",
            "ncfill2",
            "ncfill3",
            "ncfill4",
            "ncfill5",
            "no_cmd_upper_bound",
            "hello",
            "util_lower_bound",
            "utility",
            "incopy",
            "ufill1",
            "outcopy",
            "diag_outcopy",
            "ufill3",
            "ufill4",
            "ufill5",
            "ufill6",
            "ufill7",
            "util_upper_bound",
            "specials_lower_bound",
            "switch",
            "switchlimit",
            "buflength",
            "minbuf",
            "maxbuf",
            "state_utility",
            "sfill2",
            "sfill3",
            "sfill4",
            "sfill5",
            "specials_upper_bound",
            "wait_for_event",
            "procserv_lower_bound",
            "procserv_call",
            "procserv_reply",
            "procserv_fill1",
            "procserv_fill2",
            "procserv_fill3",
            "procserv_fill4",
            "procserv_fill5",
            "procserv_upper_bound",
        };
    }

    //
    // copy of vsp001::tsp1_part_kind
    //
    internal struct PartKind
    {
        public const byte Nil = 0;
        public const byte ApplParameterDescription = 1;
        public const byte ColumnNames = 2;
        public const byte Command = 3;
        public const byte ConvTablesReturned = 4;
        public const byte Data = 5;
        public const byte ErrorText = 6;
        public const byte GetInfo = 7;
        public const byte Modulname = 8;
        public const byte Page = 9;
        public const byte ParseId = 10;
        public const byte ParsidOfSelect = 11;
        public const byte ResultCount = 12;
        public const byte ResultTableName = 13;
        public const byte ShortInfo = 14;
        public const byte UserInfoReturned = 15;
        public const byte Surrogate = 16;
        public const byte Bdinfo = 17;
        public const byte LongData = 18;
        public const byte TableName = 19;
        public const byte SessionInfoReturned = 20;
        public const byte OutputColsNoParameter = 21;
        public const byte Key = 22;
        public const byte Serial = 23;
        public const byte RelativePos = 24;
        public const byte AbapIStream = 25;
        public const byte AbapOStream = 26;
        public const byte AbapInfo = 27;
        public const byte CheckpointInfo = 28;
        public const byte Procid = 29;
        public const byte LongDemand = 30;
        public const byte MessageList = 31;
        public const byte VardataShortInfo = 32;
        public const byte Vardata = 33;
        public const byte Feature = 34;
        public const byte ClientId = 35;

        public static readonly string[] Name =
        {
            "Nil",
            "ApplParameterDescription",
            "ColumnNames",
            "Command",
            "ConvTablesReturned",
            "Data",
            "ErrorText",
            "GetInfo",
            "ModulName",
            "Page",
            "Parsid",
            "ParsidOfSelect",
            "ResultCount",
            "ResultTableName",
            "ShortInfo",
            "UserInfoReturned",
            "Surrogate",
            "Bdinfo",
            "LongData",
            "TableName",
            "SessionInfoReturned",
            "OutputColsNoParameter",
            "Key",
            "Serial",
            "RelativePos",
            "AbapIStream",
            "AbapOStream",
            "AbapInfo",
            "CheckpointInfo",
            "Procid",
            "LongDemand",
            "MessageList",
            "VardataShortinfo",
            "Vardata",
            "Feature",
            "Clientid",
        };
    }

    internal struct Feature
    {
        public const byte MultipleDropParseid = 1;
        public const byte SpaceOption = 2;
        public const byte VariableInput = 3;
        public const byte OptimizedStreams = 4;
        public const byte CheckScrollableOption = 5;
    }

    //
    // copy of vsp001::tsp1_part_attributes.
    //
    // The _E-values can be used to build a set by ORing them
    //
    internal struct PartAttributes
    {
        public const byte LastPacket = 0;
        public const byte NextPacket = 1;
        public const byte FirstPacket = 2;
        public const byte Fill3 = 3;
        public const byte Fill4 = 4;
        public const byte Fill5 = 5;
        public const byte Fill6 = 6;
        public const byte Fill7 = 7;
        public const byte LastPacketExt = 1;
        public const byte NextPacketExt = 2;
        public const byte FirstPacketExt = 4;
    }

    /// <summary>
    /// copy of gsp00::tsp00_LongDescBlock and related constants
    /// </summary>
    internal struct LongDesc
    {
        // tsp00_LdbChange
        public const byte UseTermchar = 0;

        // tsp00_LdbChange
        public const byte UseConversion = 1;

        // tsp00_LdbChange
        public const byte UseToAscii = 2;

        // tsp00_LdbChange
        public const byte UseUCS2Swap = 3;

        // tsp00_LdbChange
        // tsp00_ValMode
        public const byte DataPart = 0;

        // tsp00_LdbChange
        public const byte AllData = 1;

        // tsp00_LdbChange
        public const byte LastData = 2;

        // tsp00_LdbChange
        public const byte NoData = 3;

        // tsp00_LdbChange
        public const byte NoMoreData = 4;

        // tsp00_LdbChange
        public const byte LastPutval = 5;

        // tsp00_LdbChange
        public const byte DataTrunc = 6;

        // tsp00_LdbChange
        public const byte Close = 7;

        // tsp00_LdbChange
        public const byte Error = 8;

        // tsp00_LdbChange
        public const byte StartposInvalid = 9;

        // tsp00_LdbChange
        // infoset
        public const byte ExTrigger = 1;

        // tsp00_LdbChange
        public const byte WithLock = 2;

        // tsp00_LdbChange
        public const byte NoCLose = 4;

        // tsp00_LdbChange
        public const byte NewRec = 8;

        // tsp00_LdbChange
        public const byte IsComment = 16;

        // tsp00_LdbChange
        public const byte IsCatalog = 32;

        // tsp00_LdbChange
        public const byte Unicode = 64;

        // tsp00_LdbChange
        // state
        public const byte StateUseTermChar = 1;

        // tsp00_LdbChange
        public const byte StateStream = 1;

        // tsp00_LdbChange
        public const byte StateUseConversion = 2;

        // tsp00_LdbChange
        public const byte StateUseToAscii = 4;

        // tsp00_LdbChange
        public const byte StateUseUcs2Swap = 8;

        // tsp00_LdbChange
        public const byte StateShortScol = 16;

        // tsp00_LdbChange
        public const byte StateFirstInsert = 32;

        // tsp00_LdbChange
        public const byte StateCopy = 64;

        // tsp00_LdbChange
        public const byte StateFirstCall = 128;

        // tsp00_LdbChange
        public const byte 
// tsp00_LongDescBlock = RECORD
Descriptor = 0;   // c8

        // tsp00_LdbChange
        public const byte Tabid = 8;        // c8

        // tsp00_LdbChange
        public const byte MaxLen = 16;      // c4

        // tsp00_LdbChange
        public const byte InternPos = 20;   // i4

        // tsp00_LdbChange
        public const byte Infoset = 24;     // set1

        // tsp00_LdbChange
        public const byte State = 25;       // bool

        // tsp00_LdbChange
        public const byte unused1 = 26;     // c1

        // tsp00_LdbChange
        public const byte ValMode = 27;     // i1

        // tsp00_LdbChange
        public const byte ValInd = 28;      // i2

        // tsp00_LdbChange
        public const byte Unused = 30;      // i2

        // tsp00_LdbChange
        public const byte ValPos = 32;      // i4

        // tsp00_LdbChange
        public const byte ValLen = 36;      // i4

        // tsp00_LdbChange
        public const byte Size = 40;
    }

    /// <summary>
    /// Packet indicators.
    /// </summary>
    internal struct Packet
    {
        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte MaxOneByteLength = 245;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte Ignored = 250;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte SpecialNull = 251;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte BlobDescription = 252;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte DefaultValue = 253;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte NullValue = 254;

        /// <summary>
        /// Indicators for fields with variable length.
        /// </summary>
        public const byte TwiByteLength = 255;

        /// <summary>
        /// Property names used to identify fields.
        /// </summary>
        public const string MaxPasswordLenTag = "maxpasswordlen";
    }

    /// <summary>
    /// Garbage collection mode.
    /// </summary>
    internal struct GCMode
    {
        /// <summary>
        /// Control flag for garbage collection on execute. If this is
        /// set, old cursors/parse ids are sent <i>together with</i> the current
        /// statement for being dropped.
        /// </summary>
        public const int ALLOWED = 1;

        /// <summary>
        /// Control flag for garbage collection on execute. If this is
        /// set, old cursors/parse ids are sent <i>after</i> the current
        /// statement for being dropped.
        /// </summary>
        public const int DELAYED = 2;

        /// <summary>
        /// Control flag for garbage collection on execute. If this is
        /// set, nothing is done to drop cursors or parse ids.
        /// </summary>
        public const int NONE = 3;
    }

    internal struct FunctionCode
    {
        public const int Nil = 0;
        public const int CreateTable = 1;
        public const int SetRole = 2;
        public const int Insert = 3;
        public const int Select = 4;
        public const int Update = 5;
        public const int Delete = 9;
        public const int Explain = 27;
        public const int DBProcExecute = 34;
        public const int FetchFirst = 206;
        public const int FetchLast = 207;
        public const int FetchNext = 208;
        public const int FetchPrev = 209;
        public const int FetchPos = 210;
        public const int FetchSame = 211;
        public const int FetchRelative = 247;
        public const int Show = 216;
        public const int Describe = 224;
        public const int SelectInto = 244;
        public const int DBProcWithResultSetExecute = 248;
        public const int MSelect = 1004;
        public const int MDelete = 1009;
        public const int MFetchFirst = 1206;
        public const int MFetchLast = 1207;
        public const int MFetchNext = 1208;
        public const int MFetchPrev = 1209;
        public const int MFetchPos = 1210;
        public const int MFetchSame = 1211;
        public const int MSelectInto = 1244;
        public const int MFetchRelative = 1247;

        // copy of application codes coded in the 11th byte of parseid
        public const int none = 0;
        public const int commandExecuted = 1;
        public const int useAdbs = 2;
        public const int releaseFound = 10;
        public const int fastSelectDirPossible = 20;
        public const int notAllowedForProgram = 30;
        public const int closeFound = 40;
        public const int describeFound = 41;
        public const int fetchFound = 42;
        public const int mfetchFound = 43;
        public const int massSelectFound = 44;
        public const int reuseMassSelectFound = 46;
        public const int massCommand = 70;
        public const int mselectFound = 114;
        public const int forUpdMselectFound = 115;
        public const int reuseMselectFound = 116;
        public const int reuseUpdMselectFound = 117;

        public static readonly int[] massCmdAppCodes =
        {
            mfetchFound,
            massSelectFound,
            reuseMassSelectFound,
            massCommand,
            mselectFound,
            forUpdMselectFound,
            reuseMselectFound,
            reuseUpdMselectFound,
        };

        public static bool IsQuery(int code) =>
            code == Select || code == Show || code == DBProcWithResultSetExecute || code == Explain;
    }

    internal struct Ports
    {
        public const int Default = 7210;
        public const int DefaultSecure = 7270;
        public const int DefaultNI = 7269;
    }

    internal struct CommError
    {
        public static readonly string[] ErrorText =
        {
            MaxDBMessages.Extract(MaxDBError.COMMOK),
            MaxDBMessages.Extract(MaxDBError.COMMCONNECTDOWN),
            MaxDBMessages.Extract(MaxDBError.COMMTASKLIMIT),
            MaxDBMessages.Extract(MaxDBError.COMMTIMEOUT),
            MaxDBMessages.Extract(MaxDBError.COMMCRASH),
            MaxDBMessages.Extract(MaxDBError.COMMRESTARTREQUIRED),
            MaxDBMessages.Extract(MaxDBError.COMMSHUTDOWN),
            MaxDBMessages.Extract(MaxDBError.COMMSENDLINEDOWN),
            MaxDBMessages.Extract(MaxDBError.COMMRECVLINEDOWN),
            MaxDBMessages.Extract(MaxDBError.COMMPACKETLIMIT),
            MaxDBMessages.Extract(MaxDBError.COMMRELEASED),
            MaxDBMessages.Extract(MaxDBError.COMMWOULDBLOCK),
            MaxDBMessages.Extract(MaxDBError.COMMUNKNOWNREQUEST),
            MaxDBMessages.Extract(MaxDBError.COMMSERVERDBUNKNOWN),
        };
    }

    internal struct RTEReturnCodes
    {
        // rte return codes
        public const byte SQLOK = 0;

        // rte return codes
        public const byte SQLNOTOK = 1;

        // rte return codes
        public const byte SQLTASKLIMIT = 2;

        // rte return codes
        public const byte SQLTIMEOUT = 3;

        // rte return codes
        public const byte SQLCRASH = 4;

        // rte return codes
        public const byte SQLSTARTREQUIRED = 5;

        // rte return codes
        public const byte SQLSHUTDOWN = 6;

        // rte return codes
        public const byte SQLSENDLINEDOWN = 7;

        // rte return codes
        public const byte SQLRECEIVELINEDOWN = 8;

        // rte return codes
        public const byte SQLPACKETLIMIT = 9;

        // rte return codes
        public const byte SQLRELEASED = 10;

        // rte return codes
        public const byte SQLWOULDBLOCK = 11;

        // rte return codes
        public const byte SQLUNKNOWNREQUEST = 12;

        // rte return codes
        public const byte SQLSERVERDBUNKNOWN = 13;
    }

    internal struct RSQLTypes
    {
        // request/reply types
        public const byte RTEPROTTCP = 3;

        // request/reply types
        public const byte INFOREQUESTKEEPALIVE = 50;

        // request/reply types
        public const byte INFOREQUEST = 51;

        // request/reply types
        public const byte INFOREPLY = 52;

        // request/reply types
        public const byte USERCONNREQUEST = 61;

        // request/reply types
        public const byte USERCONNREPLY = 62;

        // request/reply types
        public const byte USERDATAREQUEST = 63;

        // request/reply types
        public const byte USERDATAREPLY = 64;

        // request/reply types
        public const byte USERCANCELREQUEST = 65;

        // request/reply types
        public const byte USERRELEASEREQUEST = 66;

        // request/reply types
        public const byte KERNCONNREQUEST = 71;

        // request/reply types
        public const byte KERNCONNREPLY = 72;

        // request/reply types
        public const byte KERNDATAREQUEST = 73;

        // request/reply types
        public const byte KERNDATAREPLY = 74;

        // request/reply types
        public const byte KERNRELEASEREQUEST = 76;

        // request/reply types
        public const byte DUMPREQUEST = 81;

        // request/reply types
        public const byte CTRLCONNREQUEST = 91;

        // request/reply types
        public const byte CTRLCONNREPLY = 92;

        // request/reply types
        public const byte CTRLCANCELREQUEST = 93;

        // request/reply types
        public const byte NORMAL = 0;
    }

    /// <summary>
    /// Little Endian/Big Endian byte order.
    /// </summary>
    internal struct SwapMode
    {
        /// <summary>
        /// Not Swapped order (BE).
        /// </summary>
        public const byte NotSwapped = 1;

        /// <summary>
        /// Swapped order (LE).
        /// </summary>
        public const byte Swapped = 2;

        /// <summary>
        /// Swap type description.
        /// </summary>
        public static readonly string[] SwapType = { "dummy", "normal", "full", "part" };
    }

    internal struct ProducerType
    {
        public static readonly string[] Name = { "nil", "user", "internal", "kernel", "installation" };
    }

    /// <summary>
    /// SQL Mode name.
    /// </summary>
    internal struct SqlModeName
    {
        public static readonly string[] Value = { "NULL", "SESSION", "INTERNAL", "ANSI", "DB2", "ORACLE", "SAPR3" };
    }

    /// <summary>
    /// SQL User type.
    /// </summary>
    internal struct SqlType
    {
        /// <summary>
        /// Regular User.
        /// </summary>
        public const byte USER = 0;

        /// <summary>
        /// Async user.
        /// </summary>
        public const byte ASYNCUSER = 1;

        /// <summary>
        /// Utility.
        /// </summary>
        public const byte UTILITY = 2;

        /// <summary>
        /// Distribution.
        /// </summary>
        public const byte DISTRIBUTION = 3;

        /// <summary>
        /// Control.
        /// </summary>
        public const byte CONTROL = 4;

        /// <summary>
        /// Event.
        /// </summary>
        public const byte EVENT = 5;
    }

    internal struct ArgType
    {
        // geo03.h
        public const byte PORTNO = 0x50;   // = P

        // geo03.h
        public const byte REMPID = 0x49;   // = I

        // geo03.h
        public const byte ACKNOWLEDGE = 0x52;   // = R

        // geo03.h
        public const byte NODE = 0x3E;   // = N

        // geo03.h
        public const byte DBROOT = 0x64;   // = d

        // geo03.h
        public const byte SERVERPGM = 0x70;   // = p

        // geo03.h
        public const byte AUTHALLOW = 0x61;   // = a

        // geo03.h
        public const byte OMITREPLYPART = 0x72;   // = r
    }

    internal struct DataType
    {
        public const byte MIN = 0;
        public const byte FIXED = MIN;
        public const byte FLOAT = 1;
        public const byte CHA = 2;
        public const byte CHE = 3;
        public const byte CHB = 4;
        public const byte ROWID = 5;
        public const byte STRA = 6;
        public const byte STRE = 7;
        public const byte STRB = 8;
        public const byte STRDB = 9;
        public const byte DATE = 10;
        public const byte TIME = 11;
        public const byte VFLOAT = 12;
        public const byte TIMESTAMP = 13;
        public const byte UNKNOWN = 14;
        public const byte NUMBER = 15;
        public const byte NONUMBER = 16;
        public const byte DURATION = 17;
        public const byte DBYTEEBCDIC = 18;
        public const byte LONGA = 19;
        public const byte LONGE = 20;
        public const byte LONGB = 21;
        public const byte LONGDB = 22;
        public const byte BOOLEAN = 23;
        public const byte UNICODE = 24;
        public const byte DTFILLER1 = 25;
        public const byte DTFILLER2 = 26;
        public const byte DTFILLER3 = 27;
        public const byte DTFILLER4 = 28;
        public const byte SMALLINT = 29;
        public const byte INTEGER = 30;
        public const byte VARCHARA = 31;
        public const byte VARCHARE = 32;
        public const byte VARCHARB = 33;
        public const byte STRUNI = 34;
        public const byte LONGUNI = 35;
        public const byte VARCHARUNI = 36;
        public const byte UDT = 37;
        public const byte ABAPTABHANDLE = 38;
        public const byte DWYDE = 39;
        public const byte MAX = DWYDE;

        public static readonly string[] StrValues =
        {
            "FIXED",
            "FLOAT",
            "CHAR ASCII",
            "CHAR EBCDIC",
            "CHAR BYTE",
            "ROWID",
            "STRA",
            "STRE",
            "STRB",
            "STRDB",
            "DATE",
            "TIME",
            "VFLOAT",
            "TIMESTAMP",
            "UNKNOWN",
            "NUMBER",
            "NONUMBER",
            "DURATION",
            "DBYTEEBCDIC",
            "LONG ASCII",
            "LONG EBCDIC",
            "LONG BYTE",
            "LONGDB",
            "BOOLEAN",
            "UNICODE",
            "DTFILLER1",
            "DTFILLER2",
            "DTFILLER3",
            "DTFILLER4",
            "SMALLINT",
            "INTEGER",
            "VARCHAR ASCII",
            "VARCHAR EBCDIC",
            "VARCHAR BYTE",
            "STRUNI",
            "LONG UNICODE",
            "VARCHAR UNICODE",
            "UDT",
            "ABAP STREAM",
            "DWYDE",
        };
    }

    // copies of tsp1_param_opt_type, tsp1_param_io_type, tsp1_param_info
    internal struct ParamInfo
    {
        // param modes, declared as set values
        public const int Mandatory = 1;

        // param modes, declared as set values
        public const int Optional = 2;

        // param modes, declared as set values
        public const int Default = 4;

        // param modes, declared as set values
        public const int EscapeChar = 8;

        // param modes, declared as set values
        public const int // param io types
Input = 0;

        // param modes, declared as set values
        public const int Output = 1;

        // param modes, declared as set values
        public const int InOut = 2;

        // param modes, declared as set values
        public const int // layout of tsp1_param_info
ModeOffset = 0;  // Set 1

        // param modes, declared as set values
        public const int IOTypeOffset = 1;  // enum 1

        // param modes, declared as set values
        public const int DataTypeOffset = 2;  // enum1

        // param modes, declared as set values
        public const int FracOffset = 3;  // int1

        // param modes, declared as set values
        public const int LengthOffset = 4;  // int2

        // param modes, declared as set values
        public const int InOutLenOffset = 6;  // int2

        // param modes, declared as set values
        public const int BufPosOffset = 8;  // int4

        // param modes, declared as set values
        public const int ParamNoOffset = 8;  // int2

        // param modes, declared as set values
        public const int ReadOnlyOffset = 10; // int1

        // param modes, declared as set values
        public const int SerialOffset = 11; // int1

        // param modes, declared as set values
        public const int END = 12;// The size of tsp1_param_info
    }

    internal struct StreamHandle
    {
        public const int HeaderLength = 20;
        public const int RowDef = 0;
        public const int TabHandle = 4;
        public const int  TabHandleReserved = 4;

        public const int TabHandleRowsize = 8;
        public const int TabHandleColCount = 10;
        public const int TabHandleRowCount = 12;
        public const int TabHandleABAPTabID = 16;
        public const int TabHandleColDesc0 = 20;
        public const int ColDescInOut = 0;

        public const int ColDescABAPType = 1;
        public const int ColDescDec = 2;
        public const int ColDescLength = 4;
        public const int ColDescOffset = 6;
        public const int ColDescSize = 8;

        public const int StreamTypeBool = 0;

        public const int StreamTypeInt1 = 1;
        public const int StreamTypeUInt1 = 2;
        public const int StreamTypeInt2 = 3;
        public const int StreamTypeUInt2 = 4;
        public const int StreamTypeInt4 = 5;
        public const int StreamTypeUInt4 = 6;
        public const int StreamTypeInt8 = 7;
        public const int StreamTypeUInt8 = 8;
        public const int StreamTypeFloat = 9;
        public const int StreamTypeDouble = 10;
        public const int StreamTypeInt = 12;
        public const int StreamTypeUInt = 13;
        public const int StreamTypeChar = 14;
        public const int StreamTypeWChar = 15;
        public const int StreamTypeUDT = 16;
        public const int StreamTypeWYDE = 17;
        public const int StreamIN = 0;

        public const int StreamOUT = 1;
        public const int StreamINOUT = 2;
    }

    internal enum FetchType
    {
        FIRST = 1,  // The fetch operation type of a FETCH FIRST.
        LAST = 2,   // The fetch operation type of a FETCH LAST.
        ABSOLUTE_UP = 3,    // The fetch operation type of a FETCH ABSOLUTE with an argument >1.
        ABSOLUTE_DOWN = 4,  // The fetch operation type of a FETCH ABSOLUTE with an argument <1.
        RELATIVE_UP = 5,    // The fetch operation type of a FETCH RELATIVE with an argument >1.
        RELATIVE_DOWN = 6,       // The fetch operation type of a FETCH RELATIVE with an argument <1.
    }

    internal enum PositionType
    {
        BEFORE_FIRST = 1,   // Constant indicating that the current position is <i>before the first row
        INSIDE = 2, // Constant indicating that the current position is at the result set.
        AFTER_LAST = 3, // Constant indicating that the current position is behind the last row.
        NOT_AVAILABLE = 4,   // Constant indicating that the current position is not available.
    }

    internal struct ConnectionStringParams
    {
        public const string DATASOURCE = "DATA SOURCE";

        public const string INITIALCATALOG = "INITIAL CATALOG";

        public const string USERID = "USER ID";

        public const string PASSWORD = "PASSWORD";

        public const string TIMEOUT = "TIMEOUT";

        public const string SPACEOPTION = "SPACE OPTION";

        public const string CACHE = "CACHE";

        public const string CACHELIMIT = "CACHE LIMIT";

        public const string CACHESIZE = "CACHE SIZE";

        public const string ENCRYPT = "ENCRYPT";

        public const string MODE = "MODE";

        public const string SSLCERTIFICATE = "SSL CERTIFICATE";

        public const string POOLING = "POOLING";

        public const string CONNECTIONLIFETIME = "CONNECTION LIFETIME";

        public const string MINPOOLSIZE = "MIN POOL SIZE";

        public const string MAXPOOLSIZE = "MAX POOL SIZE";

        public const string CODEPAGE = "CODE_PAGE";
    }

    /// <summary>
    /// Summary description for Consts.
    /// </summary>
    internal class Consts
    {
        public static bool IsLittleEndian => BitConverter.IsLittleEndian;

        public static int UnicodeWidth { get; } = Encoding.Unicode.GetByteCount(BlankChar);

        public const int FillBufSize = 1024;
        public const string BlankChar = " ";

        public static byte[] BlankBytes { get; } = InitializeBlankBytes;

        private static byte[] InitializeBlankBytes
        {
            get
            {
                byte[] blanks = new byte[FillBufSize];
                for (int i = 0; i < FillBufSize; i++)
                {
                    blanks[i] = Encoding.ASCII.GetBytes(BlankChar)[0];
                }

                return blanks;
            }
        }

        public static byte[] BlankUnicodeBytes { get; } = InitializeBlankUnicodeBytes;

        private static byte[] InitializeBlankUnicodeBytes
        {
            get
            {
                byte[] blanks = new byte[FillBufSize * UnicodeWidth];
                for (int i = 0; i < FillBufSize; i += UnicodeWidth)
                {
                    if (IsLittleEndian)
                    {
                        Encoding.Unicode.GetBytes(BlankChar).CopyTo(blanks, i);
                    }
                    else
                    {
                        Encoding.BigEndianUnicode.GetBytes(BlankChar).CopyTo(blanks, i);
                    }
                }

                return blanks;
            }
        }

        // some constants
        public const byte ASCIIClient = 0;
        public const byte UnicodeSwapClient = 19;
        public const byte UnicodeClient = 20;
        public const byte RSQLDOTNET = 13;
        public const byte DefinedAscii = 32;
        public const byte DefinedUnicode = 1;

        public static readonly string[] MessageCode =
        {
            "ascii",
            "ebcdic",
            "codeneutral",
            "unknown3",
            "unknown4",
            "unknown5",
            "unknown6",
            "unknown7",
            "unknown8",
            "unknown9",
            "unknown10",
            "unknown11",
            "unknown12",
            "unknown13",
            "unknown14",
            "unknown15",
            "unknown16",
            "unknown17",
            "unknown18",
            "unicode_swap",
            "unicode",
        };

        public static byte[] ZeroBytes { get; } = new byte[FillBufSize];

        public const int AlignValue = 8;

        public const int ReserveFor2ndSegment = 8192; // 8kB reserve size in order packet if more than 1 segment will be used
        public const int ReserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;
        public const int DefaultMaxNumberOfSegm = 6; // default maximum number of segments for a request packet
        public const int ResultCountSize = 6;

        public const string AppID = "ODB";
        public const string AppVersion = "70400"; // "10100";

        public const string CursorPrefix = "ADONET_CURSOR_";
        public const string TimeStampFormat = "yyyy-MM-dd hh:mm:ss.ffffff";

        public static string ToHexString(byte[] array, int offset = 0, int length = int.MaxValue)
        {
            if (array != null)
            {
                var result = new StringBuilder((array.Length - offset) * 2);
                for (int i = offset; i < array.Length && i < length; i++)
                {
                    result.Append(array[i].ToString("X2", CultureInfo.InvariantCulture));
                }

                return result.ToString();
            }
            else
            {
                return "NULL";
            }
        }
    }
}