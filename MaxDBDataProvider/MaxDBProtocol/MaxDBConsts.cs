// Copyright © 2005-2018 Dmitry S. Kataev
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
        public const byte selfetch_off = 1;
        public const byte scrollable_cursor_on = 2;
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
        public const byte MultipleDropParseid = 1,
            SpaceOption = 2,
            VariableInput = 3,
            OptimizedStreams = 4,
            CheckScrollableOption = 5;
    }

    //
    // copy of vsp001::tsp1_part_attributes.
    //
    // The _E-values can be used to build a set by ORing them
    //
    internal struct PartAttributes
    {
        public const byte LastPacket = 0,
            NextPacket = 1,
            FirstPacket = 2,
            Fill3 = 3,
            Fill4 = 4,
            Fill5 = 5,
            Fill6 = 6,
            Fill7 = 7,
            LastPacket_Ext = 1,
            NextPacket_Ext = 2,
            FirstPacket_Ext = 4;
    }

    /// <summary>
    /// copy of gsp00::tsp00_LongDescBlock and related constants
    /// </summary>
    internal struct LongDesc
    {
        // tsp00_LdbChange
        public const byte UseTermchar = 0,
            UseConversion = 1,
            UseToAscii = 2,
            UseUCS2_Swap = 3,

            // tsp00_ValMode
            DataPart = 0,
            AllData = 1,
            LastData = 2,
            NoData = 3,
            NoMoreData = 4,
            LastPutval = 5,
            DataTrunc = 6,
            Close = 7,
            Error = 8,
            StartposInvalid = 9,

            // infoset
            ExTrigger = 1,
            WithLock = 2,
            NoCLose = 4,
            NewRec = 8,
            IsComment = 16,
            IsCatalog = 32,
            Unicode = 64,

            // state
            StateUseTermChar = 1,
            StateStream = 1,
            StateUseConversion = 2,
            StateUseToAscii = 4,
            StateUseUcs2Swap = 8,
            StateShortScol = 16,
            StateFirstInsert = 32,
            StateCopy = 64,
            StateFirstCall = 128,

            // tsp00_LongDescBlock = RECORD
            Descriptor = 0,   // c8
            Tabid = 8,        // c8
            MaxLen = 16,      // c4
            InternPos = 20,   // i4
            Infoset = 24,     // set1
            State = 25,       // bool
            unused1 = 26,     // c1
            ValMode = 27,     // i1
            ValInd = 28,      // i2
            unused = 30,      // i2
            ValPos = 32,      // i4
            ValLen = 36,      // i4
            Size = 40;
    }

    internal struct Packet
    {
        // indicators for fields with variable length
        public const byte MaxOneByteLength = 245,
            Ignored = 250,
            SpecialNull = 251,
            BlobDescription = 252,
            DefaultValue = 253,
            NullValue = 254,
            TwiByteLength = 255;

        // property names used to identify fields
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
        public const int Nil = 0,
            CreateTable = 1,
            SetRole = 2,
            Insert = 3,
            Select = 4,
            Update = 5,
            Delete = 9,
            Explain = 27,
            DBProcExecute = 34,

            FetchFirst = 206,
            FetchLast = 207,
            FetchNext = 208,
            FetchPrev = 209,
            FetchPos = 210,
            FetchSame = 211,
            FetchRelative = 247,

            Show = 216,
            Describe = 224,
            Select_into = 244,
            DBProcWithResultSetExecute = 248,
            MSelect = 1004,
            MDelete = 1009,

            MFetchFirst = 1206,
            MFetchLast = 1207,
            MFetchNext = 1208,
            MFetchPrev = 1209,
            MFetchPos = 1210,
            MFetchSame = 1211,
            MSelect_into = 1244,
            MFetchRelative = 1247,

            // copy of application codes coded in the 11th byte of parseid
            none = 0,
            command_executed = 1,
            use_adbs = 2,
            release_found = 10,
            fast_select_dir_possible = 20,
            not_allowed_for_program = 30,
            close_found = 40,
            describe_found = 41,
            fetch_found = 42,
            mfetch_found = 43,
            mass_select_found = 44,
            reuse_mass_select_found = 46,
            mass_command = 70,
            mselect_found = 114,
            for_upd_mselect_found = 115,
            reuse_mselect_found = 116,
            reuse_upd_mselect_found = 117;

        public static readonly int[] massCmdAppCodes =
        {
            mfetch_found,
            mass_select_found,
            reuse_mass_select_found,
            mass_command,
            mselect_found,
            for_upd_mselect_found,
            reuse_mselect_found,
            reuse_upd_mselect_found,
        };

        public static bool IsQuery(int code) =>
            code == FunctionCode.Select || code == FunctionCode.Show || code == FunctionCode.DBProcWithResultSetExecute || code == FunctionCode.Explain;
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
        public const byte SQLSTART_REQUIRED = 5;

        // rte return codes
        public const byte SQLSHUTDOWN = 6;

        // rte return codes
        public const byte SQLSEND_LINE_DOWN = 7;

        // rte return codes
        public const byte SQLRECEIVE_LINE_DOWN = 8;

        // rte return codes
        public const byte SQLPACKETLIMIT = 9;

        // rte return codes
        public const byte SQLRELEASED = 10;

        // rte return codes
        public const byte SQLWOULDBLOCK = 11;

        // rte return codes
        public const byte SQLUNKNOWN_REQUEST = 12;

        // rte return codes
        public const byte SQLSERVER_DB_UNKNOWN = 13;
    }

    internal struct RSQLTypes
    {
        // request/reply types
        public const byte RTE_PROT_TCP = 3,
            INFO_REQUEST_KEEP_ALIVE = 50,
            INFO_REQUEST = 51,
            INFO_REPLY = 52,
            USER_CONN_REQUEST = 61,
            USER_CONN_REPLY = 62,
            USER_DATA_REQUEST = 63,
            USER_DATA_REPLY = 64,
            USER_CANCEL_REQUEST = 65,
            USER_RELEASE_REQUEST = 66,
            KERN_CONN_REQUEST = 71,
            KERN_CONN_REPLY = 72,
            KERN_DATA_REQUEST = 73,
            KERN_DATA_REPLY = 74,
            KERN_RELEASE_REQUEST = 76,
            DUMP_REQUEST = 81,
            CTRL_CONN_REQUEST = 91,
            CTRL_CONN_REPLY = 92,
            CTRL_CANCEL_REQUEST = 93,
            NORMAL = 0;
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
        public const byte PORT_NO = 0x50;   // = P

        // geo03.h
        public const byte REM_PID = 0x49;   // = I

        // geo03.h
        public const byte ACKNOWLEDGE = 0x52;   // = R

        // geo03.h
        public const byte NODE = 0x3E;   // = N

        // geo03.h
        public const byte DBROOT = 0x64;   // = d

        // geo03.h
        public const byte SERVERPGM = 0x70;   // = p

        // geo03.h
        public const byte AUTH_ALLOW = 0x61;   // = a

        // geo03.h
        public const byte OMIT_REPLY_PART = 0x72;   // = r
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
            "DWYDE"
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
        public const int // The size of tsp1_param_info
END = 12;
    }

    internal struct StreamHandle
    {
        public const int Header_Length = 20;
        public const int RowDef = 0;
        public const int TabHandle = 4;
        public const int 
TabHandle_Reserved = 4;
        public const int TabHandle_Rowsize = 8;
        public const int TabHandle_ColCount = 10;
        public const int TabHandle_RowCount = 12;
        public const int TabHandle_ABAPTabID = 16;
        public const int TabHandle_ColDesc_0 = 20;
        public const int 
ColDesc_InOut = 0;
        public const int ColDesc_ABAPType = 1;
        public const int ColDesc_Dec = 2;
        public const int ColDesc_Length = 4;
        public const int ColDesc_Offset = 6;
        public const int 
ColDesc_Size = 8;
        public const int 
StreamType_Bool = 0;
        public const int StreamType_Int1 = 1;
        public const int StreamType_UInt1 = 2;
        public const int StreamType_Int2 = 3;
        public const int StreamType_UInt2 = 4;
        public const int StreamType_Int4 = 5;
        public const int StreamType_UInt4 = 6;
        public const int StreamType_Int8 = 7;
        public const int StreamType_UInt8 = 8;
        public const int StreamType_Float = 9;
        public const int StreamType_Double = 10;
        public const int StreamType_Int = 12;
        public const int StreamType_UInt = 13;
        public const int StreamType_Char = 14;
        public const int StreamType_WChar = 15;
        public const int StreamType_UDT = 16;
        public const int StreamType_WYDE = 17;
        public const int 
Stream_IN = 0;
        public const int Stream_OUT = 1;
        public const int Stream_INOUT = 2;
    }

    internal enum FetchType
    {
        FIRST = 1,  // The fetch operation type of a FETCH FIRST.
        LAST = 2,   // The fetch operation type of a FETCH LAST.
        ABSOLUTE_UP = 3,    // The fetch operation type of a FETCH ABSOLUTE with an argument >1.
        ABSOLUTE_DOWN = 4,  // The fetch operation type of a FETCH ABSOLUTE with an argument <1.
        RELATIVE_UP = 5,    // The fetch operation type of a FETCH RELATIVE with an argument >1.
        RELATIVE_DOWN = 6       // The fetch operation type of a FETCH RELATIVE with an argument <1.
    }

    internal enum PositionType
    {
        BEFORE_FIRST = 1,   // Constant indicating that the current position is <i>before the first row
        INSIDE = 2, // Constant indicating that the current position is at the result set.
        AFTER_LAST = 3, // Constant indicating that the current position is behind the last row.
        NOT_AVAILABLE = 4   // Constant indicating that the current position is not available.
    }

    internal struct ConnectionStringParams
    {
        public const string DATA_SOURCE = "DATA SOURCE";

        public const string INITIAL_CATALOG = "INITIAL CATALOG";

        public const string USER_ID = "USER ID";

        public const string PASSWORD = "PASSWORD";

        public const string TIMEOUT = "TIMEOUT";

        public const string SPACE_OPTION = "SPACE OPTION";

        public const string CACHE = "CACHE";

        public const string CACHE_LIMIT = "CACHE LIMIT";

        public const string CACHE_SIZE = "CACHE SIZE";

        public const string ENCRYPT = "ENCRYPT";

        public const string MODE = "MODE";

        public const string SSL_CERTIFICATE = "SSL CERTIFICATE";

        public const string POOLING = "POOLING";

        public const string CONNECTION_LIFETIME = "CONNECTION LIFETIME";

        public const string MIN_POOL_SIZE = "MIN POOL SIZE";

        public const string MAX_POOL_SIZE = "MAX POOL SIZE";

        public const string CODE_PAGE = "CODE_PAGE";
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
        public const byte RSQL_DOTNET = 13;
        public const byte DefinedAscii = 32;
        public const byte DefinedUnicode = 1;

        public static readonly string[] MessageCode = {
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