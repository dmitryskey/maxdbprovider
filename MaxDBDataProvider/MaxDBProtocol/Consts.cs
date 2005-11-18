using System;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Offsets"

	internal struct HeaderOffset
	{
		public const int ActSendLen        =      0;   // INT4
		public const int ProtocolID        =      4;   // INT1
		public const int MessClass         =      5;   // INT1
		public const int RTEFlags          =      6;   // INT1
		public const int ResidualPackets   =      7;   // INT1
		public const int SenderRef         =      8;   // INT4
		public const int ReceiverRef       =     12;   // INT4
		public const int RTEReturnCode     =     16;   // INT2
		public const int Filler            =     18;   // INT2
		public const int MaxSendLen        =     20;   // INT4
		public const int END               =     24;
	}

	internal struct ConnectPacketOffset
	{
		public const int MessCode	     =	   0;   // C2
		public const int ConnectLength   =	   2;   // INT2
		public const int ServiceType     =	   4;   // INT1
		public const int OSType          =	   5;   // INT1
		public const int Filler1         =	   6;   // INT1
		public const int Filler2         =	   7;   // INT1
		public const int MaxSegmentSize  =	   8;   // INT4
		public const int MaxDataLen      =	  12;   // INT4
		public const int PacketSize      =	  16;   // INT4
		public const int MinReplySize    =	  20;   // INT4
		public const int ServerDB        =	  24;   // C8
		public const int ClientDB        =	  32;   // C8
		public const int VarPart		 =	  40;   // C256
		public const int END             =	 296;
	}

	//
	// offsets of vsp001::tsp1_packet
	//
	internal struct PacketHeaderOffset
	{
		public const int MessCode		=	0;  // enum1
		public const int MessSwap		=	1;  // enum1
		public const int Filler1		=   2;  // int2
		public const int ApplVersion	=   4;  // c5
		public const int Appl			=   9;  // c3
		public const int VarpartSize	=   12; // int4
		public const int VarpartLen		=   16; // int4
		public const int Filler2		=   20; // int2
		public const int NoOfSegm		=   22; // int2
		public const int Filler3		=   24; // c8
		public const int Segment		=   32;
	}

	//
	// offsets of vsp001::tsp1_segment
	//
	internal struct SegmentHeaderOffset
	{
		// common header
		public const int Len				= 0;  // int4
		public const int Offs				= 4;  // int4
		public const int NoOfParts			= 8;  // int2
		public const int OwnIndex			= 10; // int2
		public const int SegmKind			= 12; // enum1
		// request segment
		public const int MessType			= 13; // enum1
		public const int SqlMode			= 14; // enum1
		public const int Producer			= 15; // enum1
		public const int CommitImmediateley = 16; // bool
		public const int IgnoreCostwarning	= 17; // bool
		public const int Prepare			= 18; // bool
		public const int WithInfo			= 19; // bool
		public const int MassCmd			= 20; // bool
		public const int ParsingAgain		= 21; // bool
		public const int CommandOptions		= 22; // enum1
		// reply segment
		public const int SqlState			= 13; // c5
		public const int Returncode			= 18; // int2
		public const int ErrorPos			= 20; // int4
		public const int ExternWarning		= 24; // set2
		public const int InternWarning		= 26; // set2
		public const int FunctionCode		= 28; // int2
		public const int TraceLevel			= 30; // int1
		public const int Part				= 40;
	}

	//
	// offsets of vsp001::tsp1_part
	//
	internal struct PartHeaderOffset 
	{
		public const int PartKind = 0;     // enum1
		public const int Attributes = 1;   // set1
		public const int ArgCount = 2;     // int2
		public const int SegmOffs = 4;     // int4
		public const int BufLen = 8;       // int4
		public const int BufSize = 12;     // int4
		public const int Data = 16;
	}

	#endregion

	//
	// copy of vsp001::tsp1_segment_kind
	//
	internal struct SegmKind 
	{
		public const byte Nil                =   0;
		public const byte Cmd                =   1;
		public const byte Return             =   2;
		public const byte Proccall           =   3;
		public const byte Procreply          =   4;
		public const byte LastSegmentKind    =   5;
	}

	internal struct SegmentCmdOption 
	{
		public const byte selfetch_off         =   1;
		public const byte scrollable_cursor_on =   2;
	}

	//
	// copy of vsp001::tsp1_sqlmode
	//
	internal struct SqlMode 
	{
		public const byte Nil               =   0;
		public const byte SessionSqlmode    =   1;
		public const byte Internal          =   2;
		public const byte Ansi              =   3;
		public const byte Db2               =   4;
		public const byte Oracle            =   5;
	}

	//
	// copy of vsp001::tsp1_producer
	//
	internal struct Producer 
	{
		public const byte Nil             =   0;
		public const byte UserCmd         =   1;
		public const byte InternalCmd     =   2;
		public const byte Kernel          =   3;
		public const byte Installation    =   4;
	}

	//
	// copy of vsp001::tsp1_cmd_mess_type.
	//
	internal struct CmdMessType 
	{
		public const byte Nil                     =   0;
		public const byte CmdLowerBound           =   1;
		public const byte Dbs                     =   2;
		public const byte Parse                   =   3;
		public const byte Getparse                =   4;
		public const byte Syntax                  =   5;
		public const byte Cfill1                  =   6;
		public const byte Cfill2                  =   7;
		public const byte Cfill3                  =   8;
		public const byte Cfill4                  =   9;
		public const byte Cfill5                  =  10;
		public const byte CmdUpperBound           =  11;
		public const byte NoCmdLowerBound         =  12;
		public const byte Execute                 =  13;
		public const byte Getexecute              =  14;
		public const byte Putval                  =  15;
		public const byte Getval                  =  16;
		public const byte Load                    =  17;
		public const byte Unload                  =  18;
		public const byte Ncfill1                 =  19;
		public const byte Ncfill2                 =  20;
		public const byte Ncfill3                 =  21;
		public const byte Ncfill4                 =  22;
		public const byte Ncfill5                 =  23;
		public const byte NoCmdUpperBound         =  24;
		public const byte Hello                   =  25;
		public const byte UtilLowerBound          =  26;
		public const byte Utility                 =  27;
		public const byte Incopy                  =  28;
		public const byte Ufill1                  =  29;
		public const byte Outcopy                 =  30;
		public const byte DiagOutcopy             =  31;
		public const byte Ufill3                  =  32;
		public const byte Ufill4                  =  33;
		public const byte Ufill5                  =  34;
		public const byte Ufill6                  =  35;
		public const byte Ufill7                  =  36;
		public const byte UtilUpperBound          =  37;
		public const byte SpecialsLowerBound      =  38;
		public const byte Switch                  =  39;
		public const byte Switchlimit             =  40;
		public const byte Buflength               =  41;
		public const byte Minbuf                  =  42;
		public const byte Maxbuf                  =  43;
		public const byte StateUtility            =  44;
		public const byte Sfill2                  =  45;
		public const byte Sfill3                  =  46;
		public const byte Sfill4                  =  47;
		public const byte Sfill5                  =  48;
		public const byte SpecialsUpperBound      =  49;
		public const byte WaitForEvent            =  50;
		public const byte ProcservLowerBound      =  51;
		public const byte ProcservCall            =  52;
		public const byte ProcservReply           =  53;
		public const byte ProcservFill1           =  54;
		public const byte ProcservFill2           =  55;
		public const byte ProcservFill3           =  56;
		public const byte ProcservFill4           =  57;
		public const byte ProcservFill5           =  58;
		public const byte ProcservUpperBound      =  59;
		public const byte LastCmdMessType           =  60;
	}

	//
	// copy of vsp001::tsp1_part_kind
	//
	internal struct PartKind 
	{
		public const byte Nil                     =   0;
		public const byte ApplParameterDescription =  1;
		public const byte Columnnames             =   2;
		public const byte Command                 =   3;
		public const byte ConvTablesReturned      =   4;
		public const byte Data                    =   5;
		public const byte Errortext               =   6;
		public const byte Getinfo                 =   7;
		public const byte Modulname               =   8;
		public const byte Page                    =   9;
		public const byte Parsid                  =  10;
		public const byte ParsidOfSelect          =  11;
		public const byte Resultcount             =  12;
		public const byte Resulttablename         =  13;
		public const byte Shortinfo               =  14;
		public const byte UserInfoReturned        =  15;
		public const byte Surrogate               =  16;
		public const byte Bdinfo                  =  17;
		public const byte Longdata                =  18;
		public const byte Tablename               =  19;
		public const byte SessionInfoReturned     =  20;
		public const byte OutputColsNoParameter   =  21;
		public const byte Key                     =  22;
		public const byte Serial                  =  23;
		public const byte RelativePos             =  24;
		public const byte AbapIStream             =  25;
		public const byte AbapOStream             =  26;
		public const byte AbapInfo                =  27;
		public const byte CheckpointInfo          =  28;
		public const byte Procid                  =  29;
		public const byte Long_Demand             =  30;
		public const byte MessageList             =  31;
		public const byte Vardata_Shortinfo       =  32;
		public const byte Vardata                 =  33;
		public const byte Feature                 =  34;
		public const byte Clientid                =  35;
	}


	internal struct Ports
	{
		public const int Default        =   7210;
		public const int DefaultSecure  =   7270;
		public const int DefaultNI      =   7269;
	}

	internal class CommError
	{
		public static string[] ErrorText = new string[14]
			{
				"OK",
				"Connection down, session released",
				"Tasklimit",
				"Timeout",
				"Crash",
				"Restart required",
				"Shutdown",
				"Send line down",
				"Receive line down",
				"Packet limit",
				"Released",
				"Would block",
				"Unknown Request",
				"Server or DB unknown"
			};
	}

	internal struct RTEReturnCodes
	{
		// rte return codes
		public const byte SQLOK                   =      0;
		public const byte SQLNOTOK                =      1;
		public const byte SQLTASKLIMIT            =      2;
		public const byte SQLTIMEOUT              =      3;
		public const byte SQLCRASH                =      4;
		public const byte SQLSTART_REQUIRED       =      5;
		public const byte SQLSHUTDOWN             =      6;
		public const byte SQLSEND_LINE_DOWN       =      7;
		public const byte SQLRECEIVE_LINE_DOWN    =      8;
		public const byte SQLPACKETLIMIT          =      9;
		public const byte SQLRELEASED             =     10;
		public const byte SQLWOULDBLOCK           =     11;
		public const byte SQLUNKNOWN_REQUEST      =     12;
		public const byte SQLSERVER_DB_UNKNOWN    =     13;
	}

	internal struct RSQLTypes
	{
		// request/reply types
		public const byte RTE_PROT_TCP          =      3;
		public const byte INFO_REQUEST_KEEP_ALIVE  =     50;
		public const byte INFO_REQUEST          =     51;
		public const byte INFO_REPLY            =     52;
		public const byte USER_CONN_REQUEST     =     61;
		public const byte USER_CONN_REPLY       =     62;
		public const byte USER_DATA_REQUEST     =     63;
		public const byte USER_DATA_REPLY       =     64;
		public const byte USER_CANCEL_REQUEST   =     65;
		public const byte USER_RELEASE_REQUEST  =     66;
		public const byte KERN_CONN_REQUEST     =     71;
		public const byte KERN_CONN_REPLY       =     72;
		public const byte KERN_DATA_REQUEST     =     73;
		public const byte KERN_DATA_REPLY       =     74;
		public const byte KERN_RELEASE_REQUEST  =     76;
		public const byte DUMP_REQUEST          =     81;
		public const byte CTRL_CONN_REQUEST     =     91;
		public const byte CTRL_CONN_REPLY       =     92;
		public const byte CTRL_CANCEL_REQUEST   =     93;
		public const byte NORMAL                =      0;
	}

	internal struct SwapMode
	{
		public const byte NotSwapped  = 1;
		public const byte Swapped	  =	2;
	}

	internal struct SQLType
	{
		// user types
		public const byte USER           = 0;
		public const byte ASYNC_USER     = 1;
		public const byte UTILITY        = 2;
		public const byte DISTRIBUTION   = 3;
		public const byte CONTROL        = 4;
		public const byte EVENT          = 5;
	}

	internal struct ArgType
	{
		//geo03.h
		public const byte PORT_NO         = 0x50;   // = P
		public const byte REM_PID         = 0x49;   // = I
		public const byte ACKNOWLEDGE     = 0x52;   // = R
		public const byte NODE            = 0x3E;   // = N
		public const byte DBROOT          = 0x64;   // = d
		public const byte SERVERPGM       = 0x70;   // = p
		public const byte AUTH_ALLOW      = 0x61;   // = a
		public const byte OMIT_REPLY_PART = 0x72;   // = r
	}

	/// <summary>
	/// Summary description for Consts.
	/// </summary>
	internal struct Consts
	{
		// some constants
		public const byte ASCIIClient              =      0;
		public const byte UnicodeSwapClient        =     19;
		public const byte RSQL_WIN32               =     13;
		public const byte RSQL_JAVA                = RSQL_WIN32;

		public const int AlignValue				   = 8;

		public const int ReserveFor2ndSegment = 8192; //8kB reserve size in order packet if more than 1 segment will be used
		public const int ReserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;
		public const int defaultmaxNumberOfSegm = 6; //default maximum number of segments for a request packet

		// other connect header constants
		public const int DBNameSize        =      8;
		public const int MinSize           =     64;   // for Unix vserver
	}
}
