using System;

namespace MaxDBDataProvider.MaxDBProtocol
{
	internal struct ConnectPacketOffset
	{
		public const int MessCode	     =	0;   // C2
		public const int ConnectLength  =	2;   // INT2
		public const int ServiceType    =	4;   // INT1
		public const int OSType         =	5;   // INT1
		public const int Filler1        =	6;   // INT1
		public const int Filler2        =	7;   // INT1
		public const int MaxSegmentSize =	8;   // INT4
		public const int MaxDataLen     =	12;   // INT4
		public const int PacketSize     =	16;   // INT4
		public const int MinReplySize   =	20;   // INT4
		public const int ServerDB       =	24;   // C8
		public const int ClientDB       =	32;   // C8
		public const int VarPart		 =    40;   // C256
		public const int END             =    296;

		// other connect header constants
		public const int DBNameSize        =      8;
		public const int MinSize            =     64;   // for Unix vserver
	}

	internal struct HeaderOffset
	{
		public const int ActSendLen          =      0;   // INT4
		public const int ProtocolID          =      4;   // INT1
		public const int MessClass           =      5;   // INT1
		public const int RTEFlags            =      6;   // INT1
		public const int ResidualPackets     =      7;   // INT1
		public const int SenderRef           =      8;   // INT4
		public const int ReceiverRef         =     12;   // INT4
		public const int RTEReturnCode       =     16;   // INT2
		public const int Filler              =     18;   // INT2
		public const int MaxSendLen          =     20;   // INT4
		public const int END               =     24;
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

	/// <summary>
	/// Summary description for Consts.
	/// </summary>
	internal struct Consts
	{
		// user types
		public const byte SQL_USER                   =      0;
		public const byte SQL_ASYNC_USER             =      1;
		public const byte SQL_UTILITY                =      2;
		public const byte SQL_DISTRIBUTION           =      3;
		public const byte SQLONTROL                =      4;
		public const byte SQL_EVENT                  =      5;
		
		//geo03.h
		public const byte ARGID_PORT_NO              =   0x50;   // = P
		public const byte ARGID_REM_PID              =   0x49;   // = I
		public const byte ARGID_ACKNOWLEDGE          =   0x52;   // = R
		public const byte ARGID_NODE                 =   0x3E;   // = N
		public const byte ARGID_DBROOT               =   0x64;   // = d
		public const byte ARGID_SERVERPGM            =   0x70;   // = p
		public const byte ARGID_AUTH_ALLOW           =   0x61;   // = a
		public const byte ARGID_OMIT_REPLY_PART      =   0x72;   // = r int1 
    
		// some constants
		public const byte ASCIIClient                =      0;
		public const byte UnicodeSwapClient          =     19;
		public const byte NotSwapped                 =      1;
		public const byte RSQL_WIN32               =     13;
		public const byte RSQL_JAVA                = RSQL_WIN32;

		// other connect header constants
		public const byte DBNameSize    =      8;
		public const byte MinSize        =     64;   // for Unix vserver
	}
}
