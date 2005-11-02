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

	internal enum Ports
	{
		Default        =   7210,
		DefaultSecure  =   7270,
		DefaultNI      =   7269
	}

	/// <summary>
	/// Summary description for Consts.
	/// </summary>
	internal enum Consts
	{
		// request/reply types
		RSQL_RTE_PROT_TCP          =      3,
		RSQL_INFO_REQUEST_KEEP_ALIVE  =     50,
		RSQL_INFO_REQUEST          =     51,
		RSQL_INFO_REPLY            =     52,
		RSQL_USERONN_REQUEST     =     61,
		RSQL_USERONN_REPLY       =     62,
		RSQL_USER_DATA_REQUEST     =     63,
		RSQL_USER_DATA_REPLY       =     64,
		RSQL_USERANCEL_REQUEST   =     65,
		RSQL_USER_RELEASE_REQUEST  =     66,
		RSQL_KERNONN_REQUEST     =     71,
		RSQL_KERNONN_REPLY       =     72,
		RSQL_KERN_DATA_REQUEST     =     73,
		RSQL_KERN_DATA_REPLY       =     74,
		RSQL_KERN_RELEASE_REQUEST  =     76,
		RSQL_DUMP_REQUEST          =     81,
		RSQLTRLONN_REQUEST     =     91,
		RSQLTRLONN_REPLY       =     92,
		RSQLTRLANCEL_REQUEST   =     93,
		RSQL_NORMAL                =      0,
		// rte return codes
		SQLOK                      =      0,
		SQLNOTOK                   =      1,
		SQLTASKLIMIT               =      2,
		SQLTIMEOUT                 =      3,
		SQLCRASH                   =      4,
		SQLSTART_REQUIRED          =      5,
		SQLSHUTDOWN                =      6,
		SQLSEND_LINE_DOWN          =      7,
		SQLRECEIVE_LINE_DOWN       =      8,
		SQLPACKETLIMIT             =      9,
		SQLRELEASED                =     10,
		SQLWOULDBLOCK              =     11,
		SQLUNKNOWN_REQUEST         =     12,
		SQLSERVERR_DB_UNKNOWN    =     13,
		// user types
		SQL_USER                   =      0,
		SQL_ASYNC_USER             =      1,
		SQL_UTILITY                =      2,
		SQL_DISTRIBUTION           =      3,
		SQLONTROL                =      4,
		SQL_EVENT                  =      5,
		// other connect header constants
		Connect_Dbname_Size        =      8,
		Connect_MinSize            =     64,   // for Unix vserver
		/*geo03.h*/
		ARGID_PORT_NO              =   0x50,   // = P
		ARGID_REM_PID              =   0x49,   // = I
		ARGID_ACKNOWLEDGE          =   0x52,   // = R
		ARGID_NODE                 =   0x3E,   // = N
		ARGID_DBROOT               =   0x64,   // = d
		ARGID_SERVERPGM            =   0x70,   // = p
		ARGID_AUTH_ALLOW           =   0x61,   // = a
		ARGID_OMIT_REPLY_PART      =   0x72,   // = r int1 
    
		// some constants
		ASCIIClient                =      0,
		UnicodeSwapClient          =     19,
		NotSwapped                 =      1,
		RSQL_WIN32               =     13,
		RSQL_JAVA                = RSQL_WIN32,
		// other connect header constants
		DBNameSize    =      8,
		MinSize        =     64   // for Unix vserver
	}
}
