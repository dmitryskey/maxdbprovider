using System;
using System.Runtime.InteropServices;

namespace MaxDBDataProvider
{
	public enum StringEncodingType 
	{
		Unknown     = 0,
		Ascii       = 1,
		UCS2        = 2,
		UCS2Swapped = 3,
		UTF8        = 4
	};

	enum SQLDBC_SQLType 
	{
		SQLDBC_SQLTYPE_MIN       = 0,            
		SQLDBC_SQLTYPE_FIXED     = SQLDBC_SQLTYPE_MIN, 
		SQLDBC_SQLTYPE_FLOAT     = 1,            
		SQLDBC_SQLTYPE_CHA       = 2,            
		SQLDBC_SQLTYPE_CHE       = 3,            
		SQLDBC_SQLTYPE_CHB       = 4,            
		SQLDBC_SQLTYPE_ROWID     = 5,            
		SQLDBC_SQLTYPE_STRA      = 6,            
		SQLDBC_SQLTYPE_STRE      = 7,            
		SQLDBC_SQLTYPE_STRB      = 8,            
		SQLDBC_SQLTYPE_STRDB     = 9,            
		SQLDBC_SQLTYPE_DATE      = 10,           
		SQLDBC_SQLTYPE_TIME      = 11,           
		SQLDBC_SQLTYPE_VFLOAT    = 12,           
		SQLDBC_SQLTYPE_TIMESTAMP = 13,           
		SQLDBC_SQLTYPE_UNKNOWN   = 14,           
		SQLDBC_SQLTYPE_NUMBER    = 15,           
		SQLDBC_SQLTYPE_NONUMBER  = 16,           
		SQLDBC_SQLTYPE_DURATION  = 17,           
		SQLDBC_SQLTYPE_DBYTEEBCDIC = 18,         
		SQLDBC_SQLTYPE_LONGA     = 19,           
		SQLDBC_SQLTYPE_LONGE     = 20,           
		SQLDBC_SQLTYPE_LONGB     = 21,           
		SQLDBC_SQLTYPE_LONGDB    = 22,           
		SQLDBC_SQLTYPE_BOOLEAN   = 23,           
		SQLDBC_SQLTYPE_UNICODE   = 24,           
		SQLDBC_SQLTYPE_DTFILLER1 = 25,           
		SQLDBC_SQLTYPE_DTFILLER2 = 26,           
		SQLDBC_SQLTYPE_DTFILLER3 = 27,           
		SQLDBC_SQLTYPE_DTFILLER4 = 28,           
		SQLDBC_SQLTYPE_SMALLINT  = 29,           
		SQLDBC_SQLTYPE_INTEGER   = 30,           
		SQLDBC_SQLTYPE_VARCHARA  = 31,           
		SQLDBC_SQLTYPE_VARCHARE  = 32,           
		SQLDBC_SQLTYPE_VARCHARB  = 33,           
		SQLDBC_SQLTYPE_STRUNI    = 34,           
		SQLDBC_SQLTYPE_LONGUNI   = 35,           
		SQLDBC_SQLTYPE_VARCHARUNI = 36,          
		SQLDBC_SQLTYPE_UDT       = 37,           
		SQLDBC_SQLTYPE_ABAPTABHANDLE = 38,       
		SQLDBC_SQLTYPE_DWYDE     = 39,           
		SQLDBC_SQLTYPE_MAX = SQLDBC_SQLTYPE_DWYDE            
	};

	public enum SQLDBC_HostType 
	{
		SQLDBC_HOSTTYPE_MIN         = 0, 
		SQLDBC_HOSTTYPE_PARAMETER_NOTSET = 0, 
		SQLDBC_HOSTTYPE_BINARY      =  1, 
		SQLDBC_HOSTTYPE_ASCII       =  2, 
		SQLDBC_HOSTTYPE_UTF8        =  4, 
		SQLDBC_HOSTTYPE_UINT1       =  5, 
		SQLDBC_HOSTTYPE_INT1        =  6, 
		SQLDBC_HOSTTYPE_UINT2       =  7,  
		SQLDBC_HOSTTYPE_INT2        =  8, 
		SQLDBC_HOSTTYPE_UINT4       =  9, 
		SQLDBC_HOSTTYPE_INT4        = 10, 
		SQLDBC_HOSTTYPE_UINT8       = 11, 
		SQLDBC_HOSTTYPE_INT8        = 12, 
		SQLDBC_HOSTTYPE_DOUBLE      = 13, 
		SQLDBC_HOSTTYPE_FLOAT       = 14, 
		SQLDBC_HOSTTYPE_ODBCDATE    = 15, 
		SQLDBC_HOSTTYPE_ODBCTIME    = 16, 
		SQLDBC_HOSTTYPE_ODBCTIMESTAMP = 17, 
		SQLDBC_HOSTTYPE_ODBCNUMERIC = 18, 
		SQLDBC_HOSTTYPE_GUID        = 19, 
		SQLDBC_HOSTTYPE_UCS2        =  20, 
		SQLDBC_HOSTTYPE_UCS2_SWAPPED=  21, 
		SQLDBC_HOSTTYPE_BLOB         = 22, 
		SQLDBC_HOSTTYPE_ASCII_CLOB   = 23, 
		SQLDBC_HOSTTYPE_UTF8_CLOB    = 24, 
		SQLDBC_HOSTTYPE_UCS2_CLOB    = 25, 
		SQLDBC_HOSTTYPE_UCS2_SWAPPED_CLOB = 26, 
		SQLDBC_HOSTTYPE_STREAM      = 27,  
		SQLDBC_HOSTTYPE_RAWHEX      = 28,  
		SQLDBC_HOSTTYPE_DECIMAL         = 29, /*<! BCD encoded decimal number. */
		SQLDBC_HOSTTYPE_OMS_PACKED_8_3  = 30, /*<! OMS packed decimal (a @c FIXED(15,3) ). */
		SQLDBC_HOSTTYPE_OMS_PACKED_15_3 = 31, /*<! OMS packed decimal (a @c FIXED(29,3) ). */
		SQLDBC_HOSTTYPE_OMS_TIMESTAMP   = 32, /*<! OMS timestamp (a @c FIXED(15,0) ). */
		SQLDBC_HOSTTYPE_OMS_ASCII       = 33, /*<! Special OMS Ascii type that does allow only 7-bit under certain conditions. */ 
		SQLDBC_HOSTTYPE_USERDEFINED = 100, 
		SQLDBC_HOSTTYPE_MAX = SQLDBC_HOSTTYPE_USERDEFINED  
	}


	/// <summary>
	/// Summary description for SQLDBC.
	/// </summary>
	public class SQLDBC
	{
		[DllImport("libsqldbc_c")]
		public extern static IntPtr ClientRuntime_GetClientRuntime(byte[] errorText, int errorTextSize);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Environment_new_SQLDBC_Environment(IntPtr runtime);

		[DllImport("libsqldbc_c")]
		public extern static void SQLDBC_Environment_delete_SQLDBC_Environment(IntPtr env);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Environment_createConnection(IntPtr environment);
 
		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

		[DllImport("libsqldbc_c")]
		public extern static string SQLDBC_ConnectProperties_getProperty(IntPtr conn_prop, byte[] key, byte[] defaultvalue);
  
		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_getTransactionIsolation(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_setTransactionIsolation(IntPtr conn, int level);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_commit(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_rollback(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static void SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(IntPtr prop); 

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_connectASCII(IntPtr conn, string host, string dbname, string username, string password, IntPtr conn_prop);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_close(IntPtr conn);
	
		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Connection_getError(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_PreparedStatement_getError(IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_ResultSet_getError(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static string SQLDBC_ErrorHndl_getErrorText(IntPtr herror);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Connection_createStatement(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Connection_createPreparedStatement(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_releaseStatement(IntPtr conn, IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Connection_releasePreparedStatement(IntPtr conn, IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_PreparedStatement_prepareNTS(IntPtr stmt, byte[] query , StringEncodingType type);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_PreparedStatement_prepareASCII(IntPtr stmt, string query);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_PreparedStatement_getParameterMetaData(IntPtr stmt);
 
		[DllImport("libsqldbc_c")]
		public extern static short SQLDBC_ParameterMetaData_getParameterCount(IntPtr hdl);
 
		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ParameterMetaData_getParameterName(IntPtr hdl, short param, char[] buffer, StringEncodingType type,
								int size, out int length);  

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_PreparedStatement_executeASCII(IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_Statement_executeASCII(IntPtr stmt, string query);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_PreparedStatement_clearParameters(IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_PreparedStatement_bindParameter(IntPtr stmt, ushort index, SQLDBC_HostType type, IntPtr paramAddr,  
						ref int length, int size, int terminate);  

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_Statement_getResultSet(IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static IntPtr SQLDBC_PreparedStatement_getResultSet(IntPtr stmt);

		[DllImport("libsqldbc_c")]
		public extern static void SQLDBC_ResultSet_close(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ResultSet_next(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ResultSet_prev(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ResultSet_first(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ResultSet_last(IntPtr result);

		[DllImport("libsqldbc_c")]
		public extern static byte SQLDBC_Connection_isUnicodeDatabase(IntPtr conn);

		[DllImport("libsqldbc_c")]
		public extern static int SQLDBC_ResultSet_getObject(IntPtr result, int index, SQLDBC_HostType type, IntPtr paramAddr, ref int length, int size, int terminate); 
	}
}
