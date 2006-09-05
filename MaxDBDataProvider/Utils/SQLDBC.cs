//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using System.Runtime.InteropServices;

namespace MaxDB.Data.Utilities
{
#if !SAFE

	#region "Classes, structures and enumerations to support interropting"

	#region "Enumerations"

	internal enum SQLDBC_Retcode 
	{
		SQLDBC_INVALID_OBJECT           =-10909, // Application tries to use an invalid object reference. 
		SQLDBC_OK                       = 0,	 // Function call successful. 
		SQLDBC_NOT_OK                   = 1,     // Function call not successful. Further information can be found in the corresponding error object.
		SQLDBC_DATA_TRUNC               = 2,     // Data was truncated during the call. 
		SQLDBC_OVERFLOW                 = 3,     // Signalizes a numeric overflow. 
		SQLDBC_SUCCESS_WITH_INFO        = 4,     // The method succeeded with warnings. 
		SQLDBC_NO_DATA_FOUND            = 100,   // Data was not found. 
		SQLDBC_NEED_DATA                = 99     // Late binding, data is needed for execution. 
	}

	internal enum SQLDBC_StringEncodingType 
	{
		Unknown     = 0,
		Ascii       = 1,
		UCS2        = 2,
		UCS2Swapped = 3,
		UTF8        = 4
	};

	internal enum SQLDBC_HostType 
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

	internal enum ColumnNullBehavior 
	{
		columnNoNulls = 0,
		columnNullable = 1,
		columnNullableUnknown = 2
	}

	internal enum SQLDBC_BOOL
	{
		SQLDBC_FALSE = 0,
		SQLDBC_TRUE = 1
	}

	internal enum SQLDBC_ParameterMode 
	{
		Unknown = 0,
		In = 1,
		InOut = 2,
		Out = 4
	}

	#endregion

	#region "Structures"

	[StructLayout(LayoutKind.Sequential)]
	internal struct OdbcDate
	{
		public short year;
		public ushort month;
		public ushort day;
	}

	[StructLayout(LayoutKind.Sequential)]
	internal struct OdbcTime
	{
		public ushort hour;
		public ushort minute;
		public ushort second;
	}

	[StructLayout(LayoutKind.Sequential)]
	internal struct OdbcTimeStamp
	{
		public short year;
		public ushort month;
		public ushort day;
		public ushort hour;
		public ushort minute;
		public ushort second;
		public uint fraction;
	}

	#endregion

	#region "ODBC Date/Time format converter"

	internal sealed class ODBCConverter
	{
        private ODBCConverter()
        {
        }

		public static DateTime GetDateTime(OdbcDate dt_val)
		{
			return new DateTime(dt_val.year, dt_val.month, dt_val.day);
		}

		public static DateTime GetDateTime(OdbcTime tmval)
		{
			return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, 
				tmval.hour, tmval.minute, tmval.second);
		}

		public static DateTime GetDateTime(OdbcTimeStamp ts_val)
		{
			return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second).AddTicks(
				(ts_val.fraction / 1000) * (TimeSpan.TicksPerMillisecond / 1000));
		}
	}

	#endregion

	/// <summary>
	/// Summary description for SQLDBC.
	/// </summary>
	internal struct UnsafeNativeMethods
	{
		#region "Constants"

			public const int SQLDBC_NULL_DATA = -1;
			public const int SQLDBC_DATA_AT_EXEC = -2;
			public const int SQLDBC_NTS = -3;
			public const int SQLDBC_NO_TOTAL = -4;
			public const int SQLDBC_DEFAULT_PARAM = -5;
			public const int SQLDBC_IGNORE = -6;
			public const int SQLDBC_LEN_DATA_AT_EXEC_OFFSET = -100;

		#endregion

		#region "Runtime"

		[DllImport("libSQLDBC_C")]
		public unsafe extern static IntPtr ClientRuntime_GetClientRuntime(byte* errorText, int errorTextSize);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ErrorHndl_getErrorCode(IntPtr herror);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ErrorHndl_getSQLState(IntPtr herror);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ErrorHndl_getErrorText(IntPtr herror);
		
		#endregion

		#region "Environment"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Environment_new_SQLDBC_Environment(IntPtr runtime);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Environment_setTraceOptions(IntPtr env, IntPtr traceoptions);  

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Environment_delete_SQLDBC_Environment(IntPtr env);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Environment_createConnection(IntPtr env);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Environment_releaseConnection (IntPtr env, IntPtr conn); 

		#endregion
 
		#region "Connect Properties"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_ConnectProperties_setProperty(IntPtr conn_prop,
			[MarshalAs(UnmanagedType.LPStr)]string key, [MarshalAs(UnmanagedType.LPStr)]string value);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ConnectProperties_getProperty(IntPtr conn_prop,
			[MarshalAs(UnmanagedType.LPStr)]string key, [MarshalAs(UnmanagedType.LPStr)]string defaultvalue);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(IntPtr prop); 
  
		#endregion

		#region "Connection"

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_Connection_getTransactionIsolation(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_setTransactionIsolation(IntPtr conn, int level);

		[DllImport("libSQLDBC_C")]
		public extern static byte SQLDBC_Connection_isUnicodeDatabase(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_Connection_getKernelVersion (IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_BOOL SQLDBC_Connection_isConnected(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Connection_setSQLMode(IntPtr conn, int sqlmode);
 
		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Connection_setAutoCommit(IntPtr conn, SQLDBC_BOOL autocommit); 

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_BOOL SQLDBC_Connection_getAutoCommit(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_commit(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_rollback(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_connectASCII(IntPtr conn,
			[MarshalAs(UnmanagedType.LPStr)]string host, [MarshalAs(UnmanagedType.LPStr)]string dbname,
			[MarshalAs(UnmanagedType.LPStr)]string username, [MarshalAs(UnmanagedType.LPStr)]string password, IntPtr conn_prop);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_close(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Connection_createPreparedStatement(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Connection_releasePreparedStatement(IntPtr conn, IntPtr stmt);
		
		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_cancel(IntPtr conn);   
	
		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Connection_getError(IntPtr conn);

		#endregion

		#region "Statement"

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Statement_setMaxRows(IntPtr stmt, uint rows);  

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_BOOL SQLDBC_Statement_isQuery(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public unsafe extern static SQLDBC_Retcode SQLDBC_Statement_getTableName(IntPtr stmt, IntPtr buffer, SQLDBC_StringEncodingType encoding, 
			int bufferSize, int* bufferLength); 

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Statement_getError(IntPtr stmt);

		#endregion

		#region "Prepared Statement"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getError(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public unsafe extern static SQLDBC_Retcode SQLDBC_PreparedStatement_prepareNTS(IntPtr stmt,
			[MarshalAs(UnmanagedType.LPWStr)]string query, SQLDBC_StringEncodingType encoding);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_prepareASCII(IntPtr stmt, [MarshalAs(UnmanagedType.LPStr)]string query);

		[DllImport("libSQLDBC_C")]
		public unsafe extern static SQLDBC_Retcode SQLDBC_PreparedStatement_bindParameterAddr(IntPtr stmt, short index, SQLDBC_HostType type, void* addr,  
						int* length, int size, SQLDBC_BOOL terminate);		
		
		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getParameterMetaData(IntPtr stmt);
 
		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_executeASCII(IntPtr stmt);
		
		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getResultSet(IntPtr stmt);
		
		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_PreparedStatement_getRowsAffected(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_setBatchSize(IntPtr stmt, uint size);
		
		#endregion

		#region "Result Set"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ResultSet_getError(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_ResultSet_close(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSet_getResultCount(IntPtr result); 

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_next(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ResultSet_getResultSetMetaData(IntPtr hdl); 

		[DllImport("libSQLDBC_C")]
		public unsafe extern static SQLDBC_Retcode SQLDBC_ResultSet_getObject(IntPtr result, int index, SQLDBC_HostType type, void* paramAddr, 
			int* length, int size, SQLDBC_BOOL terminate);

		#endregion

		#region "Parameter Metadata"

		[DllImport("libSQLDBC_C")]
		public extern static short SQLDBC_ParameterMetaData_getParameterCount(IntPtr hdl);
 
		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ParameterMetaData_getParameterLength(IntPtr hdl, short param);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_ParameterMode SQLDBC_ParameterMetaData_getParameterMode(IntPtr hdl, short param);

		#endregion

		#region "Result Set Meta Data"

		[DllImport("libSQLDBC_C")]
		public extern static short SQLDBC_ResultSetMetaData_getColumnCount(IntPtr hdl);

		[DllImport("libSQLDBC_C")]
		public unsafe extern static SQLDBC_Retcode SQLDBC_ResultSetMetaData_getColumnName(IntPtr hdl, short column, void* buffer, 
			SQLDBC_StringEncodingType encoding, int size, int* length); 

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getColumnType(IntPtr hdl, short column);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getPrecision(IntPtr hdl, short column);
 
		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getScale(IntPtr hdl, short column); 

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getPhysicalLength(IntPtr hdl, short column);

		[DllImport("libSQLDBC_C")]
		public extern static ColumnNullBehavior SQLDBC_ResultSetMetaData_isNullable(IntPtr hdl, short column);
 
		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_isWritable(IntPtr hdl, short column); 

		#endregion
	}

	#endregion

#endif
}
