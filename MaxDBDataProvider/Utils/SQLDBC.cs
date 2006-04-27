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

namespace MaxDBDataProvider.Utils
{
#if !SAFE

	#region "Classes, structures and enumerations to support interropting"

	#region "Enumerations"

	public enum SQLDBC_Retcode 
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

	public enum SQLDBC_StringEncodingType 
	{
		Unknown     = 0,
		Ascii       = 1,
		UCS2        = 2,
		UCS2Swapped = 3,
		UTF8        = 4
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

	public enum ColumnNullBehavior 
	{
		columnNoNulls = 0,
		columnNullable = 1,
		columnNullableUnknown = 2
	}

	public enum SQLDBC_BOOL : byte
	{
		SQLDBC_FALSE = 0,
		SQLDBC_TRUE = 1
	}

	public enum SQLDBC_ParameterMode 
	{
		Unknown = 0,
		In = 1,
		InOut = 2,
		Out = 4
	}

	#endregion

	#region "Structures"

	[StructLayout(LayoutKind.Sequential)]
	public struct ODBCDATE
	{
		public short year;
		public ushort month;
		public ushort day;
	}

	[StructLayout(LayoutKind.Sequential)]
	public struct ODBCTIME
	{
		public ushort hour;
		public ushort minute;
		public ushort second;
	}

	[StructLayout(LayoutKind.Sequential)]
	public struct ODBCTIMESTAMP
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

	public class ODBCConverter
	{
		public static unsafe byte[] GetBytes(ODBCDATE dt)
		{
			byte[] result = new byte[sizeof(ODBCDATE)];

			Array.Copy(BitConverter.GetBytes(dt.year), 0, result, 0, sizeof(short));
			Array.Copy(BitConverter.GetBytes(dt.month), 0, result, sizeof(short), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(dt.day), 0, result, sizeof(short) + sizeof(ushort), sizeof(ushort));

			return result;
		}

		public static unsafe byte[] GetBytes(ODBCTIME tm)
		{
			byte[] result = new byte[sizeof(ODBCTIME)];

			Array.Copy(BitConverter.GetBytes(tm.hour), 0, result, 0, sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(tm.minute), 0, result, sizeof(ushort), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(tm.second), 0, result, 2 * sizeof(ushort), sizeof(ushort));

			return result;
		}

		public static unsafe byte[] GetBytes(ODBCTIMESTAMP ts)
		{
			byte[] result = new byte[sizeof(ODBCTIMESTAMP)];

			Array.Copy(BitConverter.GetBytes(ts.year), 0, result, 0, sizeof(short));
			Array.Copy(BitConverter.GetBytes(ts.month), 0, result, sizeof(short), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(ts.day), 0, result, sizeof(short) + sizeof(ushort), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(ts.hour), 0, result, sizeof(short) + 2*sizeof(ushort), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(ts.minute), 0, result, sizeof(short) + 3*sizeof(ushort), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(ts.second), 0, result, sizeof(short) + 4*sizeof(ushort), sizeof(ushort));
			Array.Copy(BitConverter.GetBytes(ts.fraction), 0, result, sizeof(short) + 5*sizeof(ushort), sizeof(uint));

			return result;
		}

		public static unsafe ODBCDATE GetDate(byte[] data)
		{
			ODBCDATE dt_val;
			
			dt_val.year = BitConverter.ToInt16(data, 0);
			dt_val.month = BitConverter.ToUInt16(data, sizeof(short));
			dt_val.day = BitConverter.ToUInt16(data, sizeof(short) + sizeof(ushort));

			return dt_val;
		}

		public static unsafe ODBCTIME GetTime(byte[] data)
		{
			ODBCTIME tm_val;

			tm_val.hour = BitConverter.ToUInt16(data, 0);
			tm_val.minute = BitConverter.ToUInt16(data, sizeof(ushort));
			tm_val.second = BitConverter.ToUInt16(data, 2 * sizeof(ushort));

			return tm_val;
		}

		public static unsafe ODBCTIMESTAMP GetTimeStamp(byte[] data)
		{
			ODBCTIMESTAMP ts_val;

			ts_val.year = BitConverter.ToInt16(data, 0);
			ts_val.month = BitConverter.ToUInt16(data, sizeof(short));
			ts_val.day = BitConverter.ToUInt16(data, sizeof(short) + sizeof(ushort));
			ts_val.hour = BitConverter.ToUInt16(data, sizeof(short) + 2 * sizeof(ushort));
			ts_val.minute = BitConverter.ToUInt16(data, sizeof(short) + 3 * sizeof(ushort));
			ts_val.second = BitConverter.ToUInt16(data, sizeof(short) + 4 * sizeof(ushort));
			ts_val.fraction = BitConverter.ToUInt32(data, sizeof(short) + 5 * sizeof(ushort));

			return ts_val;
		}

		public static DateTime GetDateTime(ODBCDATE dt_val)
		{
			return new DateTime(dt_val.year, dt_val.month, dt_val.day);
		}

		public static DateTime GetDateTime(ODBCTIME tm_val)
		{
			return new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, 
				tm_val.hour, tm_val.minute, tm_val.second);
		}

		public static DateTime GetDateTime(ODBCTIMESTAMP ts_val)
		{
			return new DateTime(ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second).AddTicks(
				(ts_val.fraction / 1000) * (TimeSpan.TicksPerMillisecond / 1000));
		}

		public static TimeSpan GetTimeSpan(ODBCTIME tm_val)
		{
			return new TimeSpan(tm_val.hour, tm_val.minute, tm_val.second);
		}
	}

	#endregion

	/// <summary>
	/// Summary description for SQLDBC.
	/// </summary>
	public class SQLDBC
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
		public extern static IntPtr ClientRuntime_GetClientRuntime(IntPtr errorText, int errorTextSize);

		[DllImport("libSQLDBC_C")]
		public extern static string SQLDBC_ErrorHndl_getErrorText(IntPtr herror);
		
		#endregion

		#region "Environment"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Environment_new_SQLDBC_Environment(IntPtr runtime);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Environment_delete_SQLDBC_Environment(IntPtr env);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Environment_createConnection(IntPtr environment);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Environment_releaseConnection (IntPtr hdl, IntPtr conn); 

		#endregion
 
		#region "Connect Properties"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

		[DllImport("libSQLDBC_C")]
		public extern static string SQLDBC_ConnectProperties_getProperty(IntPtr conn_prop, string key, string defaultvalue);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_ConnectProperties_setProperty(IntPtr conn_prop, string key, string defaultvalue);

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
		public extern static SQLDBC_Retcode SQLDBC_Connection_connectASCII(IntPtr conn, string host, string dbname, string username, string password, IntPtr conn_prop);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Connection_close(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Connection_createStatement(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Connection_createPreparedStatement(IntPtr conn);

		[DllImport("libSQLDBC_C")]
		public extern static void SQLDBC_Connection_releaseStatement(IntPtr conn, IntPtr stmt);

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
		public extern static SQLDBC_Retcode SQLDBC_Statement_executeASCII(IntPtr stmt, string query);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_BOOL SQLDBC_Statement_isQuery(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Statement_getResultSet(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_Statement_getTableName(IntPtr stmt, IntPtr buffer, SQLDBC_StringEncodingType encoding, 
			int bufferSize, ref int bufferLength); 

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_Statement_getError(IntPtr stmt);

		#endregion

		#region "Prepared Statement"

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getError(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_prepareNTS(IntPtr stmt, byte[] query, SQLDBC_StringEncodingType encoding);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_prepareASCII(IntPtr stmt, string query);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_bindParameter(IntPtr stmt, short index, SQLDBC_HostType type, IntPtr paramAddr,  
						ref int length, int size, SQLDBC_BOOL terminate);		
		
		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getParameterMetaData(IntPtr stmt);
 
		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_executeASCII(IntPtr stmt);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_PreparedStatement_clearParameters(IntPtr stmt);
		
		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_PreparedStatement_getResultSet(IntPtr stmt);
		
		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_PreparedStatement_getRowsAffected(IntPtr stmt); 
		
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
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_prev(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_first(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_last(IntPtr result);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_relative(IntPtr result, short offset);

		[DllImport("libSQLDBC_C")]
		public extern static IntPtr SQLDBC_ResultSet_getResultSetMetaData(IntPtr hdl); 

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_getObject(IntPtr result, int index, SQLDBC_HostType type, IntPtr paramAddr, 
			ref int length, int size, SQLDBC_BOOL terminate);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSet_getObject(IntPtr result, int index, SQLDBC_HostType type, IntPtr paramAddr, 
			ref int length, int size, int startPos, SQLDBC_BOOL terminate);

		#endregion

		#region "Parameter Metadata"

		[DllImport("libSQLDBC_C")]
		public extern static short SQLDBC_ParameterMetaData_getParameterCount(IntPtr hdl);
 
		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ParameterMetaData_getParameterName(IntPtr hdl, short param, byte[] buffer, 
			SQLDBC_StringEncodingType type, int size, ref int length);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ParameterMetaData_getParameterLength(IntPtr hdl, short param);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ParameterMetaData_getPhysicalLength(IntPtr hdl, short param);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_ParameterMode SQLDBC_ParameterMetaData_getParameterMode(IntPtr hdl, short param);

		#endregion

		#region "Result Set Meta Data"

		[DllImport("libSQLDBC_C")]
		public extern static short SQLDBC_ResultSetMetaData_getColumnCount(IntPtr hdl);

		[DllImport("libSQLDBC_C")]
		public extern static SQLDBC_Retcode SQLDBC_ResultSetMetaData_getColumnName(IntPtr hdl, short column, IntPtr buffer, 
			SQLDBC_StringEncodingType encoding, int size, ref int length); 

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getColumnType(IntPtr hdl, short column);

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getColumnLength(IntPtr hdl, short column); 

		[DllImport("libSQLDBC_C")]
		public extern static int SQLDBC_ResultSetMetaData_getColumnPrecision(IntPtr hdl, short column); 

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
