using System;
using System.Text;
using System.IO;
using System.Data;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class Class1
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static unsafe void Main(string[] args)
		{
			//
			// TODO: Add code to start application here
			//

			MaxDBConnection maxdbconn = new MaxDBConnection("DATA SOURCE=R55S;INITIAL CATALOG=TESTDB;USER ID=DBA;PASSWORD=123");
			maxdbconn.Open();

			using(MaxDBCommand cmd = new MaxDBCommand("SELECT DATE_FIELD FROM TEST", maxdbconn))
			{
				IDataReader reader = cmd.ExecuteReader();
				string name = reader.GetName(0);

				DataSet ds = new DataSet();
				MaxDBDataAdapter da = new MaxDBDataAdapter();
				da.SelectCommand = cmd;
				da.Fill(ds, "List");
				foreach(DataRow row in ds.Tables[0].Rows)
					Console.WriteLine(row[0].ToString());
			}
			
			maxdbconn.Close();

			byte[] errorText = new byte[200];

			IntPtr runtime;
				
			fixed (byte* buff = errorText)
			{
				runtime = SQLDBC.ClientRuntime_GetClientRuntime(errorText, 200);
			}
			IntPtr Environment = SQLDBC.SQLDBC_Environment_new_SQLDBC_Environment(runtime);

			IntPtr conn = SQLDBC.SQLDBC_Environment_createConnection(Environment);
			IntPtr conn_prop = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
			SQLDBC.SQLDBC_Connection_setSQLMode(conn, SQLDBC_SQLMode.SQLDBC_INTERNAL);

			if(SQLDBC.SQLDBC_Connection_connectASCII(conn, "localhost", "TESTDB", "DBA", "123", conn_prop) != SQLDBC_Retcode.SQLDBC_OK) 
			{
				Console.Out.WriteLine("Connecting to the database failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(conn)));
				return;
			}

			bool isUnicode = (SQLDBC.SQLDBC_Connection_isUnicodeDatabase(conn) == 1);
			Encoding enc;
			isUnicode = false;
			if (isUnicode)
				enc = Encoding.Unicode;
			else
				enc = Encoding.ASCII;

			//int ddd = SQLDBC.SQLDBC_Connection_getTransactionIsolation(conn);

			SQLDBC.SQLDBC_ConnectProperties_setProperty(conn_prop, "DATE_TIME-FORMAT", "ÂÀÏ");
			string prop = SQLDBC.SQLDBC_ConnectProperties_getProperty(conn_prop, "DATE_TIME-FORMAT", "ÂÀÏ123");

			/*
			* Create a new statment object and execute it.
			*/
			
			IntPtr stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(conn);

			SQLDBC_Retcode rc;

			if (isUnicode)
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(stmt, enc.GetBytes("SELECT 'Hello World (Ïðèâåò)!' from DUAL"), StringEncodingType.UCS2Swapped);
			else
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(stmt, "SELECT LOB_FIELD FROM TEST WHERE DATE_FIELD = :field");
			
			if(rc != SQLDBC_Retcode.SQLDBC_OK) 
			{
				Console.Out.WriteLine("Execution failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
				return;
			}

			rc = SQLDBC.SQLDBC_PreparedStatement_clearParameters(stmt);

			ODBCDATE date;
			date.year = 1999;
			date.month = 2;
			date.day = 18;
 
			int b_len = sizeof(ODBCDATE);
			
			//fixed (byte *b_ref = b)
			//{
				rc = SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, new IntPtr(&date), ref b_len, b_len, 0);
				rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);
			//}			
			if(rc != SQLDBC_Retcode.SQLDBC_OK) 
			{
				Console.Out.WriteLine("Execution failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
				return;
			}

			/*
			* Check if the SQL command return a resultset and get a result set object.
			*/  
			IntPtr result = SQLDBC.SQLDBC_PreparedStatement_getResultSet(stmt);
			if(result == IntPtr.Zero) 
			{
				Console.Out.WriteLine("SQL command doesn't return a result set " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
				return;
			}
			/*
			 * Position the curors within the resultset by doing a fetch next call.
			 */

			if(SQLDBC.SQLDBC_ResultSet_next(result) != SQLDBC_Retcode.SQLDBC_OK) 
			{
				Console.Out.WriteLine("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_ResultSet_getError(result)));
				return;
			}
			/*
			 * Get a string value from the column.
			 */
			byte[] szString = new byte[30];
			Int32 ind = 0;

			byte[] columnName = new byte[1];
			int len = 0;

			fixed(byte *namePtr = columnName)
			{
				rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(result), 1, columnName, 
					StringEncodingType.UCS2Swapped, len, ref len);
			}

			if (rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
			{
				Console.Out.WriteLine("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_ResultSet_getError(result)));
				return;
			}

			len += sizeof(char);
			columnName = new byte[len];

			fixed(byte *namePtr = columnName)
			{
				rc = SQLDBC.SQLDBC_ResultSetMetaData_getColumnName(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(result), 1, columnName, 
					StringEncodingType.UCS2Swapped, len, ref len);
			}
			
			if (rc != SQLDBC_Retcode.SQLDBC_OK)
			{
				Console.Out.WriteLine("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_ResultSet_getError(result)));
				return;
			}

			string colName = Encoding.Unicode.GetString(columnName).TrimEnd('\0');

			SQLDBC_SQLType type = SQLDBC.SQLDBC_ResultSetMetaData_getColumnType(SQLDBC.SQLDBC_ResultSet_getResultSetMetaData(result), 1);

			byte val;
			int val_length = sizeof(byte);
			rc = SQLDBC.SQLDBC_ResultSet_getObject(result, 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT1, new IntPtr(&val), 
						ref val_length, val_length, 0);

			fixed(byte *buffer = szString)
			{
				if(SQLDBC.SQLDBC_ResultSet_getObject(result, 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, new IntPtr(buffer), ref ind, 30, 0) != SQLDBC_Retcode.SQLDBC_OK) 
				{
					Console.Out.WriteLine("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_ResultSet_getError(result)));
					return;
				}

				Console.Out.WriteLine(enc.GetString(szString));
			}

			SQLDBC.SQLDBC_ResultSet_close(result);

			SQLDBC.SQLDBC_Connection_releasePreparedStatement(conn, stmt);

			SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(conn_prop);

			rc = SQLDBC.SQLDBC_Connection_close(conn);

			SQLDBC.SQLDBC_Environment_delete_SQLDBC_Environment(Environment);
		}
	}
}
