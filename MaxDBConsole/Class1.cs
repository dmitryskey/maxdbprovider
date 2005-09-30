using System;
using System.Text;
using System.IO;

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
			byte[] errorText = new byte[200];

			IntPtr runtime = SQLDBC.ClientRuntime_GetClientRuntime(errorText, 200);

			IntPtr Environment = SQLDBC.SQLDBC_Environment_new_SQLDBC_Environment(runtime);

			IntPtr conn = SQLDBC.SQLDBC_Environment_createConnection(Environment);
			IntPtr conn_prop = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

			int rc = SQLDBC.SQLDBC_Connection_connectASCII(conn, "localhost", "TESTDB", "DBA", "123", conn_prop);

			if(0 != rc) 
			{
				IntPtr herror = SQLDBC.SQLDBC_Connection_getError(conn);
				Console.Out.WriteLine("Connecting to the database failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
				return;
			}

			bool isUnicode = (SQLDBC.SQLDBC_Connection_isUnicodeDatabase(conn) == 1);
			Encoding enc;
			isUnicode = false;
			if (isUnicode)
				enc = Encoding.Unicode;
			else
				enc = Encoding.ASCII;

			int ddd = SQLDBC.SQLDBC_Connection_getTransactionIsolation(conn);

			string prop = SQLDBC.SQLDBC_ConnectProperties_getProperty(conn_prop, Encoding.ASCII.GetBytes("APPVERSION\0"), Encoding.ASCII.GetBytes("0\0"));

			/*
			* Create a new statment object and execute it.
			*/
			
			IntPtr stmt = SQLDBC.SQLDBC_Connection_createStatement(conn);

			if (isUnicode)
				rc = SQLDBC.SQLDBC_Statement_executeNTS(stmt, enc.GetBytes("SELECT 'Hello World (Привет)!' from DUAL"), StringEncodingType.UCS2Swapped);
			else
				rc = SQLDBC.SQLDBC_Statement_executeASCII(stmt, "SELECT 'Hello World!' from DUAL");
			
			rc = SQLDBC.SQLDBC_Statement_executeASCII(stmt, "SELECT timeout FROM DOMAIN.CONNECTPARAMETERS");
			
			if(0 != rc) 
			{
				IntPtr herror = SQLDBC.SQLDBC_Statement_getError(stmt);
				Console.Out.WriteLine("Execution failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
				return;
			}

			/*
			* Check if the SQL command return a resultset and get a result set object.
			*/  
			IntPtr result = SQLDBC.SQLDBC_Statement_getResultSet(stmt);
			if(result == IntPtr.Zero) 
			{
				IntPtr herror = SQLDBC.SQLDBC_Statement_getError(stmt);
				Console.Out.WriteLine("SQL command doesn't return a result set " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
				return;
			}
			/*
			 * Position the curors within the resultset by doing a fetch next call.
			 */

			rc = SQLDBC.SQLDBC_ResultSet_next(result);
			if(0 != rc) 
			{
				IntPtr herror = SQLDBC.SQLDBC_ResultSet_getError(result);
				Console.Out.WriteLine("Error fetching data " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
				return;
			}
			/*
			 * Get a string value from the column.
			 */
			byte[] szString = new byte[sizeof(Int32)];
			Int32 ind = 0;

			fixed(byte *buffer = szString)
			{
				rc = SQLDBC.SQLDBC_ResultSet_getObject(result, 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, new IntPtr(buffer), ref ind, sizeof(Int32), 0);
				if(0 != rc) 
				{
					IntPtr herror = SQLDBC.SQLDBC_ResultSet_getError(result);
					Console.Out.WriteLine("Error getObject " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
					return;
				}

				Console.Out.WriteLine(enc.GetString(szString));
			}

			SQLDBC.SQLDBC_ResultSet_close(result);

			rc = SQLDBC.SQLDBC_Connection_releaseStatement(conn, stmt);

			SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(conn_prop);

			rc = SQLDBC.SQLDBC_Connection_close(conn);

			SQLDBC.SQLDBC_Environment_delete_SQLDBC_Environment(Environment);
			rc=1;
		}
	}
}
