using System;
using System.Text;
using System.IO;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.Configuration;
using System.ComponentModel;
using System.Threading;
using MaxDB.Data;

namespace MaxDB.Test
{
	/// <summary>
	/// Summary description for class.
	/// </summary>
	class ConsoleDefault
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
            string connStr = 
#if NET20 && !MONO
                    System.Configuration.ConfigurationManager.AppSettings["ConnectionString"];
#else
                    System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];
#endif // NET20 && !MONO

			MaxDBConnection maxdbconn = new MaxDBConnection(connStr);
			maxdbconn.Open();

			using (MaxDBCommand cmd = new MaxDBCommand("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))", maxdbconn))
			{
				cmd.ExecuteNonQuery();

				cmd.CommandText = "INSERT INTO Test (id, name) VALUES(1, 'name 1')";
				cmd.ExecuteNonQuery();

				cmd.CommandText = "SELECT * FROM Test";

				using (MaxDBDataReader reader = cmd.ExecuteReader())
				{
					reader.Read();
					Console.WriteLine(reader.GetString(1));
				}

				cmd.CommandText = "DROP TABLE Test";
				cmd.ExecuteNonQuery();

				cmd.CommandText = "CREATE TABLE Test (id int NOT NULL, name1 VARCHAR(100))";
				cmd.ExecuteNonQuery();

				cmd.CommandText = "INSERT INTO Test (id, name1) VALUES(1, 'name 1')";
				cmd.ExecuteNonQuery();

				cmd.CommandText = "SELECT * FROM Test";

				using (MaxDBDataReader reader = cmd.ExecuteReader())
				{
					reader.Read();
					Console.WriteLine(reader.GetString(1));
				}

				cmd.CommandText = "DROP TABLE Test";
				cmd.ExecuteNonQuery();
			}

			maxdbconn.Close();

            //SslTest();
            //PerfomanceTest();
                        
			return;

//			StreamWriter sw = new StreamWriter(ConfigurationSettings.AppSettings["LogFileName"]);
//
//			Trace.Listeners.Add(new TextWriterTraceListener(sw));
//
//            MaxDBConnection maxdbconn = null;
//            MaxDBTransaction trans = null;
//
//            try
//            {
//                maxdbconn = new MaxDBConnection(connStr);
//                maxdbconn.Open();
//
//                trans = maxdbconn.BeginTransaction(IsolationLevel.ReadCommitted);
//
//                using (MaxDBCommand cmd = new MaxDBCommand("DELETE FROM ruscity WHERE zip = :a", maxdbconn))
//                {
//                    cmd.Parameters.Add(":a", MaxDBType.VarCharUni).Value = "42600";
//                    cmd.Transaction = trans;
//                    cmd.ExecuteNonQuery();
//                }
//
//				int len = 50000;
//				char[] chars = new char[len];
//				for (int i = 0; i < len; i++)
//					chars[i] = (char)(i % 128);
//
//                using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO ruscity (zip,name,state,info) VALUES (:a, :b, :c, :d)", maxdbconn))
//                {
//                    cmd.Parameters.Add(":a", MaxDBType.VarCharUni).Value = "42600";
//                    cmd.Parameters.Add(":b", MaxDBType.VarCharUni).Value = "Ижевск";
//                    cmd.Parameters.Add(":c", MaxDBType.VarCharUni).Value = DBNull.Value;
//					cmd.Parameters.Add(":d", MaxDBType.LongUni).Value = chars;
//                    cmd.Transaction = trans;
//                    cmd.ExecuteNonQuery();
//                    cmd.Transaction.Commit();
//                }
//
////				using (MaxDBCommand cmd = new MaxDBCommand("EXISTS TABLE hotel", maxdbconn))
////				{
////				    cmd.ExecuteNonQuery();
////				}
//
//
//				using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM hotel where zip=:b FOR UPDATE", maxdbconn))
//				{
//					//cmd.Parameters.Add(":a", MaxDBType.LongUni).Direction = ParameterDirection.Output;
//					//cmd.Parameters[0].Size = 32000;
//					cmd.Parameters.Add(":b", MaxDBType.VarCharUni).Value = "20005";
//
//					//cmd.Transaction = trans;
//
//					//cmd.ExecuteNonQuery();
//
//					MaxDBDataReader reader = cmd.ExecuteReader();
//					while (reader.Read())
//					    System.Windows.Forms.MessageBox.Show(reader.GetString(0), "Name",
//					        System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Exclamation);
//
//					DataTable dt = reader.GetSchemaTable();
//
//					//reader.Close();
//					//											
//					//					DataTable dt = reader.GetSchemaTable();
//					//
////					DataSet ds = new DataSet();
////					MaxDBDataAdapter da = new MaxDBDataAdapter();
////					da.SelectCommand = cmd;
////					da.Fill(ds, "List");
////					DataTable dt = ds.Tables[0];
//					//					
//					//					
//					//					foreach(DataRow row in ds.Tables[0].Rows)
//					//						Console.WriteLine(row[0].ToString());
//					//				}
//				}
//            }
//            catch (Exception ex)
//            {
//                if (trans != null) trans.Rollback();
//                Console.WriteLine(ex.Message);
//            }
//            finally
//            {
//				if (maxdbconn != null)
//					maxdbconn.Dispose();
//				sw.Close();
//            }
//
//            return;
		}

        static void PerfomanceTest()
        {
            try
            {
                MaxDB.UnitTesting.StressTests test = new MaxDB.UnitTesting.StressTests();

                test.SetUp();

                DateTime start = DateTime.Now;

                test.TestSequence();

                Console.WriteLine(DateTime.Now.Subtract(start));

                test.TearDown();
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(ex.Message);
                while (ex.InnerException != null)
                {
                    ex = ex.InnerException;
                    Console.Out.WriteLine(ex.Message);
                }
            }
        }

        static void SslTest()
        {
            try
            {
#if NET20 || MONO
                MaxDB.UnitTesting.ConnectionTests test = new MaxDB.UnitTesting.ConnectionTests();

                test.SetUp();

                test.TestConnectionSsl();

                Console.WriteLine("Ssl connection test passed");
#endif
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(ex.Message);
                while (ex.InnerException != null)
                {
                    ex = ex.InnerException;
                    Console.Out.WriteLine(ex.Message);
                }
            }
        }
	}
}

/*
using System;
using System.Data;
using MaxDB.Data;
using System.Collections.Specialized;
using System.IO;
using System.Diagnostics;

public class Test
{
	public static void Main(string[] args)
	{
		NameValueCollection mAppSettings = System.Configuration.ConfigurationSettings.AppSettings;

		StreamWriter msw = null;

		if (mAppSettings["LogFileName"] != null)
		{
			msw = new StreamWriter(mAppSettings["LogFileName"]);

			Trace.Listeners.Clear();
			Trace.Listeners.Add(new TextWriterTraceListener(msw));
		}

		string connectionString =
		   "Server=sheep;" +
		   "Database=uhoteldb;" +
		   "User ID=SCOTT;" +
		   "Password=TIGER;";
		IDbConnection dbcon = new MaxDBConnection(connectionString);
		dbcon.Open();
		IDbCommand dbcmd = dbcon.CreateCommand();
		// requires a table to be created named employee
		// with columns firstname and lastname
		// such as,
		//        CREATE TABLE employee_tmp (
		//           firstname varchar(32),
		//           lastname varchar(32));
		string sql =
			"SELECT firstname, lastname " +
			"FROM employee_tmp";
		dbcmd.CommandText = sql;
		IDataReader reader = dbcmd.ExecuteReader();
		while (reader.Read())
		{
			string FirstName = (string)reader["firstname"];
			string LastName = (string)reader["lastname"];
			Console.WriteLine("Name: " +
				 FirstName + " " + LastName);
		}
		// clean up
		reader.Dispose();
		reader = null;
		dbcmd.Dispose();
		dbcmd = null;
		dbcon.Dispose();
		dbcon = null;

		if (msw != null) msw.Close();
	}
}
*/