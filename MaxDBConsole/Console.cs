﻿using System;
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

			//MaxDBConnection maxdbconn = new MaxDBConnection(connStr);
			//maxdbconn.Open();
			//DataTable dt = maxdbconn.GetSchema("SystemInfo");//, new string[] { "DBA", "MESSAGES" });
			//maxdbconn.Close();

            //SslTest();
            PerfomanceTest();
                        
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
