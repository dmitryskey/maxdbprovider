using System;
using System.Text;
using System.IO;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.Configuration;
using System.ComponentModel;

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
		static void Main(string[] args)
		{
			//
			// TODO: Add code to start application here
			//

            //ICustomTypeDescriptor bl = (ICustomTypeDescriptor)new System.Data.SqlClient.SqlConnectionStringBuilder();
            ICustomTypeDescriptor b2 = (ICustomTypeDescriptor)new MaxDBConnectionStringBuilder();

            AttributeCollection ff = b2.GetAttributes();
 
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
//                maxdbconn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
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
            MaxDBConnection maxdbconn = null;

            try
            {
				maxdbconn = 
#if NET20
                    new MaxDBConnection(System.Configuration.ConfigurationManager.AppSettings["ConnectionString"]);
#else
                    new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
#endif // NET20
                maxdbconn.SQLMode = SqlMode.Oracle;
				maxdbconn.Open();

                DateTime start_time = DateTime.Now;

                for(int i=0;i<1000;i++)
                {
                    using(MaxDBCommand cmd = new MaxDBCommand("SELECT NAME FROM HOTEL WHERE zip LIKE ? FOR UPDATE", maxdbconn))
                    {
                        cmd.Parameters.Add("?", MaxDBType.VarCharUni).Value = "2%";

                        MaxDBDataReader reader = cmd.ExecuteReader();

                        while(reader.Read())
                            Console.Out.WriteLine(reader.GetString(0));
						reader.Close();
                    }
                }

                Console.WriteLine(DateTime.Now - start_time);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                if (maxdbconn != null)
                    maxdbconn.Close();
            }

            return;
        }

	}
}
