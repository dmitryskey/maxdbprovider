using System;
using System.Text;
using System.IO;
using System.Data;
using System.Data.Odbc;
using MaxDBDataProvider.MaxDBProtocol;

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

			Console.Out.WriteLine(Logger.ToHexString(new byte[]{0,1,2,15}));

			BigDecimal d = new BigDecimal("12345,e-10");
			//d.setScale(0);
			Console.Out.WriteLine(((BigInteger)d).ToString());


			return;

			try
			{
				MaxDBConnection maxdbconn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);

				maxdbconn.Open();
				maxdbconn.Close();
				
				string ver = maxdbconn.ServerVersion;
				bool auto = maxdbconn.AutoCommit;

				DateTime start_time = DateTime.Now;
				
				MaxDBTransaction trans = maxdbconn.BeginTransaction(IsolationLevel.ReadUncommitted);

				using(MaxDBCommand cmd = new MaxDBCommand("INSERT INTO TEST (CHARU_FIELD) VALUES('123Hello')", maxdbconn))
				{
					cmd.Transaction = trans;
					cmd.ExecuteNonQuery();
					cmd.Transaction.Commit();
				}

				for(int i=0;i<1000;i++)
				{
					using(MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM TEST --WHERE CHARU_FIELD=:a --AND DATE_FIELD=:b", maxdbconn))
					{
						//						cmd.Parameters.Add(":a", MaxDBType.VarCharUni, "Hello");
						//						cmd.Parameters.Add(":b", MaxDBType.Fixed, 0.0);

						cmd.Transaction = trans;
												
						//MaxDBDataReader reader = cmd.ExecuteReader();
						//						DataTable dt = reader.GetSchemaTable();

						DataSet ds = new DataSet();
						MaxDBDataAdapter da = new MaxDBDataAdapter();
						da.SelectCommand = cmd;
						da.Fill(ds, "List");

						cmd.Transaction.Rollback();

						foreach(DataRow row in ds.Tables[0].Rows)
							Console.WriteLine(row[0].ToString());
					}
				}

				Console.WriteLine(DateTime.Now - start_time);

				maxdbconn.Close();

				//				OdbcConnection odbcconn = new OdbcConnection("Dsn=TESTDB;Uid=DBA;Pwd=123;");
				//				odbcconn.Open();
				//				
				//				start_time = DateTime.Now;
				//				
				//				for(int i=0;i<1000;i++)
				//				{
				//					using(OdbcCommand cmd = new OdbcCommand("SELECT * FROM TEST WHERE CHARA_FIELD=:a", odbcconn))
				//					{
				//						cmd.Parameters.Add(":a", "Test");
				//
				//						DataSet ds = new DataSet();
				//						OdbcDataAdapter da = new OdbcDataAdapter();
				//						da.SelectCommand = cmd;
				//						da.Fill(ds, "List");
				//					}
				//				}
				//				
				//				Console.WriteLine(DateTime.Now - start_time);
				//				
				//				odbcconn.Close();

			}
			catch(Exception ex)
			{
				Console.WriteLine(ex.Message);
			}

			return;
		}
	}
}
