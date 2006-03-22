using System;
using System.Text;
using System.IO;
using System.Data;
using System.Data.Odbc;

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

			MaxDBConnection maxdbconn = null;
			MaxDBTransaction trans = null;

			try
			{
				maxdbconn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
				maxdbconn.Open();

				trans = maxdbconn.BeginTransaction(IsolationLevel.ReadCommitted);

				using(MaxDBCommand cmd = new MaxDBCommand("INSERT INTO ruscity (zip,name,state) VALUES (:a,:b, :c)", maxdbconn))
				{
					cmd.Parameters.Add(":a", MaxDBType.VarCharUni).Value = "42600";
					cmd.Parameters.Add(":b", MaxDBType.VarCharUni).Value = "������";
					cmd.Parameters.Add(":c", MaxDBType.VarCharUni).Value = "UD";
					cmd.Transaction = trans;
					cmd.ExecuteNonQuery();
					cmd.Transaction.Commit();
				}

				using(MaxDBCommand cmd = new MaxDBCommand("SELECT NAME FROM RUSCITY WHERE zip = :b", maxdbconn))
				{
					//cmd.Parameters.Add(":a", MaxDBType.VarCharUni).Direction = ParameterDirection.Output;
					cmd.Parameters.Add(":b", MaxDBType.VarCharUni).Value = "20005";

					//cmd.Transaction = trans;

					//cmd.ExecuteNonQuery();

					//string sdf = cmd.Parameters[0].Value.ToString();

					MaxDBDataReader reader = cmd.ExecuteReader();
					while(reader.Read())
						Console.Out.WriteLine(reader.GetString(0));
				}
//											
//					DataTable dt = reader.GetSchemaTable();
//
//					DataSet ds = new DataSet();
//					MaxDBDataAdapter da = new MaxDBDataAdapter();
//					da.SelectCommand = cmd;
//					da.Fill(ds, "List");
//					
//					
//					foreach(DataRow row in ds.Tables[0].Rows)
//						Console.WriteLine(row[0].ToString());
//				}
			}
			catch(Exception ex)
			{
				if (trans != null) trans.Rollback();
				Console.WriteLine(ex.Message);
			}
			finally
			{
				if (maxdbconn != null)
					maxdbconn.Close();
			}

			return;
		}

		static void PerfomanceTest()
		{
			MaxDBConnection maxdbconn = null;

			try
			{
				maxdbconn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
				maxdbconn.Open();

				DateTime start_time = DateTime.Now;

				for(int i=0;i<1000;i++)
				{
					using(MaxDBCommand cmd = new MaxDBCommand("SELECT NAME FROM HOTEL WHERE zip = :b", maxdbconn))
					{
						cmd.Parameters.Add(":b", MaxDBType.VarCharUni).Value = "20005";

						MaxDBDataReader reader = cmd.ExecuteReader();
						while(reader.Read())
							Console.Out.WriteLine(reader.GetString(0));
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
