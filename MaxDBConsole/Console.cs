using System;
using System.Data;
using MaxDB.Data;
using System.Collections.Generic;

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
		static void Main(string[] args)
		{
			//
			// TODO: Add code to start application here
			//
			string connStr = System.Configuration.ConfigurationManager.AppSettings["ConnectionString"];
			MaxDBConnection maxdbconn = new MaxDBConnection(connStr);
			maxdbconn.Open();

			using (MaxDBCommand cmd = new MaxDBCommand("CREATE TABLE Test (id int NOT NULL, name VARCHAR(2000))", maxdbconn))
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

                MaxDBDataAdapter ta = new MaxDBDataAdapter();
                ta.SelectCommand = new MaxDBCommand("CALL spTest(:val)", maxdbconn);
                ta.SelectCommand.CommandType = CommandType.StoredProcedure;
                MaxDBParameter p = ta.SelectCommand.Parameters.Add(":val", MaxDBType.Number);
                p.Precision = 10;
                p.Scale = 3;
                p.Value = 123.334;
                //ta.SelectCommand = new MaxDBCommand("SELECT * FROM Test", maxdbconn);

                DataSet ds = new DataSet();
                ta.Fill(ds);

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

			return;

		}
	}
}