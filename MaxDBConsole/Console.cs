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
#if NET20
			System.Configuration.ConfigurationManager.AppSettings["ConnectionString"];
#else
			System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];
#endif // NET20


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


			return;

		}
	}
}