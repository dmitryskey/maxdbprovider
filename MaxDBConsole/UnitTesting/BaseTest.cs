using System;
using MaxDBDataProvider;
using NUnit.Framework;
using System.Configuration;
using System.IO;
using System.Diagnostics;

namespace MaxDBConsole.UnitTesting
{
	/// <summary>
	/// Summary description for BaseTest.
	/// </summary>
	public class BaseTest
	{
		protected MaxDBConnection m_conn;
		protected StreamWriter m_sw = null;

		public BaseTest()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		protected void Init(string DDLQuery)
		{
			if (ConfigurationSettings.AppSettings["LogFileName"] != null)
			{
				m_sw = new StreamWriter(ConfigurationSettings.AppSettings["LogFileName"]);

				Trace.Listeners.Clear();
				Trace.Listeners.Add(new TextWriterTraceListener(m_sw));
			}

			m_conn = new MaxDBConnection(ConfigurationSettings.AppSettings["ConnectionString"]);
			m_conn.Open();
			m_conn.AutoCommit = true;
			try
			{
				(new MaxDBCommand("EXISTS TABLE Test", m_conn)).ExecuteNonQuery();
				(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
			}
			catch(MaxDBException ex)
			{
				if (ex.DetailErrorCode != -708)
					throw;
			}

			(new MaxDBCommand(DDLQuery, m_conn)).ExecuteNonQuery();
		}

		protected void Close() 
		{
			(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
			m_conn.Dispose();
			if (m_sw != null) m_sw.Close();
		}

		protected void ClearTestTable()
		{
			(new MaxDBCommand("DELETE FROM Test", m_conn)).ExecuteNonQuery();
		}
	}
}
