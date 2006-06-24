//	Copyright (C) 2005-2006 Dmitry S. Kataev
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
//
//	This program is distributed in the hope that it will be useful,
//	but WITHOUT ANY WARRANTY; without even the implied warranty of
//	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//	GNU General Public License for more details.
//
//	You should have received a copy of the GNU General Public License
//	along with this program; if not, write to the Free Software
//	Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System;
using MaxDB.Data;
using NUnit.Framework;
using System.Configuration;
using System.IO;
using System.Diagnostics;
using System.Collections.Specialized;

namespace MaxDB.Data.Test.UnitTesting
{
	/// <summary>
	/// Summary description for BaseTest.
	/// </summary>
	public class BaseTest
	{
		protected MaxDBConnection m_conn;
		protected StreamWriter m_sw = null;
        protected NameValueCollection m_AppSettings =
#if NET20
            System.Configuration.ConfigurationManager.AppSettings;
#else
            System.Configuration.ConfigurationSettings.AppSettings;
#endif // NET20

        public BaseTest()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		protected void Init(string DDLQuery)
		{
			if (m_AppSettings["LogFileName"] != null)
			{
				m_sw = new StreamWriter(m_AppSettings["LogFileName"]);

				Trace.Listeners.Clear();
				Trace.Listeners.Add(new TextWriterTraceListener(m_sw));
			}

			m_conn = new MaxDBConnection(m_AppSettings["ConnectionString"]);
			m_conn.Open();
			m_conn.AutoCommit = true;
			try
			{
				(new MaxDBCommand("EXISTS TABLE Test", m_conn)).ExecuteNonQuery();
				(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
			}
			catch(MaxDBException ex)
			{
				if (ex.VendorCode != -4004)
					throw;
			}

			(new MaxDBCommand(DDLQuery, m_conn)).ExecuteNonQuery();
		}

		protected void Close() 
		{
			(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
			((IDisposable)m_conn).Dispose();
			if (m_sw != null) m_sw.Close();
		}

		protected void ClearTestTable()
		{
			(new MaxDBCommand("DELETE FROM Test", m_conn)).ExecuteNonQuery();
		}
	}
}
