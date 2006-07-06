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

namespace MaxDB.UnitTesting
{
	/// <summary>
	/// Summary description for BaseTest.
	/// </summary>
	public class BaseTest
	{
		protected MaxDBConnection mconn;
		protected StreamWriter msw = null;
        protected NameValueCollection mAppSettings =
#if NET20 && !MONO
            System.Configuration.ConfigurationManager.AppSettings;
#else
            System.Configuration.ConfigurationSettings.AppSettings;
#endif // NET20 && !MONO

        public BaseTest()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		protected void Init(string DDLQuery)
		{
			if (mAppSettings["LogFileName"] != null)
			{
				msw = new StreamWriter(mAppSettings["LogFileName"]);

				Trace.Listeners.Clear();
				Trace.Listeners.Add(new TextWriterTraceListener(msw));
			}

			mconn = new MaxDBConnection(mAppSettings["ConnectionString"]);
			mconn.Open();
			mconn.AutoCommit = true;
			try
			{
				ExecuteNonQuery("EXISTS TABLE Test");
				ExecuteNonQuery("DROP TABLE Test");
			}
			catch(MaxDBException ex)
			{
				if (ex.ErrorCode != -4004)
					throw;
			}

			(new MaxDBCommand(DDLQuery, mconn)).ExecuteNonQuery();
		}

		protected void Close() 
		{
			ExecuteNonQuery("DROP TABLE Test");
			((IDisposable)mconn).Dispose();
			if (msw != null) msw.Close();
		}

		protected void ClearTestTable()
		{
			ExecuteNonQuery("DELETE FROM Test");
		}

		protected void ExecuteNonQuery(string cmdSql)
		{
			using(MaxDBCommand cmd = new MaxDBCommand(cmdSql, mconn))
				cmd.ExecuteNonQuery();
		}
	}
}
