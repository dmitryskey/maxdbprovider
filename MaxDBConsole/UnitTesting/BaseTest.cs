//-----------------------------------------------------------------------------------------------
// <copyright file="BaseTest.cs" company="Dmitry S. Kataev">
//     Copyright � 2005-2018 Dmitry S. Kataev
// </copyright>
//-----------------------------------------------------------------------------------------------
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//	of the License, or (at your option) any later version.
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
using System.IO;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace MaxDB.UnitTesting
{
    /// <summary>
    /// Summary description for BaseTest.
    /// </summary>
    public class BaseTest
    {
        protected MaxDBConnection mconn;
        protected StreamWriter msw;
        protected static IConfiguration config = new ConfigurationBuilder().AddJsonFile("appsettings.json", true, true).Build();

        public BaseTest()
        {
            if (config["LogFileName"] != null)
            {
                msw = new StreamWriter(config["LogFileName"]);

                Trace.Listeners.Clear();
                Trace.Listeners.Add(new TextWriterTraceListener(msw));
            }
        }

        protected void Init(string DDLQuery)
        {
            try
            {
                mconn = new MaxDBConnection(config["ConnectionString"]);
                mconn.Open();
                mconn.AutoCommit = true;

                DropTestTable();

                new MaxDBCommand(DDLQuery, mconn).ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        protected void Close()
        {
            DropTestTable();
            mconn.Dispose();
            if (msw != null)
            {
                msw.Close();
            }
        }

        private void DropTestTable()
        {
            try
            {
                ExecuteNonQuery("EXISTS TABLE Test");
                ExecuteNonQuery("DROP TABLE Test");
            }
            catch (MaxDBException ex)
            {
                if (ex.ErrorCode != -4004)
                {
                    throw;
                }
            }
        }

        protected void ClearTestTable()
        {
            ExecuteNonQuery("DELETE FROM Test");
        }

        protected void ExecuteNonQuery(string cmdSql)
        {
            using (var cmd = new MaxDBCommand(cmdSql, mconn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        protected void DropDbProcedure(string proc)
        {
            try
            {
                ExecuteNonQuery("DROP DBPROC " + proc);
            }
            catch (MaxDBException ex)
            {
                if (ex.ErrorCode != -4016)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        protected void DropDbFunction(string func)
        {
            try
            {
                ExecuteNonQuery("DROP FUNCTION " + func);
            }
            catch (MaxDBException ex)
            {
                if (ex.ErrorCode != -4023)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        protected byte[] CreateBlob(int size)
        {
            byte[] buf = new byte[size];

            var r = new Random();
            r.NextBytes(buf);
            return buf;
        }
    }
}
