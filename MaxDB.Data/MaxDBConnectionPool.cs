//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnectionPool.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using MaxDB.Data.Interfaces;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utils;

namespace MaxDB.Data
{
    /// <summary>
    /// MaxDB Connection pool.
    /// </summary>
    internal sealed class MaxDBConnectionPool
    {
        private static readonly Dictionary<string, MaxDBConnectionPoolEntry> Pool = new Dictionary<string, MaxDBConnectionPoolEntry>();

        public static IMaxDBComm GetPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
        {
            string key = conn.ConnStrBuilder.ConnectionString;

            lock ((Pool as ICollection).SyncRoot)
            {
                MaxDBConnectionPoolEntry poolEntry;

                if (Pool.ContainsKey(key))
                {
                    poolEntry = Pool[key];
                }
                else
                {
                    poolEntry = new MaxDBConnectionPoolEntry(conn, logger);
                    Pool[key] = poolEntry;
                }

                return poolEntry.GetEntry();
            }
        }

        public static void ReleaseEntry(MaxDBConnection conn)
        {
            lock ((Pool as ICollection).SyncRoot)
            {
                if (conn != null && conn.Comm != null)
                {
                    string key = conn.ConnStrBuilder.ConnectionString;
                    var poolEntry = Pool[key];
                    if (poolEntry == null)
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.POOLNOTFOUND, key));
                    }

                    poolEntry.ReleaseEntry(conn.Comm);
                }
            }
        }

        public static void ClearEntry(MaxDBConnection conn)
        {
            lock ((Pool as ICollection).SyncRoot)
            {
                if (conn != null && conn.Comm != null)
                {
                    Pool.Remove(conn.ConnStrBuilder.ConnectionString);
                    conn.Comm.Dispose();
                }
            }
        }

        public static void ClearAllPools()
        {
            lock ((Pool as ICollection).SyncRoot)
            {
                while (Pool.Keys.Count > 0)
                {
                    foreach (string key in Pool.Keys)
                    {
                        var poolEntry = Pool[key];
                        poolEntry.GetEntry().Dispose();
                        Pool.Remove(key);
                        break;
                    }
                }
            }
        }

        internal class MaxDBConnectionPoolEntry
        {
            private readonly MaxDBConnectionStringBuilder mConnStrBuilder;
            private readonly MaxDBLogger mLogger;
            private List<IMaxDBComm> entryList = new List<IMaxDBComm>();
            private int activeCount;

            public MaxDBConnectionPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
            {
                this.mConnStrBuilder = conn.ConnStrBuilder;

                this.entryList.Capacity = this.mConnStrBuilder.MinPoolSize;

                this.mLogger = logger;

                for (int i = 0; i < this.mConnStrBuilder.MinPoolSize; i++)
                {
                    this.entryList.Add(this.CreateEntry());
                }
            }

            public IMaxDBComm GetEntry()
            {
                var newList = new List<IMaxDBComm>();
                lock (((ICollection)this.entryList).SyncRoot)
                {
                    foreach (var comm in this.entryList)
                    {
                        var conn = new MaxDBConnection();
                        if ((this.mConnStrBuilder.ConnectionLifetime > 0 && comm.OpenTime.AddSeconds(this.mConnStrBuilder.ConnectionLifetime) < DateTime.Now) || !conn.Ping(comm))
                        {
                            comm.Close(true, false);
                            this.activeCount--;
                        }
                        else
                        {
                            newList.Add(comm);
                        }
                    }

                    this.entryList = newList;

                    for (int i = this.entryList.Count; i < this.mConnStrBuilder.MinPoolSize; i++)
                    {
                        this.entryList.Add(this.CreateEntry());
                    }
                }

                lock (((ICollection)this.entryList).SyncRoot)
                {
                    IMaxDBComm comm = null;
                    do
                    {
                        if (this.entryList.Count > 0)
                        {
                            comm = this.entryList[this.entryList.Count - 1];
                            this.entryList.RemoveAt(this.entryList.Count - 1);
                        }

                        if (comm == null && this.activeCount < this.mConnStrBuilder.MaxPoolSize)
                        {
                            comm = this.CreateEntry();
                        }

                        if (comm == null)
                        {
                            Monitor.Wait(this.entryList);
                        }
                    }
                    while (comm == null);

                    return comm;
                }
            }

            private MaxDBComm CreateEntry()
            {
                var comm = new MaxDBComm(this.mLogger)
                {
                    ConnStrBuilder = this.mConnStrBuilder,
                };

                comm.Open(this.ConnectionArguments);
                this.activeCount++;
                return comm;
            }

            public void ReleaseEntry(IMaxDBComm comm)
            {
                lock (this.entryList)
                {
                    this.entryList.Add(comm);
                    Monitor.Pulse(this.entryList);
                }
            }

            private ConnectArgs ConnectionArguments => new ConnectArgs
            {
                host = this.mConnStrBuilder.DataSource,
                dbname = this.mConnStrBuilder.InitialCatalog,
                username = this.mConnStrBuilder.UserId,
                password = this.mConnStrBuilder.Password,
            };
        }
    }
}
