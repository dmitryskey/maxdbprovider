//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnectionPool.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;
    using MaxDB.Data.MaxDBProtocol;
    using MaxDB.Data.Utilities;

    /// <summary>
    /// MaxDB Connection pool.
    /// </summary>
    internal sealed class MaxDBConnectionPool
    {
        private static readonly Hashtable Pool = new Hashtable();

        public static MaxDBComm GetPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
        {
            string key = conn.mConnStrBuilder.ConnectionString;

            lock (Pool.SyncRoot)
            {
                MaxDBConnectionPoolEntry poolEntry;

                if (Pool.ContainsKey(key))
                {
                    poolEntry = (MaxDBConnectionPoolEntry)Pool[key];
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
            lock (Pool.SyncRoot)
            {
                if (conn != null && conn.mComm != null)
                {
                    string key = conn.mConnStrBuilder.ConnectionString;
                    var poolEntry = (MaxDBConnectionPoolEntry)Pool[key];
                    if (poolEntry == null)
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.POOLNOTFOUND, key));
                    }

                    poolEntry.ReleaseEntry(conn.mComm);
                }
            }
        }

        public static void ClearEntry(MaxDBConnection conn)
        {
            lock (Pool.SyncRoot)
            {
                if (conn != null && conn.mComm != null)
                {
                    Pool.Remove(conn.mConnStrBuilder.ConnectionString);
                    conn.mComm.Dispose();
                }
            }
        }

        public static void ClearAllPools()
        {
            lock (Pool.SyncRoot)
            {
                while (Pool.Keys.Count > 0)
                {
                    foreach (string key in Pool.Keys)
                    {
                        var poolEntry = (MaxDBConnectionPoolEntry)Pool[key];
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
            private List<MaxDBComm> entryList = new List<MaxDBComm>();
            private int activeCount;

            public MaxDBConnectionPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
            {
                this.mConnStrBuilder = conn.mConnStrBuilder;

                this.entryList.Capacity = this.mConnStrBuilder.MinPoolSize;

                this.mLogger = logger;

                for (int i = 0; i < this.mConnStrBuilder.MinPoolSize; i++)
                {
                    this.entryList.Add(this.CreateEntry());
                }
            }

            public MaxDBComm GetEntry()
            {
                var newList = new List<MaxDBComm>();
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
                    MaxDBComm comm = null;
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

            public void ReleaseEntry(MaxDBComm comm)
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
