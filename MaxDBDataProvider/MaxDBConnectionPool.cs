//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnectionPool.cs" company="Dmitry S. Kataev">
//     Copyright © 2005-2018 Dmitry S. Kataev
//     Copyright © 2002-2003 SAP AG
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
using System.Collections;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Threading;
using System.Collections.Generic;

namespace MaxDB.Data
{
	internal class MaxDBConnectionPoolEntry
	{
		private readonly MaxDBConnectionStringBuilder mConnStrBuilder;
		private readonly MaxDBLogger mLogger;
        private List<MaxDBComm> entryList = new List<MaxDBComm>();
		private int activeCount;

		public MaxDBConnectionPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
		{
			mConnStrBuilder = conn.mConnStrBuilder;

			entryList.Capacity = mConnStrBuilder.MinPoolSize;

			mLogger = logger;

			for (int i = 0; i < mConnStrBuilder.MinPoolSize; i++)
			{
			    entryList.Add(CreateEntry());
			}
		}

        public MaxDBComm GetEntry()
		{
			var newList = new List<MaxDBComm>();
			lock (((ICollection)entryList).SyncRoot)
			{
				foreach (var comm in entryList)
				{
					var conn = new MaxDBConnection();
                    if ((mConnStrBuilder.ConnectionLifetime > 0 && comm.openTime.AddSeconds(mConnStrBuilder.ConnectionLifetime) < DateTime.Now) || !conn.Ping(comm))
                    {
                        comm.Close(true, false);
                        activeCount--;
                    }
                    else
                    {
                        newList.Add(comm);
                    }
				}

				entryList = newList;

                for (int i = entryList.Count; i < mConnStrBuilder.MinPoolSize; i++)
                {
                    entryList.Add(CreateEntry());
                }
			}

            lock (((ICollection)entryList).SyncRoot)
			{
				MaxDBComm comm = null;
				do
				{
					if (entryList.Count > 0)
					{
						comm = entryList[entryList.Count - 1];
						entryList.RemoveAt(entryList.Count - 1);
					}

                    if (comm == null && activeCount < mConnStrBuilder.MaxPoolSize)
                    {
                        comm = CreateEntry();
                    }

                    if (comm == null)
                    {
                        Monitor.Wait(entryList);
                    }
				}
				while (comm == null);

				return comm;
			}
		}

		private MaxDBComm CreateEntry()
		{
            var comm = new MaxDBComm(mLogger);
            comm.mConnStrBuilder = mConnStrBuilder;

			comm.Open(ConnectionArguments);
			activeCount++;
			return comm;
		}

		public void ReleaseEntry(MaxDBComm comm)
		{
			lock (entryList)
			{
				entryList.Add(comm);
				Monitor.Pulse(entryList);
			}
		}

		private ConnectArgs ConnectionArguments
		{
			get
			{
				var connArgs = new ConnectArgs();

				connArgs.host = mConnStrBuilder.DataSource;
				connArgs.dbname = mConnStrBuilder.InitialCatalog;
				connArgs.username = mConnStrBuilder.UserId;
				connArgs.password = mConnStrBuilder.Password;

				return connArgs;
			}
		}
	}

	internal sealed class MaxDBConnectionPool
	{
		private static readonly Hashtable mPool = new Hashtable();

		public static MaxDBComm GetPoolEntry(MaxDBConnection conn, MaxDBLogger logger)
		{
			string key = conn.mConnStrBuilder.ConnectionString;

			lock (mPool.SyncRoot)
			{
				MaxDBConnectionPoolEntry poolEntry;

                if (mPool.ContainsKey(key))
                {
                    poolEntry = (MaxDBConnectionPoolEntry) mPool[key];
                }
                else
                {
                    poolEntry = new MaxDBConnectionPoolEntry(conn, logger);
                    mPool[key] = poolEntry;
                }

				return poolEntry.GetEntry();
			}
		}

		public static void ReleaseEntry(MaxDBConnection conn)
		{
			lock (mPool.SyncRoot)
			{
				if (conn != null && conn.mComm != null)
				{
					string key = conn.mConnStrBuilder.ConnectionString;
					var poolEntry = (MaxDBConnectionPoolEntry)mPool[key];
                    if (poolEntry == null)
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.POOL_NOT_FOUND, key));
                    }

					poolEntry.ReleaseEntry(conn.mComm);
				}
			}
		}

		public static void ClearEntry(MaxDBConnection conn)
		{
			lock (mPool.SyncRoot)
			{
				if (conn != null && conn.mComm != null)
				{
					mPool.Remove(conn.mConnStrBuilder.ConnectionString);
					conn.mComm.Dispose();
				}
			}
		}

		public static void ClearAllPools()
		{
			lock (mPool.SyncRoot)
			{
				while (mPool.Keys.Count > 0)
				{
					foreach (string key in mPool.Keys)
					{
						var poolEntry = (MaxDBConnectionPoolEntry)mPool[key];
						poolEntry.GetEntry().Dispose();
						mPool.Remove(key);
						break;
					}
				}
			}
		}
	}
}
