//	Copyright © 2005-2018 Dmitry S. Kataev
//	Copyright © 2002-2003 SAP AG
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

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace MaxDB.Data.MaxDBProtocol
{
	internal class GarbageParseId
	{
		protected int iCanTrashOld = 20;
		protected bool bObjPending;
		protected bool bCurrentEmptyRun;
		protected bool bCurrentEmptyRun2;
		private List<byte[]> lstGarbage;
		private bool bSupportsMultipleDropParseIDs;

		public GarbageParseId(bool supportMultipleDropParseIds)
			: base()
		{
			bSupportsMultipleDropParseIDs = supportMultipleDropParseIds;
            lstGarbage = new List<byte[]>(iCanTrashOld);
		}

		public bool IsPending
		{
			get
			{
				return GarbageSize >= iCanTrashOld;
			}
		}

		public void EmptyCan(MaxDBComm communication, ConnectArgs connArgs)
		{
            if (communication == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "communication"));
            }

            if (bCurrentEmptyRun)
            {
                return;
            }

			bCurrentEmptyRun = true;

			MaxDBRequestPacket requestPacket;
			bObjPending = false;
			while (GarbageSize > 0)
			{
				try
				{
					requestPacket = communication.GetRequestPacket();
					requestPacket.Init(short.MaxValue);
					EmptyCan(requestPacket);
					communication.Execute(connArgs, requestPacket, this, GCMode.GC_NONE);
				}
				catch (MaxDBException)
				{
					// ignore 
				}
			}

			bCurrentEmptyRun = false;
		}

		public void ThrowIntoGarbageCan(byte[] obj)
		{
			lstGarbage.Add(obj);
		}

		protected int GarbageSize
		{
			get
			{
				return lstGarbage.Count;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public bool EmptyCan(MaxDBRequestPacket requestPacket)
		{
            if (requestPacket == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "requestPacket"));
            }

            if (bCurrentEmptyRun2)
            {
                return false;
            }

			bCurrentEmptyRun2 = true;

			bool packetActionFailed = false;
			int sz = GarbageSize;

            if (!bSupportsMultipleDropParseIDs)
            {
                while (sz > 0 && !packetActionFailed)
                {
                    byte[] obj = lstGarbage[sz - 1];
                    lstGarbage.RemoveAt(sz - 1);
                    packetActionFailed = !requestPacket.DropParseId(obj, false);
                    if (packetActionFailed)
                    {
                        lstGarbage.Add(obj);
                    }

                    sz--;
                }
            }
            else
            {
                if (sz > 0)
                {
                    byte[] obj = lstGarbage[sz - 1];
                    lstGarbage.RemoveAt(sz - 1);
                    packetActionFailed = !requestPacket.DropParseId(obj, false);
                    if (packetActionFailed)
                    {
                        lstGarbage.Add(obj);
                    }
                    else
                    {
                        sz--;
                        while (sz > 0 && !packetActionFailed)
                        {
                            obj = lstGarbage[sz - 1];
                            lstGarbage.RemoveAt(sz - 1);
                            packetActionFailed = !requestPacket.DropParseIdAddToParseIdPart(obj);
                            if (packetActionFailed)
                            {
                                lstGarbage.Add(obj);
                            }

                            sz--;
                        }
                    }
                }
            }

			bCurrentEmptyRun2 = false;
			return !packetActionFailed;
		}

		public void EmptyCan()
		{
			lstGarbage.Clear();
		}
	}
}
