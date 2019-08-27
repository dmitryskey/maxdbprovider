// Copyright © 2005-2018 Dmitry S. Kataev
// Copyright © 2002-2003 SAP AG
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

namespace MaxDB.Data.MaxDBProtocol
{
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;

    internal class GarbageParseId
    {
        protected int iCanTrashOld = 20;
        protected bool bObjPending;
        protected bool bCurrentEmptyRun;
        protected bool bCurrentEmptyRun2;
        private readonly List<byte[]> lstGarbage;
        private readonly bool bSupportsMultipleDropParseIDs;

        public GarbageParseId(bool supportMultipleDropParseIds)
            : base()
        {
            this.bSupportsMultipleDropParseIDs = supportMultipleDropParseIds;
            this.lstGarbage = new List<byte[]>(this.iCanTrashOld);
        }

        public bool IsPending => this.GarbageSize >= this.iCanTrashOld;

        public void EmptyCan(MaxDBComm communication, ConnectArgs connArgs)
        {
            if (communication == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "communication"));
            }

            if (this.bCurrentEmptyRun)
            {
                return;
            }

            this.bCurrentEmptyRun = true;

            MaxDBRequestPacket requestPacket;
            this.bObjPending = false;
            while (this.GarbageSize > 0)
            {
                try
                {
                    requestPacket = communication.GetRequestPacket();
                    requestPacket.Init(short.MaxValue);
                    this.EmptyCan(requestPacket);
                    communication.Execute(connArgs, requestPacket, this, GCMode.NONE);
                }
                catch (MaxDBException)
                {
                    // ignore
                }
            }

            this.bCurrentEmptyRun = false;
        }

        public void ThrowIntoGarbageCan(byte[] obj) => this.lstGarbage.Add(obj);

        protected int GarbageSize => this.lstGarbage.Count;

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool EmptyCan(MaxDBRequestPacket requestPacket)
        {
            if (requestPacket == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "requestPacket"));
            }

            if (this.bCurrentEmptyRun2)
            {
                return false;
            }

            this.bCurrentEmptyRun2 = true;

            bool packetActionFailed = false;
            int sz = this.GarbageSize;

            if (!this.bSupportsMultipleDropParseIDs)
            {
                while (sz > 0 && !packetActionFailed)
                {
                    byte[] obj = this.lstGarbage[sz - 1];
                    this.lstGarbage.RemoveAt(sz - 1);
                    packetActionFailed = !requestPacket.DropParseId(obj, false);
                    if (packetActionFailed)
                    {
                        this.lstGarbage.Add(obj);
                    }

                    sz--;
                }
            }
            else
            {
                if (sz > 0)
                {
                    byte[] obj = this.lstGarbage[sz - 1];
                    this.lstGarbage.RemoveAt(sz - 1);
                    packetActionFailed = !requestPacket.DropParseId(obj, false);
                    if (packetActionFailed)
                    {
                        this.lstGarbage.Add(obj);
                    }
                    else
                    {
                        sz--;
                        while (sz > 0 && !packetActionFailed)
                        {
                            obj = this.lstGarbage[sz - 1];
                            this.lstGarbage.RemoveAt(sz - 1);
                            packetActionFailed = !requestPacket.DropParseIdAddToParseIdPart(obj);
                            if (packetActionFailed)
                            {
                                this.lstGarbage.Add(obj);
                            }

                            sz--;
                        }
                    }
                }
            }

            this.bCurrentEmptyRun2 = false;
            return !packetActionFailed;
        }

        public void EmptyCan() => this.lstGarbage.Clear();
    }
}
