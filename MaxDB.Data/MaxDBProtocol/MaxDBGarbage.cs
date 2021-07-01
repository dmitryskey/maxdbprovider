//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBGarbage.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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

namespace MaxDB.Data.MaxDBProtocol
{
    using MaxDB.Data.Interfaces;
    using MaxDB.Data.Interfaces.MaxDBProtocol;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// MaxDB GC class.
    /// </summary>
    internal class MaxDBGarbage
    {
        private readonly int canTrashOld = 20;
        private readonly List<byte[]> lstGarbage;
        private readonly bool supportsMultipleDropParseIDs;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBGarbage"/> class.
        /// </summary>
        /// <param name="supportMultipleDropParseIds">Flag indicating if one can drop multiple parse ids.</param>
        public MaxDBGarbage(bool supportMultipleDropParseIds)
        {
            this.supportsMultipleDropParseIDs = supportMultipleDropParseIds;
            this.lstGarbage = new List<byte[]>(this.canTrashOld);
        }

        /// <summary>
        /// Gets a value indicating whether GC can is already full.
        /// </summary>
        public bool IsPending => this.GarbageSize >= this.canTrashOld;

        private int GarbageSize => this.lstGarbage.Count;

        /// <summary>
        /// Empty GC Can.
        /// </summary>
        /// <param name="comm">Communication object.</param>
        /// <param name="connArgs">Connection arguments.</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void EmptyCan(IMaxDBComm comm, ConnectArgs connArgs)
        {
            if (comm == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(comm)));
            }

            IMaxDBRequestPacket requestPacket;
            while (this.GarbageSize > 0)
            {
                try
                {
                    requestPacket = comm.GetRequestPacket();
                    requestPacket.Init(short.MaxValue);
                    this.EmptyCan(requestPacket);
                    comm.Execute(connArgs, requestPacket, this, GCMode.NONE);
                }
                catch (MaxDBException)
                {
                    // ignore
                }
            }
        }

        /// <summary>
        /// Throw an object into GC can.
        /// </summary>
        /// <param name="obj">Object to throw into.</param>
        public void ThrowIntoGarbageCan(byte[] obj) => this.lstGarbage.Add(obj);

        /// <summary>
        /// Empty GC can.
        /// </summary>
        /// <param name="requestPacket">Request packet.</param>
        /// <returns><c>true</c> if packet action succeeded and <c>false</c> otherwise.</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool EmptyCan(IMaxDBRequestPacket requestPacket)
        {
            if (requestPacket == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETERNULL, nameof(requestPacket)));
            }

            bool packetActionFailed = false;
            int sz = this.GarbageSize;

            if (!this.supportsMultipleDropParseIDs)
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

            return !packetActionFailed;
        }

        /// <summary>
        /// Empty GC can.
        /// </summary>
        public void EmptyCan() => this.lstGarbage.Clear();
    }
}
