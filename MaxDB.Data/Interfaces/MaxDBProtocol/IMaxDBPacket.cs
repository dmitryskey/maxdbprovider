//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBPacket.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General License for more details.
//
// You should have received a copy of the GNU General License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.Interfaces.MaxDBProtocol
{
    using System;

    internal interface IMaxDBPacket
    {
        void FillHeader(byte msgClass, int senderRef);

        void SetSendLength(int value);

        int MaxSendLength { get; }

        int ActSendLength { get; }

        int PacketSender { get; }

        int ReturnCode { get; }

        int FirstSegment();

        int NextSegment();

        string DumpSegment(DateTime dt);

        #region "Part operations"

        int PartLength { get; }

        int PartSize { get; }

        int PartPos { get; }

        int PartDataPos { get; }

        int PartArgsCount { get; }

        int PartType { get; }

        int PartSegmentOffset { get; }

        void ClearPartOffset();

        int PartCount { get; }

        int NextPart();

        string DumpPart(DateTime dt);

        #endregion

        string DumpPacket();
    }
}