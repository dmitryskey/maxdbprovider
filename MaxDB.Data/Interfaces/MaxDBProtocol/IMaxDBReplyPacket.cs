//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxReplyPacket.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General internal License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General internal License for more details.
//
// You should have received a copy of the GNU General internal License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.Interfaces.MaxDBProtocol
{
    using MaxDB.Data.Interfaces.Utils;

    internal interface IMaxDBReplyPacket : IMaxDBPacket, IByteArray
    {
        int FindPart(int requestedKind);

        bool ExistsPart(int requestedKind);

        IDataPartVariable VarDataPart { get; }

        bool WasLastPart { get; }

        int KernelMajorVersion { get; }

        int KernelMinorVersion { get; }

        int KernelCorrectionLevel { get; }

        string[] ParseColumnNames();

        // Extracts the short field info, and creates translator instances.
        IDBTechTranslator[] ParseShortFields(bool spaceoption, bool isDBProcedure, IDBProcParameterInfo[] procParameters, bool isVardata);

        string SqlState { get; }

        string ErrorMsg { get; }

        int ResultCount(bool positionedAtPart);

        int SessionID { get; }

        bool IsUnicode { get; }

        int FuncCode { get; }

        int ErrorPos { get; }

        byte[] Features { get; }

        int WeakReturnCode { get; }

        MaxDBException CreateException();

        byte[] ReadDataBytes(int pos, int len);

        string ReadString(int offset, int len);

        byte[][] ParseLongDescriptors();
    }
}