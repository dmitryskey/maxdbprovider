//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBRequestPacket.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

    internal interface IMaxDBRequestPacket : IMaxDBPacket, IByteArray
    {
        int PacketLength { get; }

        short PartArguments { get; set; }

        void AddNullData(int len);

        void AddUndefinedResultCount();

        /// <summary>
        /// Switch Sql Mode.
        /// </summary>
        /// <param name="newMode">Sql Mode.</param>
        /// <returns>Old Sql mode.</returns>
        byte SwitchSqlMode(byte newMode);

        void AddData(byte[] data);

        void AddDataString(string data);

        void AddString(string data);

        void AddResultCount(int count);

        bool DropParseId(byte[] parseId, bool reset);

        bool DropParseIdAddToParseIdPart(byte[] parseId);

        void Init(short maxSegment);

        void Close();

        /// <summary>
        /// Init internal DB command object.
        /// </summary>
        /// <param name="command">DB Command.</param>
        /// <param name="autoCommit">AutoCommit flag.</param>
        void InitDbsCommand(string command, bool autoCommit);

        bool InitDbsCommand(string command, bool reset, bool autoCommit, bool unicode = false);

        int InitParseCommand(string cmd, bool reset, bool parseAgain);

        void InitDbs(bool autoCommit);

        void InitDbs(bool reset, bool autoCommit);

        void InitParse(bool reset, bool parseAgain);

        bool InitChallengeResponse(string user, byte[] challenge);

        void InitExecute(byte[] parseId, bool autoCommit);

        void SetWithInfo();

        void SetMassCommand();

        IMaxDBDataPart InitGetValue(bool autoCommit);

        IMaxDBDataPart InitPutValue(bool autoCommit);

        IMaxDBDataPart NewDataPart(bool varData);

        IMaxDBDataPart NewDataPart(byte partKind);

        void NewPart(byte kind);

        void AddPartAttr(byte attr);

        void AddClientProofPart(byte[] clientProof);

        void AddClientIdPart(string clientId);

        void AddFeatureRequestPart(byte[] features);

        void AddParseIdPart(byte[] parseId);

        void AddCursorPart(string cursorName);

        void ClosePart(int extent, short args);
    }
}