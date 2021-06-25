//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBDataPart.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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

using System;
using System.IO;
using MaxDB.Data.Interfaces.Utils;
using MaxDB.Data.Interfaces.MaxDBProtocol;
using MaxDB.Data.Utils;

namespace MaxDB.Data.Interfaces.MaxDBProtocol
{
    /// <summary>
    /// DataPart interface.
    /// </summary>
    internal interface IMaxDBDataPart
    {
        int Extent { get; }

        /// <summary>
        /// Gets data part length.
        /// </summary>
        int Length { get; }

        /// <summary>
        /// Gets or sets data byte array.
        /// </summary>
        IByteArray Data { get; set; }

        /// <summary>
        /// Gets or sets request packet.
        /// </summary>
        IMaxDBRequestPacket ReqPacket { get; set; }

        /// <summary>
        /// Gets or sets original data byte array.
        /// </summary>
        IByteArray OrigData { get; set; }

        void AddArg(int pos, int len);

        /// <summary>
        /// Close request packet.
        /// </summary>
        void Close();

        /// <summary>
        /// Close array part.
        /// </summary>
        /// <param name="rows">Number of rows.</param>
        void CloseArrayPart(short rows);

        bool HasRoomFor(int recordSize, int reserve);

        bool HasRoomFor(int recordSize);

        void SetFirstPart();

        void SetLastPart();

        void MoveRecordBase();

        void WriteDefineByte(byte value, int offset);

        void WriteByte(byte value, int offset);

        void WriteBytes(byte[] value, int offset, int len);

        void WriteBytes(byte[] value, int offset);

        void WriteAsciiBytes(byte[] value, int offset, int len);

        void WriteUnicodeBytes(byte[] value, int offset, int len);

        void WriteInt16(short value, int offset);

        void WriteInt32(int value, int offset);

        void WriteInt64(long value, int offset);

        void WriteUnicode(string value, int offset, int len);

        byte[] ReadBytes(int offset, int len);

        string ReadAscii(int offset, int len);

        void MarkEmptyStream(IByteArray descriptionMark);

        bool FillWithProcedureReader(TextReader reader, short rowCount);

        void AddRow(short fieldCount);

        void WriteNull(int pos, int len);

        IByteArray WriteDescriptor(int pos, byte[] descriptor);

        bool FillWithOmsStream(Stream stream, bool asciiForUnicode);

        bool FillWithProcedureStream(Stream stream, short rowCount);

        bool FillWithStream(Stream stream, IByteArray descriptionMark, IPutValue putValue);

        bool FillWithReader(TextReader reader, IByteArray descriptionMark, IPutValue putValue);
    }
}
