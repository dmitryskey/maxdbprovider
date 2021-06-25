//-----------------------------------------------------------------------------------------------
// <copyright file="IDBProcParameterInfo.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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
    /// <summary>
    /// DB Stored Procedure parameter info interface.
    /// </summary>
    internal interface IDBProcParameterInfo
    {
        /// <summary>
        /// Gets a number of DB Stored Procedure parameters.
        /// </summary>
        int MemberCount { get; }

        /// <summary>
        /// Gets an element type.
        /// </summary>
        int ElementType { get; }

        /// <summary>
        /// Gets an element SQL type name.
        /// </summary>
        string SQLTypeName { get; }

        /// <summary>
        /// Gets a base type name.
        /// </summary>
        string BaseTypeName { get; }

        /// <summary>
        /// Gets a structure element from indexer.
        /// </summary>
        /// <param name="index">Structure element index.</param>
        /// <returns>Structure element.</returns>
        IStructureElement this[int index] { get; }

        /// <summary>
        /// Add structure element.
        /// </summary>
        /// <param name="typeName">Type name from DBPROCPARAMINFO.</param>
        /// <param name="codeType">Code type from DBPROCPARAMINFO.</param>
        /// <param name="length">The length information from DBPROCPARAMINFO.</param>
        /// <param name="precision">The precision information from DBPROCPARAMINFO.</param>
        /// <param name="asciiOffset">ASCII offset from DBPROCPARAMINFO.</param>
        /// <param name="unicodeOffset">Unicode offset from DBPROCPARAMINFO.</param>
        void AddStructureElement(string typeName, string codeType, int length, int precision, int asciiOffset, int unicodeOffset);
    }
}