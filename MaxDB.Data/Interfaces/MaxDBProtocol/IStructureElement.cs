//-----------------------------------------------------------------------------------------------
// <copyright file="IStructureElement.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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
    /// <summary>
    /// Database procedure parameter information interface.
    /// </summary>
    internal interface IStructureElement
    {
        /// <summary>
        /// Gets an element type name.
        /// </summary>
        string TypeName { get; }

        /// <summary>
        /// Gets an element code type.
        /// </summary>
        string CodeType { get; }

        /// <summary>
        /// Gets an element length.
        /// </summary>
        int Length { get; }

        /// <summary>
        /// Gets an element precision.
        /// </summary>
        int Precision { get; }

        /// <summary>
        /// Gets an element ASCII offset.
        /// </summary>
        int AsciiOffset { get; }

        /// <summary>
        /// Gets an element Unicode offset.
        /// </summary>
        int UnicodeOffset { get; }

        /// <summary>
        /// Gets an element SQL type name.
        /// </summary>
        string SqlTypeName { get; }
    }
}