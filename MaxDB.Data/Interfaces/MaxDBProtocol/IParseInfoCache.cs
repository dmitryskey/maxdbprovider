//-----------------------------------------------------------------------------------------------
// <copyright file="IParseInfoCache.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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

namespace MaxDB.Data.Interfaces.MaxDBProtocol
{
    internal interface IParseInfoCache
    {
        /// <summary>
        /// Find parse info for SQL command.
        /// </summary>
        /// <param name="sqlCmd">SQL command.</param>
        /// <returns>Parse info.</returns>
        IMaxDBParseInfo FindParseInfo(string sqlCmd);

        /// <summary>
        /// Add parse info to the cache.
        /// </summary>
        /// <param name="parseinfo">Parse infor.</param>
        void AddParseInfo(IMaxDBParseInfo parseinfo);

         /// <summary>
        /// Clear the cache.
        /// </summary>
        void Clear();
    }
}