//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBFactory.cs" company="Dmitry S. Kataev">
//     Copyright © 2005-2018 Dmitry S. Kataev
//     Copyright © 2002-2003 SAP AG
// </copyright>
//-----------------------------------------------------------------------------------------------
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

using System.Data.Common;

namespace MaxDB.Data
{
    /// <summary>
    /// MaxDB factory class.
    /// </summary>
	public sealed class MaxDBFactory : DbProviderFactory
	{
		/// <summary>
		/// The current instance.
		/// </summary>
		public static readonly MaxDBFactory Instance;

        static MaxDBFactory() => Instance = new MaxDBFactory();

        /// <summary>
        /// Returns MaxDB create command object.
        /// </summary>
        /// <returns>MaxDBCommand object instance.</returns>
		public override DbCommand CreateCommand() => new MaxDBCommand();

        /// <summary>
        /// Returns MaxDB command builder object.
        /// </summary>
        /// <returns>MaxDBCommandBuilder object instance.</returns>
		public override DbCommandBuilder CreateCommandBuilder() => new MaxDBCommandBuilder();

        /// <summary>
        /// Returns MaxDB connection object.
        /// </summary>
        /// <returns>MaxDBConnection object instance.</returns>
		public override DbConnection CreateConnection() => new MaxDBConnection();

        /// <summary>
        /// Returns MaxDB connection string builder object.
        /// </summary>
        /// <returns>MaxDBConnectionStringBuilder object instance.</returns>
		public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MaxDBConnectionStringBuilder();

        /// <summary>
        /// Returns MaxDB data adapter object.
        /// </summary>
        /// <returns>MaxDBDataAdapter object instance.</returns>
		public override DbDataAdapter CreateDataAdapter() => new MaxDBDataAdapter();

        /// <summary>
        /// Returns MaxDB parameter object.
        /// </summary>
        /// <returns>MaxDBParameter object instance.</returns>
		public override DbParameter CreateParameter() => new MaxDBParameter();
    }
}
