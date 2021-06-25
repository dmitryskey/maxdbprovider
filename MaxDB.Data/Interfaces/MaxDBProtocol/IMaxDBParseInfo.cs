//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBParseInfo.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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
    /// <summary>
    /// Parse information interface.
    /// </summary>
    internal interface IMaxDBParseInfo
    {
        /// <summary>
        /// Gets or sets a value indicating whether the statement is SELECT.
        /// </summary>
        bool IsSelect { get; set; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB Stored Procedure.
        /// </summary>
        bool IsDBProc { get; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB Function.
        /// </summary>
        int FuncCode { get; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB mass update.
        /// </summary>
        bool IsMassCmd { get; }

        /// <summary>
        /// Gets a value indicating whether the stament has LONG parameters.
        /// </summary>
        bool HasLongs { get; }

        /// <summary>
        /// Gets or sets a value indicating whether the statement is cached or not.
        /// </summary>
        bool IsCached { get; set; }

        /// <summary>
        /// Gets or sets updated table name.
        /// </summary>
        string UpdatedTableName { get; set; }

        /// <summary>
        /// Gets a DB Sql Command.
        /// </summary>
        string SqlCommand { get; }

        /// <summary>
        /// Gets or sets a mass parse ID.
        /// </summary>
        byte[] MassParseID { get; set; }

        /// <summary>
        /// Gets a value indicating whether a parse info is valid if the session is the same as of the current connection.
        /// </summary>
        /// <value><c>true</c> if the session ids are equal.</value>
        bool IsValid { get; }

        /// <summary>
        /// Gets the information about parameters in sql statement.
        /// </summary>
        IDBTechTranslator[] ParamInfo { get; }

        /// <summary>
        /// Gets the information about ptable columns.
        /// </summary>
        IDBTechTranslator[] ColumnInfo { get; }

        /// <summary>
        /// Gets the parse id.
        /// </summary>
        byte[] ParseID { get; }

        /// <summary>
        /// Gets a value indicating whether the statement is already executed during parse. (by
        /// checking byte 11 of the parse if for <c>csp1_p_command_executed</c>.
        /// </summary>
        bool IsAlreadyExecuted { get; }

        /// <summary>
        /// Gets or sets DB connection object.
        /// </summary>
        MaxDBConnection DbConnection { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the input data contains VAR parameters.
        /// </summary>
        bool VarDataInput { get; set; }

        /// <summary>
        /// Gets or sets DB stored procedure parameters info.
        /// </summary>
        IDBProcParameterInfo[] ProcParamInfos { get; set; }

        /// <summary>
        /// Gets or sets the parameter DB Tech translators.
        /// </summary>
        IDBTechTranslator[] ParamInfos { get; set; }

        /// <summary>
        /// Gets or sets the number of input parameters.
        /// </summary>
        short InputCount { get; set; }

        /// <summary>
        /// Gets or sets column names.
        /// </summary>
        string[] ColumnNames { get; set; }

        /// <summary>
        /// Gets or sets column DB Tech translators.
        /// </summary>
        IDBTechTranslator[] ColumnInfos { get; set; }

        /// <summary>
        /// Sets a parse id, together with the correct session id.
        /// </summary>
        /// <param name="parseId">The parse id.</param>
        /// <param name="sessionId">The session id of the parse id.</param>
        void SetParseIDAndSession(byte[] parseId, int sessionId);

        /// <summary>
        /// Drop parse IDs.
        /// </summary>
        void DropParseIDs();

        /// <summary>
        /// Sets the infos about parameters and result columns.
        /// </summary>
        /// <param name="shortInfo">Info about the parameters and result columns.</param>
        /// <param name="columnNames">The names of the result columns.</param>
        void SetShortInfosAndColumnNames(IDBTechTranslator[] shortInfo, string[] columnNames);

        /// <summary>
        /// Set meta data.
        /// </summary>
        /// <param name="info">DB Tech translator.</param>
        /// <param name="colName">Column name.</param>
        void SetMetaData(IDBTechTranslator[] info, string[] colName);
    }
}