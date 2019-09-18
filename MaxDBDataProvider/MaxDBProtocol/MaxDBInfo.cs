//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBInfo.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Runtime.CompilerServices;
    using System.Text;
    using MaxDB.Data.Utilities;

    /// <summary>
    /// SQL parameter controller interface.
    /// </summary>
    internal interface ISqlParameterController
    {
        /// <summary>
        /// Gets a connection object.
        /// </summary>
        MaxDBConnection Connection { get; }

        /// <summary>
        /// Gets a reply data byte array.
        /// </summary>
        ByteArray ReplyData { get; }
    }

    /// <summary>
    /// Parse information class.
    /// </summary>
    internal class MaxDBParseInfo
    {
        // 11th Byte of Parseid coded application code
        private const int ApplCodeByte = 10;

        private byte[] byMassParseId;
        private int iSessionId; // unique identifier for the connection

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBParseInfo"/> class.
        /// </summary>
        /// <param name="connection">MaxDB connection.</param>
        /// <param name="sqlCmd">SQL command.</param>
        /// <param name="functionCode">Function code.</param>
        public MaxDBParseInfo(MaxDBConnection connection, string sqlCmd, int functionCode)
        {
            this.DbConnection = connection;
            this.SqlCommand = sqlCmd;
            this.FuncCode = functionCode;
            this.iSessionId = -1;
            if (this.FuncCode == FunctionCode.Select || this.FuncCode == FunctionCode.Show || this.FuncCode == FunctionCode.DBProcWithResultSetExecute || this.FuncCode == FunctionCode.Explain)
            {
                this.IsSelect = true;
            }

            if (this.FuncCode == FunctionCode.DBProcWithResultSetExecute || this.FuncCode == FunctionCode.DBProcExecute)
            {
                this.IsDBProc = true;
            }

            if (this.FuncCode == FunctionCode.DBProcExecute)
            {
                this.DescribeProcedureCall();
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="MaxDBParseInfo"/> class.
        /// </summary>
        ~MaxDBParseInfo()
        {
            this.IsCached = false;
            this.DropParseIDs();
        }

        /// <summary>
        /// Gets or sets a value indicating whether the statement is SELECT.
        /// </summary>
        public bool IsSelect { get; set; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB Stored Procedure.
        /// </summary>
        public bool IsDBProc { get; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB Function.
        /// </summary>
        public int FuncCode { get; }

        /// <summary>
        /// Gets a value indicating whether the statement is DB mass update.
        /// </summary>
        public bool IsMassCmd { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the stament has LONG parameters.
        /// </summary>
        public bool HasLongs { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether the statement is cached or not.
        /// </summary>
        public bool IsCached { get; set; }

        /// <summary>
        /// Gets or sets updated table name.
        /// </summary>
        public string UpdatedTableName { get; set; }

        /// <summary>
        /// Gets a DB Sql Command.
        /// </summary>
        public string SqlCommand { get; }

        /// <summary>
        /// Gets or sets a mass parse ID.
        /// </summary>
        public byte[] MassParseID
        {
            get => this.byMassParseId;
            set
            {
                this.byMassParseId = value;
                if (value == null)
                {
                    return;
                }

                for (int i = 0; i < FunctionCode.massCmdAppCodes.Length; i++)
                {
                    if (value[ApplCodeByte] == FunctionCode.massCmdAppCodes[i])
                    {
                        this.IsMassCmd = true;
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether a parse info is valid if the session is the same as of the current connection.
        /// </summary>
        /// <value><c>true</c> if the session ids are equal.</value>
        public bool IsValid => this.iSessionId == this.DbConnection.mComm.SessionID;

        /// <summary>
        /// Gets the information about parameters in sql statement.
        /// </summary>
        public MaxDBTranslators.DBTechTranslator[] ParamInfo => this.ParamInfos;

        /// <summary>
        /// Gets the information about ptable columns.
        /// </summary>
        public MaxDBTranslators.DBTechTranslator[] ColumnInfo => this.ColumnInfos;

        /// <summary>
        /// Gets the parse id.
        /// </summary>
        public byte[] ParseID { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the statement is already executed during parse. (by
        /// checking byte 11 of the parse if for <c>csp1_p_command_executed</c>.
        /// </summary>
        public bool IsAlreadyExecuted => this.ParseID != null && this.ParseID[MaxDBParseInfo.ApplCodeByte] == FunctionCode.commandExecuted;

        /// <summary>
        /// Gets or sets DB connection object.
        /// </summary>
        public MaxDBConnection DbConnection { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the input data contains VAR parameters.
        /// </summary>
        internal bool VarDataInput { get; set; }

        /// <summary>
        /// Gets or sets DB stored procedure parameters info.
        /// </summary>
        internal DBProcParameterInfo[] ProcParamInfos { get; set; }

        /// <summary>
        /// Gets or sets the parameter DB Tech translators.
        /// </summary>
        internal MaxDBTranslators.DBTechTranslator[] ParamInfos { get; set; }

        /// <summary>
        /// Gets or sets the number of input parameters.
        /// </summary>
        internal short InputCount { get; set; }

        /// <summary>
        /// Gets or sets column names.
        /// </summary>
        internal string[] ColumnNames { get; set; }

        /// <summary>
        /// Gets or sets column DB Tech translators.
        /// </summary>
        internal MaxDBTranslators.DBTechTranslator[] ColumnInfos { get; set; }

        /// <summary>
        /// Sets a parse id, together with the correct session id.
        /// </summary>
        /// <param name="parseId">The parse id.</param>
        /// <param name="sessionId">The session id of the parse id.</param>
        public void SetParseIDAndSession(byte[] parseId, int sessionId)
        {
            this.iSessionId = sessionId;
            this.ParseID = parseId;
        }

        /// <summary>
        /// Drop parse IDs.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DropParseIDs()
        {
            if (this.DbConnection != null && this.DbConnection.mComm != null)
            {
                if (this.ParseID != null)
                {
                    this.DbConnection.mComm.DropParseID(this.ParseID);
                    this.ParseID = null;
                }

                if (this.byMassParseId != null)
                {
                    this.DbConnection.mComm.DropParseID(this.byMassParseId);
                    this.byMassParseId = null;
                }
            }
        }

        /// <summary>
        /// Sets the infos about parameters and result columns.
        /// </summary>
        /// <param name="shortInfo">Info about the parameters and result columns.</param>
        /// <param name="columnNames">The names of the result columns.</param>
        public void SetShortInfosAndColumnNames(MaxDBTranslators.DBTechTranslator[] shortInfo, string[] columnNames)
        {
            // clear the internal dependent fields
            this.InputCount = 0;
            this.HasLongs = false;
            this.ParamInfos = null;
            this.ColumnInfos = null;
            this.ColumnNames = columnNames;

            if (shortInfo == null && columnNames == null)
            {
                this.ParamInfos = this.ColumnInfos = Array.Empty<MaxDBTranslators.DBTechTranslator>();
                return;
            }

            // we have variants:
            // only a select is really good. All other variants
            // do not and never deliver information on being prepared.
            if (this.FuncCode == FunctionCode.Select)
            {
                if (columnNames == null || columnNames.Length == 0)
                {
                    this.ParamInfos = shortInfo;
                    for (int i = 0; i < this.ParamInfos.Length; ++i)
                    {
                        var current = shortInfo[i];
                        if (current.IsInput)
                        {
                            current.ColumnIndex = i;
                            this.InputCount++;
                        }

                        this.HasLongs |= current.IsLongKind;
                    }
                }
                else
                {
                    int column_count = columnNames.Length;
                    this.ColumnInfos = new MaxDBTranslators.DBTechTranslator[column_count];
                    this.ParamInfos = new MaxDBTranslators.DBTechTranslator[shortInfo.Length - column_count];

                    int colInfoIdx = 0;
                    int paramInfoIdx = 0;

                    for (int i = 0; i < shortInfo.Length; ++i)
                    {
                        var current = shortInfo[i];
                        if (current.IsInput)
                        {
                            if (paramInfoIdx == this.ParamInfos.Length)
                            {
                                throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNALUNEXPECTEDINPUT, paramInfoIdx));
                            }

                            current.ColumnIndex = paramInfoIdx;
                            this.ParamInfos[paramInfoIdx] = current;
                            paramInfoIdx++;
                            this.InputCount++;
                        }
                        else
                        {
                            if (colInfoIdx == this.ColumnInfos.Length)
                            {
                                throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNALUNEXPECTEDOUTPUT, colInfoIdx));
                            }

                            this.ColumnInfos[colInfoIdx] = current;
                            current.ColumnIndex = colInfoIdx;
                            current.ColumnName = columnNames[colInfoIdx];
                            colInfoIdx++;
                        }

                        this.HasLongs |= shortInfo[i].IsLongKind;
                    }
                }
            }
            else
            { // no result set data, as we cannot to be sure
                this.ParamInfos = shortInfo;
                if (columnNames != null)
                {
                    // fortunately at least column names
                    // sometimes only output parameters are named
                    if (columnNames.Length == this.ParamInfos.Length)
                    {
                        for (int i = 0; i < columnNames.Length; ++i)
                        {
                            var current = this.ParamInfos[i];
                            current.ColumnIndex = i;
                            current.ColumnName = columnNames[i];
                            if (this.ProcParamInfos != null && i < this.ProcParamInfos.Length)
                            {
                                current.SetProcParamInfo(this.ProcParamInfos[i]);
                            }

                            this.InputCount += (short)(current.IsInput ? 1 : 0);
                            this.HasLongs |= current.IsLongKind;
                        }
                    }
                    else
                    {
                        // we will leave out the input parameters
                        int colNameIdx = 0;
                        for (int j = 0; j < this.ParamInfos.Length; ++j)
                        {
                            var current = this.ParamInfos[j];
                            current.ColumnIndex = j;
                            if (this.ProcParamInfos != null && j < this.ProcParamInfos.Length)
                            {
                                current.SetProcParamInfo(this.ProcParamInfos[j]);
                            }

                            if (current.IsOutput)
                            {
                                current.ColumnName = columnNames[colNameIdx++];
                            }
                            else
                            {
                                ++this.InputCount;
                            }

                            this.HasLongs |= current.IsLongKind;
                        }
                    }
                }
                else
                {
                    // No column names at all. OK.
                    for (int i = 0; i < this.ParamInfos.Length; ++i)
                    {
                        var current = this.ParamInfos[i];
                        current.ColumnIndex = i;
                        if (this.ProcParamInfos != null && i < this.ProcParamInfos.Length)
                        {
                            current.SetProcParamInfo(this.ProcParamInfos[i]);
                        }

                        this.InputCount += (short)(current.IsInput ? 1 : 0);
                        this.HasLongs |= current.IsLongKind;
                    }
                }
            }

            MaxDBTranslators.DBTechTranslator.SetEncoding(this.ParamInfos, this.DbConnection.UserAsciiEncoding);
        }

        /// <summary>
        /// Set meta data.
        /// </summary>
        /// <param name="info">DB Tech translator.</param>
        /// <param name="colName">Column name.</param>
        public void SetMetaData(MaxDBTranslators.DBTechTranslator[] info, string[] colName)
        {
            int colCount = info.Length;
            MaxDBTranslators.DBTechTranslator currentInfo;
            string currentName;
            this.ColumnNames = colName;

            if (colCount == colName.Length)
            {
                this.ColumnInfos = info;
                for (int i = 0; i < colCount; ++i)
                {
                    currentInfo = info[i];
                    currentName = colName[i];
                    currentInfo.ColumnName = currentName;
                    currentInfo.ColumnIndex = i;
                }
            }
            else
            {
                int outputColCnt = 0;
                this.ColumnInfos = new MaxDBTranslators.DBTechTranslator[colName.Length];
                for (int i = 0; i < colCount; ++i)
                {
                    if (info[i].IsOutput)
                    {
                        currentInfo = this.ColumnInfos[outputColCnt] = info[i];
                        currentName = colName[outputColCnt];
                        currentInfo.ColumnName = currentName;
                        currentInfo.ColumnIndex = outputColCnt++;
                    }
                }
            }
        }

        private void DescribeProcedureCall()
        {
            // Syntax is one of
            // { CALL <procedure-name>(...) }
            // CALL <procedure-name>(...)
            // where procedure-name is something like IDENTIFIER, "IDENTIFIER",
            // "OWNER"."IDENTIFIER" etc.
            // we always simply give up if we find nothing that helps our needs
            char[] cmdchars = this.SqlCommand.Trim().ToCharArray();
            int i = 0;
            int cmdchars_len = cmdchars.Length;

            // ODBC like dbfunction call.
            if (cmdchars[i] == '{')
            {
                i++;
            }

            if (i == cmdchars_len)
            {
                return;
            }

            while (char.IsWhiteSpace(cmdchars[i]))
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }

            // 'call'
            if (cmdchars[i] == 'C' || cmdchars[i] == 'c')
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }
            else
            {
                return;
            }

            if (cmdchars[i] == 'A' || cmdchars[i] == 'a')
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }
            else
            {
                return;
            }

            if (cmdchars[i] == 'L' || cmdchars[i] == 'l')
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }
            else
            {
                return;
            }

            if (cmdchars[i] == 'L' || cmdchars[i] == 'l')
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }
            else
            {
                return;
            }

            while (char.IsWhiteSpace(cmdchars[i]))
            {
                if (++i == cmdchars_len)
                {
                    return;
                }
            }

            // now to the mess of parsing the first identifier.
            int idstart = i;
            int idend = i;
            bool quoted = false;
            if (cmdchars[i] == '"')
            {
                ++idstart;
                ++idend;
                quoted = true;
                if (++i == cmdchars_len)
                {
                    return;
                }
            }

            for (; ;)
            {
                if ((cmdchars[i] == '.' && !quoted) || (cmdchars[i] == '(' && !quoted) ||
                    (char.IsWhiteSpace(cmdchars[i]) && !quoted) || (quoted && cmdchars[i] == '"'))
                {
                    break;
                }

                ++idend;
                if (++i == cmdchars_len)
                {
                    return;
                }
            }

            string procedureName = new string(cmdchars, idstart, idend - idstart);
            string ownerName = null;
            if (!quoted)
            {
                procedureName = procedureName.ToUpper(CultureInfo.InvariantCulture);
            }

            if (cmdchars[i] == '"')
            {
                ++i;
            }

            while (i < cmdchars_len && char.IsWhiteSpace(cmdchars[i]))
            {
                if (++i == cmdchars_len)
                {
                    break;
                }
            }

            if (i < cmdchars_len)
            {
                if (cmdchars[i] == '.')
                {
                    if (++i == cmdchars_len)
                    {
                        return;
                    }

                    while (char.IsWhiteSpace(cmdchars[i]))
                    {
                        if (++i == cmdchars_len)
                        {
                            return;
                        }
                    }

                    idstart = i;
                    idend = i;
                    quoted = false;
                    if (cmdchars[i] == '"')
                    {
                        ++idstart;
                        ++idend;
                        quoted = true;
                        if (++i == cmdchars_len)
                        {
                            return;
                        }
                    }

                    for (; ;)
                    {
                        if ((cmdchars[i] == '.' && !quoted) || (cmdchars[i] == '(' && !quoted) ||
                            (char.IsWhiteSpace(cmdchars[i]) && !quoted) || (quoted && cmdchars[i] == '"'))
                        {
                            break;
                        }

                        ++idend;
                        if (++i == cmdchars_len)
                        {
                            return;
                        }
                    }

                    procedureName = new string(cmdchars, idstart, idend - idstart);

                    if (!quoted)
                    {
                        procedureName = procedureName.ToUpper(CultureInfo.InvariantCulture);
                    }
                }
            }

            bool oldKernelVersion = this.DbConnection.KernelVersion < 70400;
            string sql = ownerName == null
                ? oldKernelVersion
                    ? "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, OFFSET AS ASCII_OFFSET, "
                        + "OFFSET AS UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = USER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET"
                    : "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
                        + "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = USER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET"
                : oldKernelVersion
                    ? "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, OFFSET AS ASCII_OFFSET, "
                        + "OFFSET AS UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = :OWNER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET"
                    : "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
                        + "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = :OWNER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET";

            // Now we have procedure name and possibly the user name.
            using (MaxDBCommand cmd = new MaxDBCommand(sql, this.DbConnection))
            {
                if (ownerName != null)
                {
                    cmd.Parameters.Add("OWNER", ownerName);
                }

                cmd.Parameters.Add("DBPROCEDURE", procedureName);

                // We have a result set and can now create a parameter info.
                var rs = cmd.ExecuteReader();
                if (!rs.Read())
                {
                    this.ProcParamInfos = Array.Empty<DBProcParameterInfo>();
                    rs.Close();
                    return;
                }

                var parameterInfos = new List<DBProcParameterInfo>();
                DBProcParameterInfo currentInfo = null;
                int currentIndex = 0;
                do
                {
                    int index = rs.GetInt32(0);

                    // Check if we have a structure element or a new parameter.
                    if (index != currentIndex)
                    {
                        string datatype = rs.GetString(2);
                        if (string.Compare(datatype, "ABAPTABLE", true, CultureInfo.InvariantCulture) == 0 ||
                            string.Compare(datatype, "STRUCTURE", true, CultureInfo.InvariantCulture) == 0)
                        {
                            currentInfo = new DBProcParameterInfo(datatype);
                            parameterInfos.Add(currentInfo);
                        }
                        else
                        {
                            currentInfo = null;
                            parameterInfos.Add(currentInfo);
                        }

                        currentIndex = index;
                    }
                    else
                    {
                        string datatype = rs.GetString(1);
                        string code = rs.GetString(2);
                        int len = rs.GetInt32(3);
                        int dec = rs.GetInt32(4);
                        int asciiOffset = rs.GetInt32(7);
                        int unicodeOffset = rs.GetInt32(8);
                        currentInfo.AddStructureElement(datatype, code, len, dec, asciiOffset, unicodeOffset);
                    }
                }
                while (rs.Read());

                rs.Close();
                this.ProcParamInfos = parameterInfos.ToArray();
            }
        }
    }

    /// <summary>
    /// Database procedure parameter information.
    /// </summary>
    internal class StructureElement
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StructureElement"/> class.
        /// </summary>
        /// <param name="typeName">Element type name.</param>
        /// <param name="codeType">Element code type.</param>
        /// <param name="length">Element length.</param>
        /// <param name="precision">Element precision.</param>
        /// <param name="asciiOffset">Element ASCII offset.</param>
        /// <param name="unicodeOffset">Element Unicode offset.</param>
        public StructureElement(string typeName, string codeType, int length, int precision, int asciiOffset, int unicodeOffset)
        {
            this.TypeName = typeName.ToUpper(CultureInfo.InvariantCulture).Trim();
            this.CodeType = codeType.ToUpper(CultureInfo.InvariantCulture).Trim();
            this.Length = length;
            this.Precision = precision;
            this.AsciiOffset = asciiOffset;
            this.UnicodeOffset = unicodeOffset;
        }

        /// <summary>
        /// Gets an element type name.
        /// </summary>
        public string TypeName { get; private set; }

        /// <summary>
        /// Gets an element code type.
        /// </summary>
        public string CodeType { get; private set; }

        /// <summary>
        /// Gets an element length.
        /// </summary>
        public int Length { get; private set; }

        /// <summary>
        /// Gets an element precision.
        /// </summary>
        public int Precision { get; private set; }

        /// <summary>
        /// Gets an element ASCII offset.
        /// </summary>
        public int AsciiOffset { get; private set; }

        /// <summary>
        /// Gets an element Unicode offset.
        /// </summary>
        public int UnicodeOffset { get; private set; }

        /// <summary>
        /// Gets an element SQL type name.
        /// </summary>
        public string SqlTypeName
        {
            get
            {
                switch (this.TypeName.ToUpper(CultureInfo.InvariantCulture).Trim())
                {
                    case "CHAR":
                        return this.TypeName + "(" + this.Length + ") " + this.CodeType;
                    case "FIXED":
                        return this.TypeName + "(" + this.Length + ", " + this.Precision + ")";
                    case "BOOLEAN":
                        return this.TypeName;
                    default:
                        return this.TypeName + "(" + this.Length + ")";
                }
            }
        }
    }

    /// <summary>
    /// DB Stored Procedure parameter info.
    /// </summary>
    internal class DBProcParameterInfo
    {
        /// <summary>
        /// ABAP table code.
        /// </summary>
        public const int ABAPTABLE = 1;

        /// <summary>
        /// DB Stored Procedure parameter structure.
        /// </summary>
        public const int STRUCTURE = 2;

        private readonly List<StructureElement> lstTypeElements;
        private string strSqlTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="DBProcParameterInfo"/> class.
        /// </summary>
        /// <param name="datatype">The data type as read from DBPROCPARAMINFO.</param>
        public DBProcParameterInfo(string datatype)
        {
            if (string.Compare(datatype.Trim(), "ABAPTABLE", true, CultureInfo.InvariantCulture) == 0)
            {
                this.ElementType = ABAPTABLE;
                this.lstTypeElements = new List<StructureElement>();
            }
            else if (string.Compare(datatype.Trim(), "STRUCTURE", true, CultureInfo.InvariantCulture) == 0)
            {
                this.ElementType = STRUCTURE;
                this.lstTypeElements = new List<StructureElement>();
            }
        }

        /// <summary>
        /// Gets a number of DB Stored Procedure parameters.
        /// </summary>
        public int MemberCount => this.lstTypeElements.Count;

        /// <summary>
        /// Gets an element type.
        /// </summary>
        public int ElementType { get; }

        /// <summary>
        /// Gets an element SQL type name.
        /// </summary>
        public string SQLTypeName
        {
            get
            {
                if (this.strSqlTypeName == null)
                {
                    var typeBuffer = new StringBuilder();
                    var baseType = new StringBuilder();
                    string close = ")";
                    if (this.ElementType == ABAPTABLE)
                    {
                        if (this.lstTypeElements.Count == 1)
                        {
                            var el = this.lstTypeElements[0];
                            if (el.TypeName.ToUpper(CultureInfo.InvariantCulture).Trim() == "CHAR")
                            {
                                if (el.CodeType.ToUpper(CultureInfo.InvariantCulture).Trim() == "ASCII")
                                {
                                    this.strSqlTypeName = "CHARACTER STREAM";
                                }
                                else if (el.CodeType.ToUpper(CultureInfo.InvariantCulture).Trim() == "BYTE")
                                {
                                    this.strSqlTypeName = "BYTE STREAM";
                                }
                            }
                            else if (el.TypeName.ToUpper(CultureInfo.InvariantCulture).Trim() == "WYDE")
                            {
                                this.strSqlTypeName = "CHARACTER STREAM";
                            }

                            typeBuffer.Append("STREAM(");
                        }
                        else
                        {
                            typeBuffer.Append("STREAM(STRUCTURE(");
                            close = "))";
                        }
                    }
                    else
                    {
                        typeBuffer.Append("STRUCTURE(");
                    }

                    for (int i = 0; i < this.lstTypeElements.Count; ++i)
                    {
                        if (i != 0)
                        {
                            baseType.Append(", ");
                            typeBuffer.Append(", ");
                        }

                        var el = this.lstTypeElements[i];
                        typeBuffer.Append(el.SqlTypeName);
                        baseType.Append(el.SqlTypeName);
                    }

                    typeBuffer.Append(close);
                    this.strSqlTypeName = typeBuffer.ToString();
                    this.BaseTypeName = baseType.ToString();
                }

                return this.strSqlTypeName;
            }
        }

        /// <summary>
        /// Gets a base type name.
        /// </summary>
        public string BaseTypeName { get; private set; }

        /// <summary>
        /// Gets a structure element from indexer.
        /// </summary>
        /// <param name="index">Structure element index.</param>
        /// <returns>Structure element.</returns>
        public StructureElement this[int index] => this.lstTypeElements[index];

        /// <summary>
        /// Add structure element.
        /// </summary>
        /// <param name="typeName">Type name from DBPROCPARAMINFO.</param>
        /// <param name="codeType">Code type from DBPROCPARAMINFO.</param>
        /// <param name="length">The length information from DBPROCPARAMINFO.</param>
        /// <param name="precision">The precision information from DBPROCPARAMINFO.</param>
        /// <param name="asciiOffset">ASCII offset from DBPROCPARAMINFO.</param>
        /// <param name="unicodeOffset">Unicode offset from DBPROCPARAMINFO.</param>
        public void AddStructureElement(string typeName, string codeType, int length, int precision, int asciiOffset, int unicodeOffset)
        {
            if (this.lstTypeElements != null)
            {
                this.lstTypeElements.Add(new StructureElement(typeName, codeType, length, precision, asciiOffset, unicodeOffset));
            }
        }
    }

    /// <summary>
    /// Database procedure parameters structure.
    /// </summary>
    internal class DBProcStructure
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DBProcStructure"/> class.
        /// </summary>
        /// <param name="elements">DB Stored procedure attributes.</param>
        public DBProcStructure(object[] elements) => this.Attributes = elements;

        /// <summary>
        /// Gets DB Stored procedure attributes.
        /// </summary>
        public object[] Attributes { get; }
    }

    /// <summary>
    /// Fetch information class.
    /// </summary>
    internal class FetchInfo
    {
        private readonly MaxDBConnection dbConnection;           // current connection
        private readonly string strCursorName;          // cursor
        private MaxDBTranslators.DBTechTranslator[] mColumnInfo;            // short info of all columns
        private string strFetchParamString; // cache for fetch parameters

        /// <summary>
        /// Initializes a new instance of the <see cref="FetchInfo"/> class.
        /// </summary>
        /// <param name="connection">Database connection.</param>
        /// <param name="cursorName">Cursor name.</param>
        /// <param name="infos">DB Translator infos.</param>
        /// <param name="columnNames">Column names.</param>
        public FetchInfo(MaxDBConnection connection, string cursorName, MaxDBTranslators.DBTechTranslator[] infos, string[] columnNames)
        {
            this.dbConnection = connection;
            this.strCursorName = cursorName;
            if (infos != null && columnNames != null)
            {
                this.SetMetaData(infos, columnNames);
            }
        }

        public int NumberOfColumns
        {
            get
            {
                if (this.mColumnInfo == null)
                {
                    this.Describe();
                }

                return this.mColumnInfo.Length;
            }
        }

        public int RecordSize { get; private set; }

        public MaxDBReplyPacket ExecFetchNext()
        {
            if (this.mColumnInfo == null)
            {
                this.Describe();
            }

            if (this.strFetchParamString == null)
            {
                var tmp = new StringBuilder("?");
                for (int i = 1; i < this.mColumnInfo.Length; i++)
                {
                    tmp.Append(", ?");
                }

                this.strFetchParamString = tmp.ToString();
            }

            string cmd = $"FETCH NEXT \"{this.strCursorName}\" INTO {this.strFetchParamString}";

            DateTime dt = DateTime.Now;
            //// >>> SQL TRACE
            this.dbConnection.mLogger.SqlTrace(dt, "::FETCH NEXT " + this.strCursorName);
            this.dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + cmd);
            //// <<< SQL TRACE

            var request = this.dbConnection.mComm.GetRequestPacket();
            byte currentSQLMode = request.SwitchSqlMode((byte)SqlMode.Internal);
            request.InitDbsCommand(this.dbConnection.AutoCommit, cmd);

            request.SetMassCommand();
            request.AddResultCount(30000);

            try
            {
                return this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, request, this, GCMode.DELAYED);
            }
            finally
            {
                request.SwitchSqlMode(currentSQLMode);
            }
        }

        public MaxDBTranslators.DBTechTranslator GetColumnInfo(int index)
        {
            if (this.mColumnInfo == null)
            {
                this.Describe();
            }

            return this.mColumnInfo[index];
        }

        private void SetMetaData(MaxDBTranslators.DBTechTranslator[] info, string[] colName)
        {
            int colCount = info.Length;
            MaxDBTranslators.DBTechTranslator currentInfo;
            int currentFieldEnd;

            this.RecordSize = 0;

            if (colCount == colName.Length)
            {
                this.mColumnInfo = info;
                for (int i = 0; i < colCount; ++i)
                {
                    currentInfo = info[i];
                    currentInfo.ColumnName = colName[i];
                    currentInfo.ColumnIndex = i;
                    currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
                    this.RecordSize = Math.Max(this.RecordSize, currentFieldEnd);
                }
            }
            else
            {
                int outputColCnt = 0;
                this.mColumnInfo = new MaxDBTranslators.DBTechTranslator[colName.Length];
                for (int i = 0; i < colCount; ++i)
                {
                    if (info[i].IsOutput)
                    {
                        currentInfo = this.mColumnInfo[outputColCnt] = info[i];
                        currentInfo.ColumnName = colName[outputColCnt];
                        currentInfo.ColumnIndex = outputColCnt++;
                        currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
                        this.RecordSize = Math.Max(this.RecordSize, currentFieldEnd);
                    }
                }
            }

            MaxDBTranslators.DBTechTranslator.SetEncoding(this.mColumnInfo, this.dbConnection.UserAsciiEncoding);
        }

        private void Describe()
        {
            MaxDBTranslators.DBTechTranslator[] infos = null;
            string[] columnNames = null;
            var request = this.dbConnection.mComm.GetRequestPacket();
            byte currentSQLMode = request.SwitchSqlMode((byte)SqlMode.Internal);

            try
            {
                request.InitDbsCommand(false, $"DESCRIBE \"{this.strCursorName}\"");

                //// >>> SQL TRACE
                this.dbConnection.mLogger.SqlTrace(DateTime.Now, "::DESCRIBE CURSOR " + this.strCursorName);
                //// <<< SQL TRACE

                MaxDBReplyPacket reply = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, request, this, GCMode.ALLOWED);
                reply.ClearPartOffset();
                for (int i = 0; i < reply.PartCount; i++)
                {
                    reply.NextPart();

                    int partType = reply.PartType;

                    if (partType == PartKind.ColumnNames)
                    {
                        columnNames = reply.ParseColumnNames();
                    }
                    else if (partType == PartKind.ShortInfo)
                    {
                        infos = reply.ParseShortFields(this.dbConnection.mComm.ConnStrBuilder.SpaceOption, false, null, false);
                    }
                    else if (partType == PartKind.VardataShortInfo)
                    {
                        infos = reply.ParseShortFields(this.dbConnection.mComm.ConnStrBuilder.SpaceOption, false, null, true);
                    }
                }

                this.SetMetaData(infos, columnNames);
            }
            catch
            {
                throw;
            }
            finally
            {
                request.SwitchSqlMode(currentSQLMode);
            }
        }
    }

    /// <summary>
    /// General column information.
    /// </summary>
    internal class GeneralColumnInfo
    {
        public static bool IsLong(int columnType)
        {
            switch (columnType)
            {
                case DataType.STRA:
                case DataType.STRE:
                case DataType.STRB:
                case DataType.STRUNI:
                case DataType.LONGA:
                case DataType.LONGE:
                case DataType.LONGB:
                case DataType.LONGDB:
                case DataType.LONGUNI:
                    return true;
                default:
                    return false;
            }
        }

        public static bool IsTextual(int columnType)
        {
            switch (columnType)
            {
                case DataType.STRA:
                case DataType.STRE:
                case DataType.STRUNI:
                case DataType.LONGA:
                case DataType.LONGE:
                case DataType.LONGUNI:
                case DataType.VARCHARA:
                case DataType.VARCHARE:
                case DataType.VARCHARUNI:
                    return true;
                default:
                    return false;
            }
        }

        public static string GetTypeName(int columnType)
        {
            switch (columnType)
            {
                case DataType.CHA:
                case DataType.CHE:
                case DataType.DBYTEEBCDIC:
                    return DataType.StrValues[DataType.CHA];
                case DataType.CHB:
                case DataType.ROWID:
                    return DataType.StrValues[DataType.CHB];
                case DataType.UNICODE:
                    return DataType.StrValues[DataType.UNICODE];
                case DataType.VARCHARA:
                case DataType.VARCHARE:
                    return DataType.StrValues[DataType.VARCHARA];
                case DataType.VARCHARB:
                    return DataType.StrValues[DataType.VARCHARB];
                case DataType.VARCHARUNI:
                    return DataType.StrValues[DataType.VARCHARUNI];
                case DataType.STRA:
                case DataType.STRE:
                case DataType.LONGA:
                case DataType.LONGE:
                case DataType.LONGDB:
                    return DataType.StrValues[DataType.LONGA];
                case DataType.STRB:
                case DataType.LONGB:
                    return DataType.StrValues[DataType.LONGB];
                case DataType.STRUNI:
                case DataType.LONGUNI:
                    return DataType.StrValues[DataType.LONGUNI];
                case DataType.DATE:
                    return DataType.StrValues[DataType.DATE];
                case DataType.TIME:
                    return DataType.StrValues[DataType.TIME];
                case DataType.TIMESTAMP:
                    return DataType.StrValues[DataType.TIMESTAMP];
                case DataType.BOOLEAN:
                    return DataType.StrValues[DataType.BOOLEAN];
                case DataType.FIXED:
                case DataType.NUMBER:
                    return DataType.StrValues[DataType.FIXED];
                case DataType.FLOAT:
                case DataType.VFLOAT:
                    return DataType.StrValues[DataType.FLOAT];
                case DataType.SMALLINT:
                    return DataType.StrValues[DataType.SMALLINT];
                case DataType.INTEGER:
                    return DataType.StrValues[DataType.INTEGER];
                default:
                    return MaxDBMessages.Extract(MaxDBError.UNKNOWNTYPE);
            }
        }

        public static Type GetType(int columnType)
        {
            switch (columnType)
            {
                case DataType.FIXED:
                case DataType.FLOAT:
                case DataType.VFLOAT:
                case DataType.NUMBER:
                case DataType.NONUMBER:
                    return typeof(decimal);
                case DataType.CHA:
                case DataType.CHE:
                    return typeof(string);
                case DataType.CHB:
                case DataType.ROWID:
                    return typeof(byte[]);
                case DataType.DATE:
                case DataType.TIME:
                case DataType.TIMESTAMP:
                    return typeof(DateTime);
                case DataType.UNKNOWN:
                    return typeof(object);
                case DataType.DURATION:
                    return typeof(long);
                case DataType.DBYTEEBCDIC:
                case DataType.STRA:
                case DataType.STRE:
                case DataType.LONGA:
                case DataType.LONGE:
                case DataType.STRUNI:
                    return typeof(string);
                case DataType.STRB:
                case DataType.LONGB:
                case DataType.LONGDB:
                case DataType.LONGUNI:
                    return typeof(byte[]);
                case DataType.BOOLEAN:
                    return typeof(bool);
                case DataType.UNICODE:
                case DataType.VARCHARUNI:
                    return typeof(string);
                case DataType.DTFILLER1:
                case DataType.DTFILLER2:
                case DataType.DTFILLER3:
                case DataType.DTFILLER4:
                    return typeof(object);
                case DataType.SMALLINT:
                    return typeof(short);
                case DataType.INTEGER:
                    return typeof(int);
                case DataType.VARCHARA:
                case DataType.VARCHARE:
                    return typeof(string);
                case DataType.VARCHARB:
                    return typeof(byte[]);
                default:
                    return typeof(object);
            }
        }

        public static MaxDBType GetMaxDBType(int columnType)
        {
            switch (columnType)
            {
                case DataType.FIXED:
                    return MaxDBType.Fixed;
                case DataType.FLOAT:
                    return MaxDBType.Float;
                case DataType.VFLOAT:
                    return MaxDBType.VFloat;
                case DataType.NUMBER:
                    return MaxDBType.Number;
                case DataType.NONUMBER:
                    return MaxDBType.NoNumber;
                case DataType.CHA:
                    return MaxDBType.CharA;
                case DataType.CHE:
                    return MaxDBType.CharE;
                case DataType.CHB:
                    return MaxDBType.CharB;
                case DataType.ROWID:
                    return MaxDBType.RowId;
                case DataType.DATE:
                    return MaxDBType.Date;
                case DataType.TIME:
                    return MaxDBType.Time;
                case DataType.TIMESTAMP:
                    return MaxDBType.Timestamp;
                case DataType.UNKNOWN:
                    return MaxDBType.Unknown;
                case DataType.DURATION:
                    return MaxDBType.Duration;
                case DataType.DBYTEEBCDIC:
                    return MaxDBType.DByteEbcdic;
                case DataType.STRA:
                    return MaxDBType.StrA;
                case DataType.STRE:
                    return MaxDBType.StrE;
                case DataType.LONGA:
                    return MaxDBType.LongA;
                case DataType.LONGE:
                    return MaxDBType.LongE;
                case DataType.STRUNI:
                    return MaxDBType.StrUni;
                case DataType.STRB:
                    return MaxDBType.StrB;
                case DataType.LONGB:
                    return MaxDBType.LongB;
                case DataType.LONGDB:
                    return MaxDBType.LongDB;
                case DataType.LONGUNI:
                    return MaxDBType.LongUni;
                case DataType.BOOLEAN:
                    return MaxDBType.Boolean;
                case DataType.UNICODE:
                    return MaxDBType.Unicode;
                case DataType.VARCHARUNI:
                    return MaxDBType.VarCharUni;
                case DataType.DTFILLER1:
                    return MaxDBType.DTFiller1;
                case DataType.DTFILLER2:
                    return MaxDBType.DTFiller2;
                case DataType.DTFILLER3:
                    return MaxDBType.DTFiller3;
                case DataType.DTFILLER4:
                    return MaxDBType.DTFiller4;
                case DataType.SMALLINT:
                    return MaxDBType.SmallInt;
                case DataType.INTEGER:
                    return MaxDBType.Integer;
                case DataType.VARCHARA:
                    return MaxDBType.VarCharA;
                case DataType.VARCHARE:
                    return MaxDBType.VarCharE;
                case DataType.VARCHARB:
                    return MaxDBType.VarCharB;
                default:
                    return MaxDBType.Unknown;
            }
        }
    }
}
