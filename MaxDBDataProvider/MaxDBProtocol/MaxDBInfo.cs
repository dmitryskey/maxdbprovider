// Copyright © 2005-2018 Dmitry S. Kataev
// Copyright © 2002-2003 SAP AG
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

    #region "Parse information class"

    internal class MaxDBParseInfo
    {
        internal MaxDBConnection dbConnection;
        private byte[] byMassParseId;
        internal MaxDBTranslators.DBTechTranslator[] mParamInfos;
        internal DBProcParameterInfo[] mProcParamInfos;
        internal short sInputCount;
        private int iSessionId; // unique identifier for the connection
        internal string[] strColumnNames;

        internal MaxDBTranslators.DBTechTranslator[] mColumnInfos;

        internal bool bVarDataInput;

        // 11th Byte of Parseid coded application code
        private const int iApplCodeByte = 10;

        public MaxDBParseInfo(MaxDBConnection connection, string sqlCmd, int functionCode)
        {
            this.dbConnection = connection;
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

        ~MaxDBParseInfo()
        {
            this.IsCached = false;
            this.DropParseIDs();
        }

        public bool IsSelect { get; set; }

        public bool IsDBProc { get; }

        public int FuncCode { get; }

        public bool IsMassCmd { get; private set; }

        public bool HasLongs { get; private set; }

        public bool IsCached { get; set; }

        public string UpdatedTableName { get; set; }

        public string SqlCommand { get; }

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

            for (; ; )
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

                    for (; ; )
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

            // Now we have procedure name and possibly the user name.
            MaxDBCommand cmd = null;
            string sql = "SELECT 1 FROM DUAL WHERE FALSE";
            if (ownerName == null)
            {
                if (this.dbConnection.KernelVersion < 70400)
                {
                    sql = "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, OFFSET AS ASCII_OFFSET, "
                        + "OFFSET AS UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER=USER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET";
                }
                else
                {
                    sql = "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
                        + "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER=USER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET";
                }

                cmd = new MaxDBCommand(sql, this.dbConnection);
                cmd.Parameters.Add("DBPROCEDURE", procedureName);
            }
            else
            {
                if (this.dbConnection.KernelVersion < 70400)
                {
                    sql = "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, OFFSET AS ASCII_OFFSET, "
                        + "OFFSET AS UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = :OWNER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET";
                }
                else
                {
                    sql = "SELECT PARAM_NO, "
                        + "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
                        + "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER = :OWNER AND "
                        + "DBPROCEDURE = :DBPROCEDURE ORDER BY PARAM_NO, ASCII_OFFSET";
                }

                cmd = new MaxDBCommand(sql, this.dbConnection);
                cmd.Parameters.Add("OWNER", ownerName);
                cmd.Parameters.Add("DBPROCEDURE", procedureName);
            }

            // We have a result set and can now create a parameter info.
            var rs = cmd.ExecuteReader();
            if (!rs.Read())
            {
                this.mProcParamInfos = Array.Empty<DBProcParameterInfo>();
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
            this.mProcParamInfos = parameterInfos.ToArray();
        }

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
                    if (value[iApplCodeByte] == FunctionCode.massCmdAppCodes[i])
                    {
                        this.IsMassCmd = true;
                        return;
                    }
                }
            }
        }

        /**
         * Checks the validity. A parse info is valid if the session is the same as
         * of the current connection.
         *
         * @return <code>true</code> if the session ids are equal
         */
        public bool IsValid => this.iSessionId == this.dbConnection.mComm.SessionID;

        /**
         * Sets a parse id, together with the correct session id.
         *
         * @param parseId
         *            the parse id.
         * @param sessionId
         *            the session id of the parse id.
         */
        public void SetParseIDAndSession(byte[] parseId, int sessionId)
        {
            this.iSessionId = sessionId;
            this.ParseID = parseId;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DropParseIDs()
        {
            if (this.dbConnection != null && this.dbConnection.mComm != null)
            {
                if (this.ParseID != null)
                {
                    this.dbConnection.mComm.DropParseID(this.ParseID);
                    this.ParseID = null;
                }

                if (this.byMassParseId != null)
                {
                    this.dbConnection.mComm.DropParseID(this.byMassParseId);
                    this.byMassParseId = null;
                }
            }
        }

        // Gets the information about parameters in sql statement
        public MaxDBTranslators.DBTechTranslator[] ParamInfo => this.mParamInfos;

        public MaxDBTranslators.DBTechTranslator[] ColumnInfo => this.mColumnInfos;

        /**
         * Gets the parse id.
         */
        public byte[] ParseID { get; private set; }

        /**
         * Retrieves whether the statement is already executed during parse. (by
         * checking byte 11 of the parse if for <code>csp1_p_command_executed</code>.
         */
        public bool IsAlreadyExecuted => this.ParseID != null && this.ParseID[MaxDBParseInfo.iApplCodeByte] == FunctionCode.command_executed;

        /**
         * Sets the infos about parameters and result columns.
         *
         * @param shortInfo
         *            info about the parameters and result columns
         * @param columnames
         *            the names of the result columns
         */
        public void SetShortInfosAndColumnNames(MaxDBTranslators.DBTechTranslator[] shortInfo, string[] columnNames)
        {
            // clear the internal dependent fields
            this.sInputCount = 0;
            this.HasLongs = false;
            this.mParamInfos = null;
            this.mColumnInfos = null;
            this.strColumnNames = columnNames;

            if (shortInfo == null && columnNames == null)
            {
                this.mParamInfos = this.mColumnInfos = Array.Empty<MaxDBTranslators.DBTechTranslator>();
                return;
            }

            // we have variants:
            // only a select is really good. All other variants
            // do not and never deliver information on being prepared.
            if (this.FuncCode == FunctionCode.Select)
            {
                if (columnNames == null || columnNames.Length == 0)
                {
                    this.mParamInfos = shortInfo;
                    for (int i = 0; i < this.mParamInfos.Length; ++i)
                    {
                        var current = shortInfo[i];
                        if (current.IsInput)
                        {
                            current.ColumnIndex = i;
                            this.sInputCount++;
                        }

                        this.HasLongs |= current.IsLongKind;
                    }
                }
                else
                {
                    int column_count = columnNames.Length;
                    this.mColumnInfos = new MaxDBTranslators.DBTechTranslator[column_count];
                    this.mParamInfos = new MaxDBTranslators.DBTechTranslator[shortInfo.Length - column_count];

                    int colInfoIdx = 0;
                    int paramInfoIdx = 0;

                    for (int i = 0; i < shortInfo.Length; ++i)
                    {
                        var current = shortInfo[i];
                        if (current.IsInput)
                        {
                            if (paramInfoIdx == this.mParamInfos.Length)
                            {
                                throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_UNEXPECTEDINPUT, paramInfoIdx));
                            }

                            current.ColumnIndex = paramInfoIdx;
                            this.mParamInfos[paramInfoIdx] = current;
                            paramInfoIdx++;
                            this.sInputCount++;
                        }
                        else
                        {
                            if (colInfoIdx == this.mColumnInfos.Length)
                            {
                                throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_UNEXPECTEDOUTPUT, colInfoIdx));
                            }

                            this.mColumnInfos[colInfoIdx] = current;
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
                this.mParamInfos = shortInfo;
                if (columnNames != null)
                {
                    // fortunately at least column names
                    // sometimes only output parameters are named
                    if (columnNames.Length == this.mParamInfos.Length)
                    {
                        for (int i = 0; i < columnNames.Length; ++i)
                        {
                            var current = this.mParamInfos[i];
                            current.ColumnIndex = i;
                            current.ColumnName = columnNames[i];
                            if (this.mProcParamInfos != null && i < this.mProcParamInfos.Length)
                            {
                                current.SetProcParamInfo(this.mProcParamInfos[i]);
                            }

                            this.sInputCount += (short)(current.IsInput ? 1 : 0);
                            this.HasLongs |= current.IsLongKind;
                        }
                    }
                    else
                    {
                        // we will leave out the input parameters
                        int colNameIdx = 0;
                        for (int j = 0; j < this.mParamInfos.Length; ++j)
                        {
                            var current = this.mParamInfos[j];
                            current.ColumnIndex = j;
                            if (this.mProcParamInfos != null && j < this.mProcParamInfos.Length)
                            {
                                current.SetProcParamInfo(this.mProcParamInfos[j]);
                            }

                            if (current.IsOutput)
                            {
                                current.ColumnName = columnNames[colNameIdx++];
                            }
                            else
                            {
                                ++this.sInputCount;
                            }

                            this.HasLongs |= current.IsLongKind;
                        }
                    }
                }
                else
                {
                    // No column names at all. OK.
                    for (int i = 0; i < this.mParamInfos.Length; ++i)
                    {
                        var current = this.mParamInfos[i];
                        current.ColumnIndex = i;
                        if (this.mProcParamInfos != null && i < this.mProcParamInfos.Length)
                        {
                            current.SetProcParamInfo(this.mProcParamInfos[i]);
                        }

                        this.sInputCount += (short)(current.IsInput ? 1 : 0);
                        this.HasLongs |= current.IsLongKind;
                    }
                }
            }

            MaxDBTranslators.DBTechTranslator.SetEncoding(this.mParamInfos, this.dbConnection.UserAsciiEncoding);
        }

        public void SetMetaData(MaxDBTranslators.DBTechTranslator[] info, string[] colName)
        {
            int colCount = info.Length;
            MaxDBTranslators.DBTechTranslator currentInfo;
            string currentName;
            this.strColumnNames = colName;

            if (colCount == colName.Length)
            {
                this.mColumnInfos = info;
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
                this.mColumnInfos = new MaxDBTranslators.DBTechTranslator[colName.Length];
                for (int i = 0; i < colCount; ++i)
                {
                    if (info[i].IsOutput)
                    {
                        currentInfo = this.mColumnInfos[outputColCnt] = info[i];
                        currentName = colName[outputColCnt];
                        currentInfo.ColumnName = currentName;
                        currentInfo.ColumnIndex = outputColCnt++;
                    }
                }
            }
        }
    }

    #endregion

    #region "Database procedure parameter information"

    internal class StructureElement
    {
        public string strTypeName;
        public string strCodeType;
        public int iLength;
        public int iPrecision;
        public int iASCIIOffset;
        public int iUnicodeOffset;

        public StructureElement(string typeName, string codeType, int length, int precision, int asciiOffset, int unicodeOffset)
        {
            this.strTypeName = typeName.ToUpper(CultureInfo.InvariantCulture).Trim();
            this.strCodeType = codeType.ToUpper(CultureInfo.InvariantCulture).Trim();
            this.iLength = length;
            this.iPrecision = precision;
            this.iASCIIOffset = asciiOffset;
            this.iUnicodeOffset = unicodeOffset;
        }

        public string SqlTypeName
        {
            get
            {
                switch (this.strTypeName.ToUpper(CultureInfo.InvariantCulture).Trim())
                {
                    case "CHAR":
                        return this.strTypeName + "(" + this.iLength + ") " + this.strCodeType;
                    case "FIXED":
                        return this.strTypeName + "(" + this.iLength + ", " + this.iPrecision + ")";
                    case "BOOLEAN":
                        return this.strTypeName;
                    default:
                        return this.strTypeName + "(" + this.iLength + ")";
                }
            }
        }
    }

    internal class DBProcParameterInfo
    {
        public const int ABAPTABLE = 1;
        public const int STRUCTURE = 2;
        private string strSqlTypeName;
        private readonly List<StructureElement> lstTypeElements;

        /*
          Creates a new DB procedure parameter info.
          @param datatype The data type as read from DBPROCPARAMINFO.
          @param len The length information from DBPROCPARAMINFO.
          @param dec The precision information from DBPROCPARAMINFO.
        */
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

        public void AddStructureElement(string typeName, string codeType, int length, int precision, int asciiOffset, int unicodeOffset)
        {
            if (this.lstTypeElements == null)
            {
                return;
            }
            else
            {
                this.lstTypeElements.Add(new StructureElement(typeName, codeType, length, precision, asciiOffset, unicodeOffset));
            }
        }

        public int MemberCount => this.lstTypeElements.Count;

        public int ElementType { get; }

        public StructureElement this[int index] => this.lstTypeElements[index];

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
                            if (el.strTypeName.ToUpper(CultureInfo.InvariantCulture).Trim() == "CHAR")
                            {
                                if (el.strCodeType.ToUpper(CultureInfo.InvariantCulture).Trim() == "ASCII")
                                {
                                    this.strSqlTypeName = "CHARACTER STREAM";
                                }
                                else if (el.strCodeType.ToUpper(CultureInfo.InvariantCulture).Trim() == "BYTE")
                                {
                                    this.strSqlTypeName = "BYTE STREAM";
                                }
                            }
                            else if (el.strTypeName.ToUpper(CultureInfo.InvariantCulture).Trim() == "WYDE")
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

        public string BaseTypeName { get; private set; }
    }

    #endregion

    #region "Database procedure parameters structure"

    internal class DBProcStructure
    {
        public DBProcStructure(object[] elements) => this.Attributes = elements;

        public object[] Attributes { get; }
    }
    #endregion

    #region "SQL parameter controller interface"

    internal interface ISqlParameterController
    {
        MaxDBConnection Connection { get; }

        ByteArray ReplyData { get; }
    }

    #endregion

    #region "Fetch information class"

    internal class FetchInfo
    {
        private readonly MaxDBConnection dbConnection;           // current connection
        private readonly string strCursorName;          // cursor
        private MaxDBTranslators.DBTechTranslator[] mColumnInfo;            // short info of all columns
        private string strFetchParamString; // cache for fetch parameters

        public FetchInfo(MaxDBConnection connection, string cursorName, MaxDBTranslators.DBTechTranslator[] infos, string[] columnNames)
        {
            this.dbConnection = connection;
            this.strCursorName = cursorName;
            if (infos != null && columnNames != null)
            {
                this.SetMetaData(infos, columnNames);
            }
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
                request.InitDbsCommand(false, "DESCRIBE \"" + this.strCursorName + "\"");

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

            string cmd = "FETCH NEXT \"" + this.strCursorName + "\" INTO " + this.strFetchParamString;

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
    }

    #endregion

    #region "General column information"

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

    #endregion
}
