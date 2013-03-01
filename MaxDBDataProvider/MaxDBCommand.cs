//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBCommand.cs" company="Dmitry S. Kataev">
//     Copyright (C) 2005-2011 Dmitry S. Kataev
//     Copyright (C) 2002-2003 SAP AG
// </copyright>
//-----------------------------------------------------------------------------------------------
//
//  This program is free software; you can redistribute it and/or
//  modify it under the terms of the GNU General Public License
//  as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Text;
    using MaxDBProtocol;
    using Utilities;

    /// <summary>
    /// Represents a SQL statement to execute against a MaxDB database. This class cannot be inherited.
    /// </summary>
    public sealed class MaxDBCommand : DbCommand, ICloneable, ISqlParameterController
    {
        private const int iMaxParseAgainCnt = 10;
        private MaxDBConnection dbConnection;
        private MaxDBTransaction dbTransaction;
        private string strCmdText;
        private UpdateRowSource updatedRowSource = UpdateRowSource.None;
        private MaxDBParameterCollection dbParameters = new MaxDBParameterCollection();
        private CommandType mCmdType = CommandType.Text;
        private bool bDesignTimeVisible;

        #region "Native implementation parameters"

        private List<AbstractProcedurePutValue> lstInputProcedureLongs;
        private bool bSetWithInfo;
        private bool bHasRowCount;

        private List<PutValue> lstInputLongs;
        private MaxDBDataReader mCurrentDataReader;
        private object[] objInputArgs;
        private string strCursorName;
        private bool bCanceled;
        private ByteArray baReplyMemory;
        private const string strInitialParamValue = "initParam";
        private static PutValueComparator mPutValueComparator = new PutValueComparator();

        internal MaxDBParseInfo mParseInfo;
        #endregion

        internal int iRowsAffected = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommand"/> class.
        /// </summary>
        public MaxDBCommand()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommand"/> class with the text of the query.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        public MaxDBCommand(string cmdText)
        {
            this.strCmdText = cmdText;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommand"/> class 
        /// with the text of the query and a <see cref="MaxDBConnection"/>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="MaxDBConnection"/> that represents the connection to an instance of MaxDB Server.</param>
        public MaxDBCommand(string cmdText, MaxDBConnection connection) : this(cmdText)
        {
            this.dbConnection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommand"/> class with the text of the query, a <see cref="MaxDBConnection"/>, 
        /// and the <see cref="MaxDBTransaction"/>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="MaxDBConnection"/> that represents the connection to an instance of MaxDB Server.</param>
        /// <param name="transaction">The <see cref="MaxDBTransaction"/> in which the <see cref="MaxDBCommand"/> executes.</param>
        public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction transaction) : this(cmdText, connection)
        {
            this.dbTransaction = transaction;
        }

        /// <summary>
        /// Validate whether current connection is opened or not.
        /// </summary>
        internal void AssertOpen()
        {
            if (this.dbConnection == null || this.dbConnection.mComm == null
            )
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a Windows Forms Designer control.
        /// </summary>
        public override bool DesignTimeVisible
        {
            get
            {
                return this.bDesignTimeVisible;
            }

            set
            {
                this.bDesignTimeVisible = value;
            }
        }

        internal int[] ExecuteBatch(MaxDBParameterCollection[] batchParams)
        {
            this.AssertOpen();
            this.strCursorName = this.dbConnection.mComm.NextCursorName;

            this.Prepare();

            if (this.mParseInfo != null && this.mParseInfo.IsSelect)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHRESULTSET, new int[0]));
            if (this.mParseInfo != null
                && (this.mParseInfo.FuncCode == FunctionCode.DBProcExecute || this.mParseInfo.FuncCode == FunctionCode.DBProcWithResultSetExecute))
                foreach (DBTechTranslator transl in this.mParseInfo.mParamInfos)
                {
                    if (transl.IsOutput)
                    {
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHPROCOUT, new int[0]));
                    }
                }

            if (batchParams == null)
            {
                return new int[0];
            }

            ////>>> SQL TRACE
            DateTime dt = DateTime.Now;

            if (this.dbConnection.mLogger.TraceSQL)
            {
                this.dbConnection.mLogger.SqlTrace(dt, "::EXECUTE BATCH " + this.strCursorName);
                this.dbConnection.mLogger.SqlTrace(dt, "BATCH SIZE " + batchParams.Length);
            }
            //////<<< SQL TRACE

            List<PutValue> streamVec = null;
            bool inTrans = this.dbConnection.mComm.IsInTransaction;

            try
            {
                this.bCanceled = false;
                int count = batchParams.Length;
                MaxDBRequestPacket requestPacket;
                MaxDBReplyPacket replyPacket;
                int inputCursor = 0;
                const bool noError = true;
                int recordSize = 0;
                int insertCountPartSize = MaxDBRequestPacket.ResultCountPartSize;
                int executeCount = -1;
                int[] result = new int[count];
                this.iRowsAffected = -1;

                ////>>> SQL TRACE
                if (this.dbConnection.mLogger.TraceSQL && this.mParseInfo != null)
                {
                    this.dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(this.mParseInfo.ParseID));
                    this.dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + this.mParseInfo.SqlCommand);
                }
                //////<<< SQL TRACE

                if (this.mParseInfo == null || this.mParseInfo.MassParseID == null)
                {
                    this.ParseMassCmd(false);
                }

                if (this.mParseInfo != null && this.mParseInfo.mParamInfos.Length > 0)
                {
                    foreach (DBTechTranslator currentInfo in this.mParseInfo.mParamInfos)
                    {
                        if (currentInfo.IsInput)
                        {
                            int currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
                            recordSize = Math.Max(recordSize, currentFieldEnd);
                        }
                    }
                }

                while ((inputCursor < count) && noError)
                {
                    streamVec = null;
                    int firstRecordNo = inputCursor;
                    requestPacket = this.dbConnection.mComm.GetRequestPacket();
                    requestPacket.InitExecute(this.mParseInfo.MassParseID, this.dbConnection.AutoCommit);
                    if (executeCount == -1)
                    {
                        requestPacket.AddUndefinedResultCount();
                    }
                    else
                    {
                        requestPacket.AddResultCount(executeCount);
                    }

                    requestPacket.AddCursorPart(this.strCursorName);
                    DataPart dataPart;
                    if (this.mParseInfo.mParamInfos.Length > 0)
                    {
                        dataPart = requestPacket.NewDataPart(this.mParseInfo.bVarDataInput);
                        if (executeCount == -1)
                        {
                            dataPart.SetFirstPart();
                        }

                        do
                        {
                            object[] row = new object[batchParams[inputCursor].Count];
                            this.FillInputParameters(batchParams[inputCursor], ref row);
                            dataPart.AddRow(this.mParseInfo.sInputCount);
                            for (int i = 0; i < this.mParseInfo.mParamInfos.Length; i++)
                            {
                                // check whether the parameter was set by application or throw an exception
                                DBTechTranslator transl = this.mParseInfo.mParamInfos[i];

                                if (transl.IsInput && row[i] != null && strInitialParamValue == row[i].ToString())
                                {
                                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHMISSINGIN, (inputCursor + 1).ToString(CultureInfo.InvariantCulture), (i + 1).ToString(CultureInfo.InvariantCulture)), "0200");
                                }

                                transl.Put(dataPart, row[i]);
                            }

                            if (this.mParseInfo.HasLongs)
                            {
                                this.HandleStreamsForExecute(dataPart, row);
                                if (streamVec == null)
                                {
                                    streamVec = new List<PutValue>(this.lstInputLongs);
                                }
                            }

                            dataPart.MoveRecordBase();
                            inputCursor++;
                        } 
                        while ((inputCursor < count) && dataPart.HasRoomFor(recordSize, insertCountPartSize) && this.mParseInfo.IsMassCmd);

                        if (inputCursor == count)
                            dataPart.SetLastPart();
                        dataPart.CloseArrayPart((short)(inputCursor - firstRecordNo));
                    }
                    else
                    {
                        // commands without parameters
                        inputCursor++;
                    }

                    try
                    {
                        replyPacket = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, this, GCMode.GC_DELAYED);
                    }
                    catch (MaxDBException ex)
                    {
                        if (ex.ErrorCode == -8)
                        {
                            ResetPutValues(streamVec);
                            this.ParseMassCmd(true);
                            inputCursor = firstRecordNo;
                            continue;   // redo this packet
                        }

                        // An autocommit session does roll back automatically all what was
                        // in this package. Thus, in case of an error we must not
                        // add the current count from the packet.
                        if (!this.dbConnection.AutoCommit)
                        {
                            if (this.iRowsAffected > 0)
                            {
                                this.iRowsAffected += ex.ErrorPos - 1;
                            }
                            else
                            {
                                this.iRowsAffected = ex.ErrorPos - 1;
                            }
                        }

                        throw;
                    }

                    executeCount = replyPacket.ResultCount(false);
                    if (this.mParseInfo.HasLongs)
                    {
                        this.HandleStreamsForPutValue(replyPacket);
                    }

                    if (this.mParseInfo.IsMassCmd && executeCount != -1)
                    {
                        if (this.iRowsAffected > 0)
                        {
                            this.iRowsAffected += executeCount;
                        }
                        else
                        {
                            this.iRowsAffected = executeCount;
                        }
                    }
                    else
                    {
                        this.iRowsAffected = executeCount;
                    }
                }

                return result;
            }
            catch (MaxDBTimeoutException)
            {
                if (inTrans)
                {
                    throw;
                }
                else
                {
                    ResetPutValues(streamVec);
                    return this.ExecuteBatch(batchParams);
                }
            }
            finally
            {
                this.bCanceled = false;
            }
        }

        #region "Methods to support native protocol"

        private MaxDBReplyPacket SendCommand(MaxDBRequestPacket requestPacket, string sqlCmd, int gcFlags, bool parseAgain)
        {
            requestPacket.InitParseCommand(sqlCmd, true, parseAgain);
            if (this.bSetWithInfo)
            {
                requestPacket.SetWithInfo();
            }

            MaxDBReplyPacket replyPacket = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, false, true, this, gcFlags);
            return replyPacket;
        }

        private MaxDBReplyPacket SendSqlCommand(string sql, bool parseAgain)
        {
            MaxDBReplyPacket replyPacket;

            try
            {
                replyPacket = this.SendCommand(this.dbConnection.mComm.GetRequestPacket(), sql, GCMode.GC_ALLOWED, parseAgain);
            }
            catch (IndexOutOfRangeException)
            {
                // tbd: info about current length?
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLSTATEMENT_TOOLONG), "42000");
            }

            return replyPacket;
        }

        private void Reparse()
        {
            object[] tmpArgs = this.objInputArgs;
            this.mParseInfo = this.DoParse(this.mParseInfo.SqlCommand + " ", true);
            this.objInputArgs = tmpArgs;
        }

        internal bool Execute(CommandBehavior behavior)
        {
            this.AssertOpen();
            this.strCursorName = this.dbConnection.mComm.NextCursorName;

            return this.Execute(iMaxParseAgainCnt, behavior);
        }

        internal bool Execute(int afterParseAgain, CommandBehavior behavior)
        {
            if (this.dbConnection == null)
                throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_CONNECTIONNULL));

            ////>>> SQL TRACE
            DateTime dt = DateTime.Now;

            if (this.dbConnection.mLogger.TraceSQL)
            {
                this.dbConnection.mLogger.SqlTrace(dt, "::EXECUTE " + this.strCursorName);
            }
            //////<<< SQL TRACE

            MaxDBRequestPacket requestPacket;
            MaxDBReplyPacket replyPacket;
            DataPart dataPart;

            // if this is one of the statements that is executed during parse instead of execution, execute it by doing a reparse
            if (this.mParseInfo.IsAlreadyExecuted)
            {
                this.baReplyMemory = null;
                if (this.dbConnection == null)
                {
                    throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_CONNECTIONNULL));
                }

                this.Reparse();
                this.iRowsAffected = 0;
                return false;
            }

            try
            {
                this.bCanceled = false;

                // check if a reparse is needed.
                if (!this.mParseInfo.IsValid)
                {
                    this.Reparse();
                }

                ////>>> SQL TRACE
                if (this.dbConnection.mLogger.TraceSQL)
                {
                    this.dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(this.mParseInfo.ParseID));
                    this.dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + this.mParseInfo.SqlCommand);
                }
                ////<<< SQL TRACE

                this.baReplyMemory = null;

                requestPacket = this.dbConnection.mComm.GetRequestPacket();
                requestPacket.InitExecute(this.mParseInfo.ParseID, this.dbConnection.AutoCommit);
                if (this.mParseInfo.IsSelect)
                {
                    requestPacket.AddCursorPart(this.strCursorName);
                }

                this.FillInputParameters(this.dbParameters, ref this.objInputArgs);

                if (this.mParseInfo.sInputCount > 0)
                {
                    dataPart = requestPacket.NewDataPart(this.mParseInfo.bVarDataInput);
                    if (this.mParseInfo.sInputCount > 0)
                    {
                        dataPart.AddRow(this.mParseInfo.sInputCount);
                        for (int i = 0; i < this.mParseInfo.mParamInfos.Length; i++)
                        {
                            if (this.mParseInfo.mParamInfos[i].IsInput && this.objInputArgs[i] != null &&
                                strInitialParamValue == this.objInputArgs[i].ToString())
                            {
                                throw new DataException(MaxDBMessages.Extract(MaxDBError.MISSINGINOUT, i + 1, "02000"));
                            }
                            else
                            {
                                this.mParseInfo.mParamInfos[i].Put(dataPart, this.objInputArgs[i]);
                            }
                        }

                        this.lstInputProcedureLongs = null;
                        if (this.mParseInfo.HasLongs)
                        {
                            if (this.mParseInfo.IsDBProc)
                            {
                                this.HandleProcedureStreamsForExecute();
                            }
                            else
                            {
                                this.HandleStreamsForExecute(dataPart, this.objInputArgs);
                            }
                        }
                    }
                    else
                    {
                        throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.OMS_UNSUPPORTED));
                    }

                    dataPart.Close();
                }

                // add a decribe order if command rturns a resultset
                if (this.mParseInfo.IsSelect && this.mParseInfo.ColumnInfo == null && this.mParseInfo.FuncCode != FunctionCode.DBProcWithResultSetExecute)
                {
                    requestPacket.InitDbsCommand("DESCRIBE ", false, false);
                    requestPacket.AddParseIdPart(this.mParseInfo.ParseID);
                }

                try
                {
                    // Recycling of parse infos and cursor names is not allowed
                    // if streams are in the command. Even sending it just behind
                    // as next packet is harmful. Same with INPUT LONG parameters of
                    // DB Procedures.
                    replyPacket = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, this, (this.lstInputProcedureLongs == null) ? GCMode.GC_ALLOWED : GCMode.GC_NONE);
                }
                catch (MaxDBException ex)
                {
                    if (ex.ErrorCode == -8 && afterParseAgain > 0)
                    {
                        ////>>> SQL TRACE
                        if (this.dbConnection.mLogger.TraceSQL)
                        {
                            this.dbConnection.mLogger.SqlTrace(dt, "PARSE AGAIN");
                        }
                        //////<<< SQL TRACE
                        ResetPutValues(this.lstInputLongs);
                        this.Reparse();
                        this.dbConnection.mComm.FreeRequestPacket(requestPacket);
                        afterParseAgain--;
                        return this.Execute(afterParseAgain, behavior);
                    }

                    // The request packet has already been recycled
                    throw;
                }

                // --- now it becomes difficult ...
                bool isQuery;
                if (this.mParseInfo.IsSelect)
                {
                    isQuery = this.ParseResult(replyPacket, this.mParseInfo.ColumnInfo, this.mParseInfo.strColumnNames, behavior);
                }
                else
                {
                    if (this.lstInputProcedureLongs != null)
                    {
                        replyPacket = this.ProcessProcedureStreams(replyPacket);
                    }

                    isQuery = this.ParseResult(replyPacket, this.mParseInfo.ColumnInfo, this.mParseInfo.strColumnNames, behavior);
                    int returnCode = replyPacket.ReturnCode;
                    if (replyPacket.ExistsPart(PartKind.Data))
                    {
                        this.baReplyMemory = replyPacket.Clone(replyPacket.PartDataPos);
                    }

                    if ((this.mParseInfo.HasLongs && !this.mParseInfo.IsDBProc) && (returnCode == 0))
                    {
                        this.HandleStreamsForPutValue(replyPacket);
                    }
                }

                this.FillOutputParameters(replyPacket, ref this.dbParameters);

                return isQuery;
            }
            catch (MaxDBTimeoutException)
            {
                if (this.dbConnection.mComm.IsInTransaction)
                {
                    throw;
                }
                else
                {
                    ResetPutValues(this.lstInputLongs);
                    this.Reparse();
                    return this.Execute(iMaxParseAgainCnt, behavior);
                }
            }
            finally
            {
                this.bCanceled = false;
            }
        }

        private void FillInputParameters(MaxDBParameterCollection cmdParams, ref object[] inputArgs)
        {
            DateTime dt = DateTime.Now;

            ////>>> SQL TRACE
            if (this.dbConnection.mLogger.TraceSQL && this.mParseInfo.sInputCount > 0)
            {
                this.dbConnection.mLogger.SqlTrace(dt, "INPUT PARAMETERS:");
                this.dbConnection.mLogger.SqlTrace(dt, "APPLICATION");
                this.dbConnection.mLogger.SqlTraceDataHeader(dt);
            }
            ////<<< SQL TRACE

            // We must add a data part if we have input parameters or even if we have output streams.
            for (int i = 0; i < this.mParseInfo.ParamInfo.Length; i++)
            {
                MaxDBParameter param = cmdParams[i];

                if (!this.FindColumnInfo(i).IsInput)
                {
                    continue;
                }

                ////>>> SQL TRACE
                if (this.dbConnection.mLogger.TraceSQL)
                {
                    StringBuilder sbOut = new StringBuilder();
                    sbOut.Append((i + 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.NumSize));
                    sbOut.Append(this.mParseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize));
                    string strValue;

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("1".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((bool)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                            sbOut.Append((this.mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                if (param.objInputValue.GetType() == typeof(char[]))
                                {
                                    strValue = new string((char[])param.objInputValue);
                                }
                                else
                                {
                                    strValue = (string)param.objInputValue;
                                }

                                sbOut.Append(strValue.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (strValue.Length > MaxDBLogger.DataSize)
                                {
                                    sbOut.Append(strValue.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                }
                                else
                                {
                                    sbOut.Append(strValue);
                                }
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            sbOut.Append((this.mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));

                            if (param.objInputValue != DBNull.Value)
                            {
                                if (param.objInputValue.GetType() == typeof(char[]))
                                {
                                    strValue = new string((char[])param.objInputValue);
                                }
                                else
                                {
                                    strValue = (string)param.objInputValue;
                                }

                                sbOut.Append((strValue.Length * Consts.UnicodeWidth).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (strValue.Length > MaxDBLogger.DataSize)
                                {
                                    sbOut.Append(strValue.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                }
                                else
                                {
                                    sbOut.Append(strValue);
                                }
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Timestamp:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append(((byte[])this.FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue)).Length.ToString(
                                    CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((DateTime)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.Time:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                if (param.objInputValue is DateTime)
                                {
                                    sbOut.Append(((byte[])this.FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue)).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((DateTime)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(((byte[])this.FindColumnInfo(i).TransTimeSpanForInput((TimeSpan)param.objInputValue)).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((TimeSpan)param.objInputValue).ToString());
                                }
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("8".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((double)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.Integer:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("4".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((int)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        case MaxDBType.SmallInt:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("2".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((short)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                        default:
                            sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                byte[] byteValue = (byte[])param.objInputValue;
                                sbOut.Append(byteValue.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (byteValue.Length > MaxDBLogger.DataSize / 2)
                                {
                                    sbOut.Append("0X" + Consts.ToHexString(byteValue, MaxDBLogger.DataSize / 2)).Append("...");
                                }
                                else
                                {
                                    sbOut.Append("0X" + Consts.ToHexString(byteValue));
                                }
                            }
                            else
                            {
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            }

                            break;
                    }

                    this.dbConnection.mLogger.SqlTrace(dt, sbOut.ToString());
                }
                ////<<< SQL TRACE

                if (param.objInputValue != DBNull.Value)
                {
                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            inputArgs[i] = this.FindColumnInfo(i).TransBooleanForInput((bool)param.objInputValue);
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            if (param.objInputValue != null && param.objInputValue.ToString().Length == 0
                                && this.dbConnection.SqlMode == SqlMode.Oracle)
                            {
                                // in ORACLE mode a null values will be inserted if the string value is equal to "" 
                                inputArgs[i] = null;
                            }
                            else
                            {
                                inputArgs[i] = param.objInputValue != null && param.objInputValue.GetType() == typeof(char[]) ? this.FindColumnInfo(i).TransStringForInput(new string((char[])param.objInputValue)) : this.FindColumnInfo(i).TransStringForInput((string)param.objInputValue);
                            }

                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Timestamp:
                            inputArgs[i] = this.FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue);
                            break;
                        case MaxDBType.Time:
                            if (param.objInputValue is DateTime)
                            {
                                inputArgs[i] = this.FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue);
                            }
                            else
                            {
                                inputArgs[i] = this.FindColumnInfo(i).TransTimeSpanForInput((TimeSpan)param.objInputValue);
                            }

                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            inputArgs[i] = this.FindColumnInfo(i).TransDoubleForInput((double)param.objInputValue);
                            break;
                        case MaxDBType.Integer:
                            inputArgs[i] = this.FindColumnInfo(i).TransInt64ForInput((int)param.objInputValue);
                            break;
                        case MaxDBType.SmallInt:
                            inputArgs[i] = this.FindColumnInfo(i).TransInt16ForInput((short)param.objInputValue);
                            break;
                        default:
                            inputArgs[i] = this.FindColumnInfo(i).TransBytesForInput((byte[])param.objInputValue);
                            break;
                    }
                }
                else
                {
                    inputArgs[i] = null;
                }
            }
        }

        private void FillOutputParameters(MaxDBReplyPacket replyPacket, ref MaxDBParameterCollection cmd_params)
        {
            DateTime dt = DateTime.Now;

            ////>>> SQL TRACE
            if (this.dbConnection.mLogger.TraceSQL && this.mParseInfo.sInputCount < this.mParseInfo.ParamInfo.Length)
            {
                this.dbConnection.mLogger.SqlTrace(dt, "OUTPUT PARAMETERS:");
                this.dbConnection.mLogger.SqlTrace(dt, "APPLICATION");
                this.dbConnection.mLogger.SqlTraceDataHeader(dt);
            }
            ////<<< SQL TRACE

            if (replyPacket.ExistsPart(PartKind.Data))
            {
                for (int i = 0; i < this.mParseInfo.ParamInfo.Length; i++)
                {
                    MaxDBParameter param = cmd_params[i];
                    if (!this.FindColumnInfo(i).IsOutput)
                    {
                        continue;
                    }

                    if (this.FindColumnInfo(i).IsDBNull(this.baReplyMemory))
                    {
                        param.objValue = DBNull.Value;
                        continue;
                    }

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            param.objValue = this.FindColumnInfo(i).GetBoolean(this.baReplyMemory);
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            param.objValue = this.FindColumnInfo(i).GetString(this, this.baReplyMemory);
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Time:
                        case MaxDBType.Timestamp:
                            param.objValue = this.FindColumnInfo(i).GetDateTime(this.baReplyMemory);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            param.objValue = this.FindColumnInfo(i).GetDouble(this.baReplyMemory);
                            break;
                        case MaxDBType.Integer:
                            param.objValue = this.FindColumnInfo(i).GetInt32(this.baReplyMemory);
                            break;
                        case MaxDBType.SmallInt:
                            param.objValue = this.FindColumnInfo(i).GetInt16(this.baReplyMemory);
                            break;
                        default:
                            param.objValue = this.FindColumnInfo(i).GetBytes(this, this.baReplyMemory);
                            break;
                    }

                    ////>>> SQL TRACE
                    if (this.dbConnection.mLogger.TraceSQL)
                    {
                        StringBuilder sbOut = new StringBuilder();
                        sbOut.Append((i + 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.NumSize));
                        sbOut.Append(this.mParseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize));

                        switch (param.dbType)
                        {
                            case MaxDBType.Boolean:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("1".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((bool)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.CharA:
                            case MaxDBType.StrA:
                            case MaxDBType.VarCharA:
                            case MaxDBType.LongA:
                            case MaxDBType.CharE:
                            case MaxDBType.StrE:
                            case MaxDBType.VarCharE:
                            case MaxDBType.LongE:
                                sbOut.Append((this.mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    string strValue = (string)param.objValue;
                                    sbOut.Append(strValue.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (strValue.Length > MaxDBLogger.DataSize)
                                    {
                                        sbOut.Append(strValue.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                    }
                                    else
                                    {
                                        sbOut.Append(strValue);
                                    }
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.Unicode:
                            case MaxDBType.StrUni:
                            case MaxDBType.VarCharUni:
                            case MaxDBType.LongUni:
                                sbOut.Append((this.mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    string strValue = (string)param.objValue;
                                    sbOut.Append((strValue.Length * Consts.UnicodeWidth).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (strValue.Length > MaxDBLogger.DataSize)
                                    {
                                        sbOut.Append(strValue.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                    }
                                    else
                                    {
                                        sbOut.Append(strValue);
                                    }
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.Date:
                            case MaxDBType.Timestamp:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append(this.FindColumnInfo(i).GetBytes(this, this.baReplyMemory).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((DateTime)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.Time:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append(this.FindColumnInfo(i).GetBytes(this, this.baReplyMemory).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (param.objValue is DateTime)
                                    {
                                        sbOut.Append(((DateTime)param.objValue).ToString(CultureInfo.InvariantCulture));
                                    }
                                    else
                                    {
                                        sbOut.Append(((TimeSpan)param.objValue).ToString());
                                    }
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.Fixed:
                            case MaxDBType.Float:
                            case MaxDBType.VFloat:
                            case MaxDBType.Number:
                            case MaxDBType.NoNumber:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("8".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((double)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.Integer:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("4".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((int)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            case MaxDBType.SmallInt:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("2".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((short)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                            default:
                                sbOut.Append(this.mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    byte[] byteValue = (byte[])param.objValue;
                                    sbOut.Append(byteValue.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (byteValue.Length > MaxDBLogger.DataSize / 2)
                                    {
                                        sbOut.Append("0X" + Consts.ToHexString(byteValue, MaxDBLogger.DataSize / 2)).Append("...");
                                    }
                                    else
                                    {
                                        sbOut.Append("0X" + Consts.ToHexString(byteValue));
                                    }
                                }
                                else
                                {
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                }

                                break;
                        }

                        this.dbConnection.mLogger.SqlTrace(dt, sbOut.ToString());
                    }

                    ////<<< SQL TRACE
                }
            }
        }

        private bool ParseResult(MaxDBReplyPacket replyPacket, DBTechTranslator[] infos, string[] columnNames, CommandBehavior behavior)
        {
            bool rowNotFound = false;
            bool dataPartFound = false;

            this.iRowsAffected = -1;
            this.bHasRowCount = false;
            bool isQuery = FunctionCode.IsQuery(replyPacket.FuncCode);

            DateTime dt = DateTime.Now;

            replyPacket.ClearPartOffset();
            for (int i = 0; i < replyPacket.PartCount; i++)
            {
                replyPacket.NextPart();
                switch (replyPacket.PartType)
                {
                    case PartKind.ColumnNames:
                        if (columnNames == null)
                        {
                            columnNames = replyPacket.ParseColumnNames();
                        }

                        break;
                    case PartKind.ShortInfo:
                        if (infos == null)
                        {
                            infos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, false, null, false);
                        }

                        break;
                    case PartKind.VardataShortInfo:
                        if (infos == null)
                        {
                            infos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, false, null, true);
                        }

                        break;
                    case PartKind.ResultCount:
                        // only if this is not a query
                        if (!isQuery)
                        {
                            this.iRowsAffected = replyPacket.ResultCount(true);
                            this.bHasRowCount = true;
                            ////>>> SQL TRACE
                            if (this.dbConnection.mLogger.TraceSQL)
                            {
                                this.dbConnection.mLogger.SqlTrace(dt, "RESULT COUNT: " + this.iRowsAffected.ToString(CultureInfo.InvariantCulture));
                            }
                            ////<<< SQL TRACE
                        }
                        break;
                    case PartKind.ResultTableName:
                        string cname = replyPacket.ReadString(replyPacket.PartDataPos, replyPacket.PartLength);
                        if (cname.Length > 0)
                        {
                            this.strCursorName = cname;
                        }

                        break;
                    case PartKind.Data:
                        dataPartFound = true;
                        break;
                    case PartKind.ErrorText:
                        if (replyPacket.ReturnCode == 100)
                        {
                            ////>>> SQL TRACE
                            if (this.dbConnection.mLogger.TraceSQL)
                            {
                                this.dbConnection.mLogger.SqlTrace(dt, "*** ROW NOT FOUND ***");
                            }
                            ////<<< SQL TRACE
                            this.iRowsAffected = -1;
                            rowNotFound = true;
                            if (!isQuery) 
                            {
                                this.iRowsAffected = 0; // for any select update count must be -1
                            }
                        }
                        break;
                    case PartKind.ParsidOfSelect:
                        // ignore
                        break;
                    default:
                        break;
                }
            }

            if (isQuery)
            {
                if (replyPacket.NextSegment() != -1 && replyPacket.FuncCode == FunctionCode.Describe)
                {
                    bool newSFI = true;
                    replyPacket.ClearPartOffset();
                    for (int i = 0; i < replyPacket.PartCount; i++)
                    {
                        replyPacket.NextPart();
                        switch (replyPacket.PartType)
                        {
                            case PartKind.ColumnNames:
                                if (columnNames == null)
                                {
                                    columnNames = replyPacket.ParseColumnNames();
                                }

                                break;
                            case PartKind.ShortInfo:
                                if (infos == null)
                                {
                                    infos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, false, null, false);
                                }

                                break;
                            case PartKind.VardataShortInfo:
                                if (infos == null)
                                {
                                    infos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, false, null, true);
                                }

                                break;
                            case PartKind.ErrorText:
                                newSFI = false;
                                break;
                            default:
                                break;
                        }
                    }

                    if (newSFI)
                    {
                        this.mParseInfo.SetMetaData(infos, columnNames);
                    }
                }

                this.mCurrentDataReader = this.CreateDataReader(infos, columnNames, rowNotFound, behavior, dataPartFound ? replyPacket : null);
            }

            return isQuery;
        }

        private MaxDBDataReader CreateDataReader(DBTechTranslator[] infos, string[] columnNames, bool rowNotFound, CommandBehavior behavior, MaxDBReplyPacket reply)
        {
            MaxDBDataReader reader;

            try
            {
                reader = new MaxDBDataReader(this.dbConnection, new FetchInfo(this.dbConnection, this.strCursorName, infos, columnNames), this, ((behavior & CommandBehavior.SingleRow) != 0) ? 1 : 0, reply);
            }
            catch (MaxDBException ex)
            {
                if (ex.ErrorCode == -4000)
                {
                    reader = new MaxDBDataReader();
                }
                else
                {
                    throw;
                }
            }

            if (rowNotFound)
            {
                reader.Empty = true;
            }

            return reader;
        }

        // Parses the SQL command, or looks the parsed command up in the cache.
        private MaxDBParseInfo DoParse(string sql, bool parseAgain)
        {
            if (sql == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLSTATEMENT_NULL), "42000");
            }

            if (this.mCmdType == CommandType.TableDirect)
            {
                throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.TABLEDIRECT_UNSUPPORTED));
            }

            MaxDBReplyPacket replyPacket;
            MaxDBParseInfo result = null;
            ParseInfoCache cache = this.dbConnection.mComm.mParseCache;
            string[] columnNames = null;

            if (parseAgain)
            {
                result = this.mParseInfo;
                result.MassParseID = null;
            }
            else if (cache != null)
            {
                result = cache.FindParseInfo(sql);
                ////>>> SQL TRACE
                if (this.dbConnection.mLogger.TraceSQL && result != null)
                {
                    this.dbConnection.mLogger.SqlTrace(DateTime.Now, "CACHED PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
                }
                ////<<< SQL TRACE
            }

            if (result == null || parseAgain)
            {
                ////>>> SQL TRACE
                DateTime dt = DateTime.Now;
                if (this.dbConnection.mLogger.TraceSQL)
                {
                    this.dbConnection.mLogger.SqlTrace(dt, "::PARSE " + this.strCursorName);
                    this.dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + sql);
                }
                ////<<< SQL TRACE

                try
                {
                    this.bSetWithInfo = true;
                    replyPacket = this.SendSqlCommand(sql, parseAgain);
                }
                catch (MaxDBTimeoutException)
                {
                    replyPacket = this.SendSqlCommand(sql, parseAgain);
                }

                if (!parseAgain)
                {
                    result = new MaxDBParseInfo(this.dbConnection, sql, replyPacket.FuncCode);
                }

                replyPacket.ClearPartOffset();
                DBTechTranslator[] shortInfos = null;
                for (int i = 0; i < replyPacket.PartCount; i++)
                {
                    replyPacket.NextPart();
                    switch (replyPacket.PartType)
                    {
                        case PartKind.ParseId:
                            int parseidPos = replyPacket.PartDataPos;
                            result.SetParseIDAndSession(replyPacket.ReadBytes(parseidPos, 12), replyPacket.Clone(replyPacket.Offset, false).ReadInt32(parseidPos)); // session id is always BigEndian number
                            ////>>> SQL TRACE
                            if (this.dbConnection.mLogger.TraceSQL)
                            {
                                this.dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
                            }
                            ////<<< SQL TRACE
                            break;
                        case PartKind.ShortInfo:
                            shortInfos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, result.IsDBProc, result.mProcParamInfos, false);
                            break;
                        case PartKind.VardataShortInfo:
                            result.bVarDataInput = true;
                            shortInfos = replyPacket.ParseShortFields(this.dbConnection.mComm.mConnStrBuilder.SpaceOption, result.IsDBProc, result.mProcParamInfos, true);
                            break;
                        case PartKind.ResultTableName:
                            result.IsSelect = true;
                            int cursorLength = replyPacket.PartLength;

                            if (cursorLength > 0)
                            {
                                this.strCursorName = replyPacket.ReadString(replyPacket.PartDataPos, cursorLength).TrimEnd('\0');
                                if (this.strCursorName.Length == 0)
                                {
                                    result.IsSelect = false;
                                }
                            }
                            break;
                        case PartKind.TableName:
                            result.UpdatedTableName = replyPacket.ReadString(replyPacket.PartDataPos, replyPacket.PartLength);
                            break;
                        case PartKind.ColumnNames:
                            columnNames = replyPacket.ParseColumnNames();
                            break;
                        default:
                            break;
                    }
                }
                
                result.SetShortInfosAndColumnNames(shortInfos, columnNames);
                
                if (cache != null && !parseAgain)
                {
                    cache.AddParseInfo(result);
                }
            }
            this.objInputArgs = new object[result.mParamInfos.Length];
            this.ClearParameters();

            ////>>> SQL TRACE
            if (this.dbConnection.mLogger.TraceSQL)
            {
                this.dbConnection.mLogger.SqlTraceParseInfo(DateTime.Now, result);
            }
            ////<<< SQL TRACE

            result.dbConnection = this.dbConnection;
            return result;
        }

        internal void ParseMassCmd(bool parsegain)
        {
            MaxDBRequestPacket requestPacket = this.dbConnection.mComm.GetRequestPacket();
            requestPacket.InitParseCommand(this.mParseInfo.SqlCommand, true, parsegain);
            requestPacket.SetMassCommand();
            MaxDBReplyPacket replyPacket = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, this, GCMode.GC_ALLOWED);
            if (replyPacket.ExistsPart(PartKind.ParseId))
            {
                this.mParseInfo.MassParseID = replyPacket.ReadBytes(replyPacket.PartDataPos, 12);
            }
        }

        private void ClearParameters()
        {
            for (int i = 0; i < this.objInputArgs.Length; ++i)
            {
                this.objInputArgs[i] = strInitialParamValue;
            }
        }

        private static void ResetPutValues(IList inpLongs)
        {
            if (inpLongs != null)
            {
                foreach (PutValue putval in inpLongs)
                {
                    putval.Reset();
                }
            }
        }

        private void HandleStreamsForExecute(DataPart dataPart, object[] args)
        {
            // get all putval objects
            this.lstInputLongs = new List<PutValue>();
            for (int i = 0; i < this.mParseInfo.mParamInfos.Length; i++)
            {
                object inarg = args[i];
                if (inarg == null)
                {
                    continue;
                }

                try
                {
                    this.lstInputLongs.Add((PutValue)inarg);
                }
                catch (InvalidCastException)
                {
                    // not a long for input, ignore
                }
            }

            if (this.lstInputLongs.Count > 1)
            {
                this.lstInputLongs.Sort(mPutValueComparator);
            }

            // write data (and patch descriptor)
            for (short i = 0; i < this.lstInputLongs.Count; i++)
            {
                PutValue putval = (PutValue)this.lstInputLongs[i];
                if (putval.AtEnd)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_ISATEND));
                }

                putval.TransferStream(dataPart, i);
            }
        }

        private void HandleProcedureStreamsForExecute()
        {
            this.lstInputProcedureLongs = new List<AbstractProcedurePutValue>();
            for (int i = 0; i < this.mParseInfo.mParamInfos.Length; ++i)
            {
                object arg = this.objInputArgs[i];
                if (arg == null)
                {
                    continue;
                }

                try
                {
                    AbstractProcedurePutValue pv = (AbstractProcedurePutValue)arg;
                    this.lstInputProcedureLongs.Add(pv);
                    pv.UpdateIndex(this.lstInputProcedureLongs.Count - 1);
                }
                catch (InvalidCastException)
                {
                    continue;
                }
            }
        }

        private MaxDBReplyPacket ProcessProcedureStreams(MaxDBReplyPacket packet)
        {
            if (packet.ExistsPart(PartKind.AbapIStream))
            {
                throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.OMS_UNSUPPORTED));
            }

            foreach (AbstractProcedurePutValue pv in this.lstInputProcedureLongs)
            {
                pv.CloseStream();
            }

            return packet;
        }

        private void HandleStreamsForPutValue(MaxDBReplyPacket replyPacket)
        {
            if (this.lstInputLongs.Count == 0)
            {
                return;
            }

            PutValue lastStream = (PutValue)this.lstInputLongs[this.lstInputLongs.Count - 1];
            MaxDBRequestPacket requestPacket;
            DataPart dataPart;
            PutValue putval;
            short firstOpenStream = 0;
            int count = this.lstInputLongs.Count;
            bool requiresTrailingPacket = false;

            while (!lastStream.AtEnd)
            {
                this.GetChangedPutValueDescriptors(replyPacket);
                requestPacket = this.dbConnection.mComm.GetRequestPacket();
                dataPart = requestPacket.InitPutValue(this.dbConnection.AutoCommit);

                // get all descriptors and putvals
                for (short i = firstOpenStream; (i < count) && dataPart.HasRoomFor(LongDesc.Size + 1); i++)
                {
                    putval = (PutValue)this.lstInputLongs[i];
                    if (putval.AtEnd)
                        firstOpenStream++;
                    else
                    {
                        int descriptorPos = dataPart.Extent;
                        putval.PutDescriptor(dataPart, descriptorPos);
                        dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
                        if (this.bCanceled)
                        {
                            putval.MarkErrorStream();
                            firstOpenStream++;
                        }
                        else
                        {
                            putval.TransferStream(dataPart, i);
                            if (putval.AtEnd)
                            {
                                firstOpenStream++;
                            }
                        }
                    }
                }
                if (lastStream.AtEnd && !this.bCanceled)
                {
                    try
                    {
                        lastStream.MarkAsLast(dataPart);
                    }
                    catch (IndexOutOfRangeException)
                    {
                        // no place for end of LONGs marker
                        requiresTrailingPacket = true;
                    }
                }

                // at end: patch last descriptor
                dataPart.Close();

                // execute and get descriptors
                replyPacket = this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, this, GCMode.GC_DELAYED);

                // write trailing end of LONGs marker
                if (requiresTrailingPacket && !this.bCanceled)
                {
                    requestPacket = this.dbConnection.mComm.GetRequestPacket();
                    dataPart = requestPacket.InitPutValue(this.dbConnection.AutoCommit);
                    lastStream.MarkAsLast(dataPart);
                    dataPart.Close();
                    this.dbConnection.mComm.Execute(this.dbConnection.mConnArgs, requestPacket, this, GCMode.GC_DELAYED);
                }
            }

            if (this.bCanceled)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STATEMENT_CANCELLED), "42000", -102);
            }
        }

        private void GetChangedPutValueDescriptors(MaxDBReplyPacket replyPacket)
        {
            byte[][] descriptorArray = replyPacket.ParseLongDescriptors();
            ByteArray descriptorPointer;
            PutValue putval;
            if (!replyPacket.ExistsPart(PartKind.LongData))
            {
                return;
            }

            for (int i = 0; i < descriptorArray.Length; ++i)
            {
                byte[] descriptor = descriptorArray[i];
                descriptorPointer = new ByteArray(descriptor);
                int valIndex = descriptorPointer.ReadInt16(LongDesc.ValInd);
                putval = (PutValue)this.lstInputLongs[valIndex];
                putval.SetDescriptor(descriptor);
            }
        }

        private DBTechTranslator FindColumnInfo(int colIndex)
        {
            try
            {
                return this.mParseInfo.mParamInfos[colIndex];
            }
            catch (IndexOutOfRangeException)
            {
                throw new DataException(MaxDBMessages.Extract(MaxDBError.COLINDEX_NOTFOUND, colIndex, this));
            }
        }

        MaxDBConnection ISqlParameterController.Connection
        {
            get
            {
                return this.dbConnection;
            }
        }

        ByteArray ISqlParameterController.ReplyData
        {
            get
            {
                return this.baReplyMemory;
            }
        }

        #endregion

        #region ICloneable Members

        object ICloneable.Clone()
        {
            MaxDBCommand clone = new MaxDBCommand(this.strCmdText, this.dbConnection, this.dbTransaction);
            foreach (MaxDBParameter p in this.Parameters)
            {
                clone.Parameters.Add((MaxDBParameter)(p as ICloneable).Clone());
            }

            return clone;
        }

        #endregion

        #region IDbCommand Members

        /// <summary>
        /// Attempts to cancel the execution of a <b>MaxDBCommand</b>.
        /// </summary>
        public override void Cancel()
        {
            this.AssertOpen();
            this.bCanceled = true;
            this.dbConnection.mComm.Cancel(this);
        }

        /// <summary>
        /// Gets or sets the SQL statement to execute at the data source.
        /// </summary>
        public override string CommandText
        {
            get
            {
                return this.strCmdText;
            }

            set
            {
                this.strCmdText = value;
            }
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public override int CommandTimeout
        {
            get
            {
                return 0;
            }

            set
            {
                if (value != 0) throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how the <see cref="CommandText"/> property is to be interpreted.
        /// </summary>
        public override CommandType CommandType
        {
            get
            {
                return this.mCmdType;
            }

            set
            {
                this.mCmdType = value;
            }
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get
            {
                return this.Connection;
            }

            set
            {
                this.Connection = (MaxDBConnection)value;
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="MaxDBConnection"/> used by this instance of the <b>MaxDBCommand</b>.
        /// </summary>
        public new MaxDBConnection Connection
        {
            get
            {
                return this.dbConnection;
            }

            set
            {
                if (this.dbConnection != value)
                {
                    this.Transaction = null;
                }

                this.dbConnection = value;
            }
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        protected override DbParameter CreateDbParameter()
        {
            return this.CreateParameter();
        }

        /// <summary>
        /// Creates a new instance of a <see cref="MaxDBParameter"/> object.
        /// </summary>
        /// <returns>A new <see cref="MaxDBParameter"/> object.</returns>
        public new MaxDBParameter CreateParameter()
        {
            return new MaxDBParameter();
        }

        /// <summary>
        /// Executes a SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>Number of rows affected.</returns>
        /// <remarks>
        /// You can use ExecuteNonQuery to perform any type of database operation, 
        /// however any resultsets returned will not be available.  Any output parameters
        /// used in calling a stored procedure will be populated with data and can be
        /// retrieved after execution is complete.
        /// For UPDATE, INSERT, and DELETE statements, the return value is the number
        /// of rows affected by the command.  For all other types of statements, the return
        /// value is -1.
        /// </remarks>
        public override int ExecuteNonQuery()
        {
            // Execute the command.

            // There must be a valid and open connection.
            this.AssertOpen();

            this.Prepare();

            if (this.mParseInfo != null && this.mParseInfo.IsSelect)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLSTATEMENT_RESULTSET));
            }

            this.Execute(CommandBehavior.Default);
            return this.bHasRowCount ? this.iRowsAffected : -1;
        }

        /// <summary>
        /// Sends the <see cref="CommandText"/> to the <see cref="MaxDBConnection"/> and builds a <see cref="MaxDBDataReader"/>.
        /// </summary>
        /// <returns>A new <see cref="MaxDBDataReader"/> object.</returns>
        /// <remarks>
        /// <para>
        /// When the <see cref="CommandType"/> property is set to <B>StoredProcedure</B>, 
        ///    the <see cref="CommandText"/> property should be set to the name of the stored 
        ///    procedure. The command executes this stored procedure when you call 
        ///    <B>ExecuteReader</B>.
        /// </para>
        /// <para>
        ///    While the <see cref="MaxDBDataReader"/> is in use, the associated 
        ///    <see cref="MaxDBConnection"/> is busy serving the <see cref="MaxDBDataReader"/>. 
        ///    While in this state, no other operations can be performed on the 
        ///    <see cref="MaxDBConnection"/> other than closing it. This is the case until the 
        ///    <see cref="MaxDBDataReader.Close"/> method of the <see cref="MaxDBDataReader"/> is called.
        /// </para>
        /// </remarks>
        public new MaxDBDataReader ExecuteReader()
        {
            return this.ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="behavior">A <see cref="CommandBehavior"/> value.</param>
        /// <returns>A <see cref="DbDataReader"/> object.</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return this.ExecuteReader(behavior);
        }

        /// <summary>
        /// Sends the <see cref="CommandText"/> to the <see cref="MaxDBConnection"/> and builds a <see cref="MaxDBDataReader"/>.
        /// </summary>
        /// <param name="behavior">A <see cref="CommandBehavior"/> value.</param>
        /// <returns>A <see cref="MaxDBDataReader"/> object.</returns>
        public new MaxDBDataReader ExecuteReader(CommandBehavior behavior)
        {
            // Execute the command.

            // There must be a valid and open connection.
            this.AssertOpen();

            this.Prepare();

            this.Execute(behavior);

            if (this.mParseInfo.IsSelect)
            {
                if (this.mCurrentDataReader == null)
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLCOMMAND_NORESULTSET));
                }

                this.mCurrentDataReader.bCloseConn = (behavior & CommandBehavior.CloseConnection) != 0;
                this.mCurrentDataReader.bSchemaOnly = (behavior & CommandBehavior.SchemaOnly) != 0;
                return this.mCurrentDataReader;
            }

            return new MaxDBDataReader(this);
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the 
        /// result set returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns>The first column of the first row in the result set, or a null reference if the 
        /// result set is empty
        /// </returns>
        /// <remarks>
        /// <para>Use the <B>ExecuteScalar</B> method to retrieve a single value (for example, 
        /// an aggregate value) from a database. This requires less code than using the 
        /// <see cref="ExecuteReader()"/> method, and then performing the operations necessary 
        /// to generate the single value using the data returned by a <see cref="MaxDBDataReader"/>
        /// </para>
        /// </remarks>
        public override object ExecuteScalar()
        {
            IDataReader result = this.ExecuteReader(CommandBehavior.SingleResult);
            if (result.FieldCount > 0 && result.Read())
            {
                return result.GetValue(0);
            }

            return null;
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return this.Parameters;
            }
        }

        /// <summary>
        /// Gets the <see cref="MaxDBParameterCollection"/>.
        /// </summary>
        public new MaxDBParameterCollection Parameters
        {
            get
            {
                return this.dbParameters;
            }
        }

        /// <summary>
        /// This property is intended for internal use and can not to be called directly from your code.
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get
            {
                return this.Transaction;
            }

            set
            {
                this.Transaction = (MaxDBTransaction)value;
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="MaxDBTransaction"/> within which the <b>MaxDBCommand</b> executes.
        /// Consider additional steps to ensure that the transaction
        /// is compatible with the connection, because the two are usually linked.
        /// </summary>
        public new MaxDBTransaction Transaction
        {
            get
            {
                return this.dbTransaction;
            }

            set
            {
                this.dbTransaction = value;
            }
        }

        /// <summary>
        /// Gets or sets how command results are applied to the <see cref="DataRow"/>
        /// when used by the <see cref="System.Data.Common.DbDataAdapter.Update(System.Data.DataSet)"/> method 
        /// of the <see cref="System.Data.Common.DbDataAdapter"/>.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                return this.updatedRowSource;
            }

            set
            {
                this.updatedRowSource = value;
            }
        }

        /// <summary>
        /// Creates a prepared version of the command on an instance of MaxDB Server.
        /// </summary>
        public override void Prepare()
        {
            if (this.mCmdType == CommandType.TableDirect)
            {
                throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.TABLEDIRECT_UNSUPPORTED));
            }

            this.mParseInfo = this.DoParse(this.strCmdText, false);
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="disposing">The disposing flag.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                this.baReplyMemory = null;
                if (this.dbConnection != null && (this.mParseInfo != null && !this.mParseInfo.IsCached))
                {
                    if (this.mCurrentDataReader != null)
                    {
                        ((IDisposable)this.mCurrentDataReader).Dispose();
                    }

                    this.dbConnection.mComm.DropParseID(this.mParseInfo.ParseID);
                    this.mParseInfo.SetParseIDAndSession(null, -1);
                    this.dbConnection.mComm.DropParseID(this.mParseInfo.MassParseID);
                    this.mParseInfo.MassParseID = null;
                    this.dbConnection = null;
                }
            }
        }

        #endregion
    }
}
