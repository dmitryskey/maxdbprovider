//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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

using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif // NET20
using System.ComponentModel;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Reflection;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Permissions;

namespace MaxDB.Data
{
	/// <summary>
	/// Represents a SQL statement to execute against a MaxDB database. This class cannot be inherited.
	/// </summary>
	sealed public class MaxDBCommand :
#if NET20
        DbCommand
#else // NET20
        IDbCommand, IDisposable
#endif // NET20
        , ICloneable
#if SAFE
        , ISqlParameterController
#endif // SAFE
    {
		private MaxDBConnection dbConnection;
		private MaxDBTransaction dbTransaction;
		private string strCmdText;
		private UpdateRowSource updatedRowSource = UpdateRowSource.None;
		private MaxDBParameterCollection dbParameters = new MaxDBParameterCollection();
		private CommandType mCmdType = CommandType.Text;
		internal int iRowsAffected = -1;
        private bool bDesignTimeVisible;

#if SAFE
		#region "Native implementation parameters"

#if NET20
        private List<AbstractProcedurePutValue> lstInputProcedureLongs;
#else
		private ArrayList lstInputProcedureLongs;
#endif // NET20
        private bool bSetWithInfo;
		private bool bHasRowCount;
		private const int iMaxParseAgainCnt = 10;
#if NET20
        private List<PutValue> lstInputLongs;
#else
		private ArrayList lstInputLongs;
#endif // NET20
		internal MaxDBParseInfo mParseInfo;
		private MaxDBDataReader mCurrentDataReader;
		private object[] objInputArgs;
		private string strCursorName;
		private bool bCanceled;
		private ByteArray baReplyMemory;
		private const string strInitialParamValue = "initParam";
        private static PutValueComparator mPutValueComparator = new PutValueComparator();

        #endregion
#else
        #region "SQLDBC Wrapper parameters"

		internal IntPtr mStmt = IntPtr.Zero;
		
        #endregion
#endif // SAFE

        /// <summary>
		/// Implement the default constructor here.
		/// </summary>
		public MaxDBCommand()
		{
		}

		// Implement other constructors here.
		public MaxDBCommand(string cmdText)
		{
			strCmdText = cmdText;
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection) : this(cmdText)
		{
			dbConnection  = connection;
			
#if !SAFE
			RefreshStmtHandler();
#endif
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction transaction) : this(cmdText, connection)
		{
			dbTransaction      = transaction;
#if !SAFE
			RefreshStmtHandler();
#endif
		}

		internal void AssertOpen()
		{
			if (dbConnection == null
#if SAFE
				|| dbConnection.mComm == null
#endif // SAFE
				)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
		}

#if NET20
        public override bool DesignTimeVisible
#else
        public bool DesignTimeVisible
#endif // NET20
        {
            get
            {
                return bDesignTimeVisible;
            }
            set
            {
                bDesignTimeVisible = value;
            }
        }

#if SAFE && NET20
        internal int[] ExecuteBatch(MaxDBParameterCollection[] batchParams)
        {
            AssertOpen();
            strCursorName = dbConnection.NextCursorName;
			
			Prepare();

            if (mParseInfo != null && mParseInfo.IsSelect)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHRESULTSET, new int[0]));
            if (mParseInfo != null
                && (mParseInfo.FuncCode == FunctionCode.DBProcExecute || mParseInfo.FuncCode == FunctionCode.DBProcWithResultSetExecute))
                foreach (DBTechTranslator transl in mParseInfo.mParamInfos)
                    if (transl.IsOutput)
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHPROCOUT, new int[0]));

            if (batchParams == null)
                return new int[0];

            //>>> SQL TRACE
            DateTime dt = DateTime.Now;

            if (dbConnection.mLogger.TraceSQL)
            {
                dbConnection.mLogger.SqlTrace(dt, "::EXECUTE BATCH " + strCursorName);
                dbConnection.mLogger.SqlTrace(dt, "BATCH SIZE " + batchParams.Length);
            }
            //<<< SQL TRACE

            List<PutValue> streamVec = null;
            bool inTrans = dbConnection.IsInTransaction;

            try
            {
                bCanceled = false;
                int count = batchParams.Length;
                MaxDBRequestPacket requestPacket;
                MaxDBReplyPacket replyPacket = null;
                int inputCursor = 0;
                bool noError = true;
                int recordSize = 0;
                int insertCountPartSize = MaxDBRequestPacket.ResultCountPartSize;
                int executeCount = -1;
                int[] result = new int[count];
                iRowsAffected = -1;

                //>>> SQL TRACE
                if (dbConnection.mLogger.TraceSQL)
                {
                    dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(mParseInfo.ParseID));
                    dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + mParseInfo.SqlCommand);
                }
                //<<< SQL TRACE

                if (mParseInfo.MassParseID == null)
                    ParseMassCmd(false);

                if (mParseInfo.mParamInfos.Length > 0)
                {
                    int currentFieldEnd;

                    foreach (DBTechTranslator currentInfo in mParseInfo.mParamInfos)
                    {
                        if (currentInfo.IsInput)
                        {
                            currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
                            recordSize = Math.Max(recordSize, currentFieldEnd);
                        }
                    }
                }

                while ((inputCursor < count) && noError)
                {
                    streamVec = null;
                    int firstRecordNo = inputCursor;
                    requestPacket = dbConnection.GetRequestPacket();
                    requestPacket.InitExecute(mParseInfo.MassParseID, dbConnection.AutoCommit);
                    if (executeCount == -1)
                        requestPacket.AddUndefinedResultCount();
                    else
                        requestPacket.AddResultCount(executeCount);
                    requestPacket.AddCursorPart(strCursorName);
                    DataPart dataPart;
                    if (mParseInfo.mParamInfos.Length > 0)
                    {
                        dataPart = requestPacket.NewDataPart(mParseInfo.bVarDataInput);
                        if (executeCount == -1)
                            dataPart.SetFirstPart();

                        do
                        {
                            object[] row = new object[batchParams[inputCursor].Count];
                            FillInputParameters(batchParams[inputCursor], ref row);
                            dataPart.AddRow(mParseInfo.sInputCount);
                            for (int i = 0; i < mParseInfo.mParamInfos.Length; i++)
                            {
                                // check whether the parameter was set by application or throw an exception
                                DBTechTranslator transl = mParseInfo.mParamInfos[i];

                                if (transl.IsInput && row[i] != null && strInitialParamValue == row[i].ToString())
                                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BATCHMISSINGIN,
                                                                         (inputCursor + 1).ToString(CultureInfo.InvariantCulture),
                                                                         (i + 1).ToString(CultureInfo.InvariantCulture)),
                                                                         "0200");

                                transl.Put(dataPart, row[i]);
                            }
                            if (mParseInfo.HasLongs)
                            {
                                HandleStreamsForExecute(dataPart, row);
                                if (streamVec == null)
									streamVec = new List<PutValue>(lstInputLongs);

                            }
                            dataPart.MoveRecordBase();
                            inputCursor++;
                        } while ((inputCursor < count) && dataPart.HasRoomFor(recordSize, insertCountPartSize) && mParseInfo.IsMassCmd);

                        if (inputCursor == count)
                            dataPart.SetLastPart();
                        dataPart.CloseArrayPart((short)(inputCursor - firstRecordNo));
                    }
                    else
                        inputCursor++; //commands without parameters

                    try
                    {
                        replyPacket = dbConnection.Execute(requestPacket, this, GCMode.GC_DELAYED);
                    }
                    catch (MaxDBException ex)
                    {
                        if (ex.ErrorCode == -8)
                        {
                            ResetPutValues(streamVec);
                            ParseMassCmd(true);
                            inputCursor = firstRecordNo;
                            continue;   // redo this packet
                        }
                        else
                        {
                            // An autocommit session does roll back automatically all what was
                            // in this package. Thus, in case of an error we must not
                            // add the current count from the packet.
                            if (!dbConnection.AutoCommit)
                            {
                                if (iRowsAffected > 0)
                                    iRowsAffected += ex.ErrorPos - 1;
                                else
                                    iRowsAffected = ex.ErrorPos - 1;
                            }
                            throw;
                        }
                    }
                    executeCount = replyPacket.ResultCount(false);
                    if (mParseInfo.HasLongs)
                        HandleStreamsForPutValue(replyPacket);
                    if (mParseInfo.IsMassCmd && executeCount != -1)
                    {
                        if (iRowsAffected > 0)
                            iRowsAffected += executeCount;
                        else
                            iRowsAffected = executeCount;
                    }
                    else
                        iRowsAffected = executeCount;
                }
                return result;

            }
            catch(MaxDBTimeoutException)
            {
                if (inTrans)
                    throw;
                else
                {
                    ResetPutValues(streamVec);
                    return ExecuteBatch(batchParams);
                }
            }
            finally
            {
                bCanceled = false;
            }
        }
#endif // SAFE && NET20

#if !SAFE && NET20
        internal unsafe int[] ExecuteBatch(MaxDBParameterCollection[] batchParams)
        {
            Prepare();
                
            BindAndExecute(mStmt, batchParams);

            return new int[0];
        }
#endif // !SAFE && NET20


#if SAFE
		#region "Methods to support native protocol"

		private MaxDBReplyPacket SendCommand(MaxDBRequestPacket requestPacket, string sqlCmd, int gcFlags, bool parseAgain)
		{
			MaxDBReplyPacket replyPacket;
			requestPacket.InitParseCommand(sqlCmd, true, parseAgain);
			if (bSetWithInfo)
				requestPacket.SetWithInfo();
			replyPacket = dbConnection.Execute(requestPacket, false, true, this, gcFlags);
			return replyPacket;
		}

		private MaxDBReplyPacket SendSqlCommand(string sql, bool parseAgain)
		{
			MaxDBReplyPacket replyPacket;

			try
			{
				replyPacket = SendCommand(dbConnection.GetRequestPacket(), sql, GCMode.GC_ALLOWED, parseAgain);
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
			object[] tmpArgs = objInputArgs;
			mParseInfo = DoParse(mParseInfo.SqlCommand, true);
			objInputArgs = tmpArgs;
		}

		internal bool Execute(CommandBehavior behavior)
		{
			AssertOpen();
			strCursorName = dbConnection.NextCursorName;

			return Execute(iMaxParseAgainCnt, behavior);
		}

		internal bool Execute(int afterParseAgain, CommandBehavior behavior)
		{
			if (dbConnection == null) 
				throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_CONNECTIONNULL));

			//>>> SQL TRACE
			DateTime dt = DateTime.Now;

			if (dbConnection.mLogger.TraceSQL)
				dbConnection.mLogger.SqlTrace(dt, "::EXECUTE " + strCursorName);
			//<<< SQL TRACE

			MaxDBRequestPacket requestPacket = null;
			MaxDBReplyPacket replyPacket;
			bool isQuery;
			DataPart dataPart;

			// if this is one of the statements that is executed during parse instead of execution, execute it by doing a reparse
			if(mParseInfo.IsAlreadyExecuted) 
			{
				baReplyMemory = null;
				if (dbConnection == null) 
					throw new DataException(MaxDBMessages.Extract(MaxDBError.INTERNAL_CONNECTIONNULL));
				Reparse();
				iRowsAffected = 0;
				return false;
			}

			try 
			{
				bCanceled = false;
				// check if a reparse is needed.
				if (!mParseInfo.IsValid)
					Reparse();

				//>>> SQL TRACE
				if (dbConnection.mLogger.TraceSQL)
				{
					dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(mParseInfo.ParseID));
					dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " +mParseInfo.SqlCommand);
				}
				//<<< SQL TRACE
				
				baReplyMemory = null;

				requestPacket = dbConnection.GetRequestPacket();
				requestPacket.InitExecute(mParseInfo.ParseID, dbConnection.AutoCommit);
				if (mParseInfo.IsSelect) 
					requestPacket.AddCursorPart(strCursorName);

                FillInputParameters(dbParameters, ref objInputArgs);

				if (mParseInfo.sInputCount > 0) 
				{
					dataPart = requestPacket.NewDataPart(mParseInfo.bVarDataInput);
					if (mParseInfo.sInputCount > 0) 
					{
						dataPart.AddRow(mParseInfo.sInputCount);
						for (int i = 0; i < mParseInfo.mParamInfos.Length; i++) 
						{
							if (mParseInfo.mParamInfos[i].IsInput && objInputArgs[i] != null &&
								strInitialParamValue == objInputArgs[i].ToString())
									throw new DataException(MaxDBMessages.Extract(MaxDBError.MISSINGINOUT, i + 1, "02000"));
							else 
								mParseInfo.mParamInfos[i].Put(dataPart, objInputArgs[i]);
						}
						lstInputProcedureLongs = null;
						if (mParseInfo.HasLongs) 
						{
							if (mParseInfo.IsDBProc) 
								HandleProcedureStreamsForExecute();
							else 
								HandleStreamsForExecute(dataPart, objInputArgs);
						}
					} 
					else 
						throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.OMS_UNSUPPORTED));
					dataPart.Close();
				}
				// add a decribe order if command rturns a resultset
				if (mParseInfo.IsSelect && mParseInfo.ColumnInfo == null && mParseInfo.FuncCode != FunctionCode.DBProcWithResultSetExecute)
				{
					requestPacket.InitDbsCommand("DESCRIBE ", false, false);
					requestPacket.AddParseIdPart(mParseInfo.ParseID);
				}

				try 
				{
					replyPacket = dbConnection.Execute(requestPacket, this,                                              
						(lstInputProcedureLongs == null) ? GCMode.GC_ALLOWED : GCMode.GC_NONE);
					// Recycling of parse infos and cursor names is not allowed
					// if streams are in the command. Even sending it just behind
					// as next packet is harmful. Same with INPUT LONG parameters of
					// DB Procedures.
				}
				catch(MaxDBException ex) 
				{
					if (ex.ErrorCode == -8 && afterParseAgain > 0) 
					{
						//>>> SQL TRACE
						if (dbConnection.mLogger.TraceSQL)
							dbConnection.mLogger.SqlTrace(dt, "PARSE AGAIN");
						//<<< SQL TRACE
						ResetPutValues(lstInputLongs);
						Reparse();
						dbConnection.FreeRequestPacket(requestPacket);
						afterParseAgain--;
						return Execute(afterParseAgain, behavior);
					}

					// The request packet has already been recycled
					throw;
				}

				// --- now it becomes difficult ...
				if (mParseInfo.IsSelect) 
					isQuery = ParseResult(replyPacket, mParseInfo.ColumnInfo, mParseInfo.strColumnNames, behavior);
				else 
				{
					if(lstInputProcedureLongs != null) 
						replyPacket = ProcessProcedureStreams(replyPacket);
					isQuery = ParseResult(replyPacket, mParseInfo.ColumnInfo, mParseInfo.strColumnNames, behavior);
					int returnCode = replyPacket.ReturnCode;
					if (replyPacket.ExistsPart(PartKind.Data)) 
						baReplyMemory = replyPacket.Clone(replyPacket.PartDataPos);
					if ((mParseInfo.HasLongs && !mParseInfo.IsDBProc) && (returnCode == 0)) 
						HandleStreamsForPutValue(replyPacket);
				}

                FillOutputParameters(replyPacket, ref dbParameters);
 
				return isQuery;
			}
			catch (MaxDBTimeoutException) 
			{
				if (dbConnection.IsInTransaction) 
					throw;
				else 
				{
					ResetPutValues(lstInputLongs);
					Reparse();
					return Execute(iMaxParseAgainCnt, behavior);
				}
			} 
			finally
			{
				bCanceled = false;
			}
		}

        private void FillInputParameters(MaxDBParameterCollection cmd_params, ref object[] inputArgs)
        {
            DateTime dt = DateTime.Now;

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL && mParseInfo.sInputCount > 0)
            {
                dbConnection.mLogger.SqlTrace(dt, "INPUT PARAMETERS:");
                dbConnection.mLogger.SqlTrace(dt, "APPLICATION");
                dbConnection.mLogger.SqlTraceDataHeader(dt);
            }
            //<<< SQL TRACE

            // We must add a data part if we have input parameters or even if we have output streams.
            for (int i = 0; i < mParseInfo.ParamInfo.Length; i++)
            {
                MaxDBParameter param = cmd_params[i];

                if (!FindColumnInfo(i).IsInput)
                    continue;

                //>>> SQL TRACE
                if (dbConnection.mLogger.TraceSQL)
                {
                    StringBuilder sbOut = new StringBuilder();
                    sbOut.Append((i + 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.NumSize));
                    sbOut.Append(mParseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize));

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("1".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((bool)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                            sbOut.Append((mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                string str_value;
                                if (param.objInputValue.GetType() == typeof(char[]))
                                    str_value = new string((char[])param.objInputValue);
                                else
                                    str_value = (string)param.objInputValue;
                                sbOut.Append(str_value.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (str_value.Length > MaxDBLogger.DataSize)
                                    sbOut.Append(str_value.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                else
                                    sbOut.Append(str_value);
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            sbOut.Append((mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                string str_value;
                                if (param.objInputValue.GetType() == typeof(char[]))
                                    str_value = new string((char[])param.objInputValue);
                                else
                                    str_value = (string)param.objInputValue;
                                sbOut.Append((str_value.Length * Consts.UnicodeWidth).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (str_value.Length > MaxDBLogger.DataSize)
                                    sbOut.Append(str_value.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                else
                                    sbOut.Append(str_value);
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Timestamp:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append(((byte[])FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue)).Length.ToString(
                                    CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((DateTime)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.Time:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                if (param.objInputValue is DateTime)
                                {
                                    sbOut.Append(((byte[])FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue)).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((DateTime)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    sbOut.Append(((byte[])FindColumnInfo(i).TransTimeSpanForInput((TimeSpan)param.objInputValue)).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((TimeSpan)param.objInputValue).ToString());
                                }
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("8".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((double)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.Integer:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("4".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((int)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.SmallInt:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                sbOut.Append("2".PadRight(MaxDBLogger.InputSize));
                                sbOut.Append(((short)param.objInputValue).ToString(CultureInfo.InvariantCulture));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                            if (param.objInputValue != DBNull.Value)
                            {
                                byte[] byte_value = (byte[])param.objInputValue;
                                sbOut.Append(byte_value.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                if (byte_value.Length > MaxDBLogger.DataSize / 2)
                                    sbOut.Append("0X" + Consts.ToHexString(byte_value, MaxDBLogger.DataSize / 2)).Append("...");
                                else
                                    sbOut.Append("0X" + Consts.ToHexString(byte_value));
                            }
                            else
                                sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                            break;
                    }

                    dbConnection.mLogger.SqlTrace(dt, sbOut.ToString());
                }
                //<<< SQL TRACE

                if (param.objInputValue != DBNull.Value)
                {
                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            inputArgs[i] = FindColumnInfo(i).TransBooleanForInput((bool)param.objInputValue);
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
								&& dbConnection.SqlMode == SqlMode.Oracle)
                                // in ORACLE mode a null values will be inserted if the string value is equal to "" 
                                inputArgs[i] = null;
                            else
                            {
                                if (param.objInputValue.GetType() == typeof(char[]))
                                    inputArgs[i] = FindColumnInfo(i).TransStringForInput(new string((char[])param.objInputValue));
                                else
                                    inputArgs[i] = FindColumnInfo(i).TransStringForInput((string)param.objInputValue);
                            }
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Timestamp:
                            inputArgs[i] = FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue);
                            break;
                        case MaxDBType.Time:
                            if (param.objInputValue is DateTime)
                                inputArgs[i] = FindColumnInfo(i).TransDateTimeForInput((DateTime)param.objInputValue);
                            else
                                inputArgs[i] = FindColumnInfo(i).TransTimeSpanForInput((TimeSpan)param.objInputValue);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            inputArgs[i] = FindColumnInfo(i).TransDoubleForInput((double)param.objInputValue);
                            break;
                        case MaxDBType.Integer:
                            inputArgs[i] = FindColumnInfo(i).TransInt64ForInput((int)param.objInputValue);
                            break;
                        case MaxDBType.SmallInt:
                            inputArgs[i] = FindColumnInfo(i).TransInt16ForInput((short)param.objInputValue);
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            inputArgs[i] = FindColumnInfo(i).TransBytesForInput((byte[])param.objInputValue);
                            break;
                    }
                }
                else
                    inputArgs[i] = null;
            }
        }

        private void FillOutputParameters(MaxDBReplyPacket replyPacket, ref MaxDBParameterCollection cmd_params)
        {
            DateTime dt = DateTime.Now;

            //>>> SQL TRACE
            if (dbConnection.mLogger.TraceSQL && mParseInfo.sInputCount < mParseInfo.ParamInfo.Length)
            {
                dbConnection.mLogger.SqlTrace(dt, "OUTPUT PARAMETERS:");
                dbConnection.mLogger.SqlTrace(dt, "APPLICATION");
                dbConnection.mLogger.SqlTraceDataHeader(dt);
            }
            //<<< SQL TRACE

            if (replyPacket.ExistsPart(PartKind.Data))
            {
                for (int i = 0; i < mParseInfo.ParamInfo.Length; i++)
                {
                    MaxDBParameter param = cmd_params[i];
                    if (!FindColumnInfo(i).IsOutput)
                        continue;

                    if (FindColumnInfo(i).IsDBNull(baReplyMemory))
                    {
                        param.objValue = DBNull.Value;
                        continue;
                    }

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            param.objValue = FindColumnInfo(i).GetBoolean(baReplyMemory);
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
                            param.objValue = FindColumnInfo(i).GetString(this, baReplyMemory);
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Time:
                        case MaxDBType.Timestamp:
                            param.objValue = FindColumnInfo(i).GetDateTime(baReplyMemory);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            param.objValue = FindColumnInfo(i).GetDouble(baReplyMemory);
                            break;
                        case MaxDBType.Integer:
                            param.objValue = FindColumnInfo(i).GetInt32(baReplyMemory);
                            break;
                        case MaxDBType.SmallInt:
                            param.objValue = FindColumnInfo(i).GetInt16(baReplyMemory);
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            param.objValue = FindColumnInfo(i).GetBytes(this, baReplyMemory);
                            break;
                    }

                    //>>> SQL TRACE
                    if (dbConnection.mLogger.TraceSQL)
                    {
                        StringBuilder sbOut = new StringBuilder();
                        sbOut.Append((i + 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.NumSize));
                        sbOut.Append(mParseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize));

                        switch (param.dbType)
                        {
                            case MaxDBType.Boolean:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("1".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((bool)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.CharA:
                            case MaxDBType.StrA:
                            case MaxDBType.VarCharA:
                            case MaxDBType.LongA:
                            case MaxDBType.CharE:
                            case MaxDBType.StrE:
                            case MaxDBType.VarCharE:
                            case MaxDBType.LongE:
                                sbOut.Append((mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    string str_value = (string)param.objValue;
                                    sbOut.Append(str_value.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (str_value.Length > MaxDBLogger.DataSize)
                                        sbOut.Append(str_value.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                    else
                                        sbOut.Append(str_value);
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.Unicode:
                            case MaxDBType.StrUni:
                            case MaxDBType.VarCharUni:
                            case MaxDBType.LongUni:
                                sbOut.Append((mParseInfo.ParamInfo[i].PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    string str_value = (string)param.objValue;
                                    sbOut.Append((str_value.Length * Consts.UnicodeWidth).ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (str_value.Length > MaxDBLogger.DataSize)
                                        sbOut.Append(str_value.Substring(0, MaxDBLogger.DataSize)).Append("...");
                                    else
                                        sbOut.Append(str_value);
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.Date:
                            case MaxDBType.Timestamp:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append(FindColumnInfo(i).GetBytes(this, baReplyMemory).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((DateTime)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.Time:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append(FindColumnInfo(i).GetBytes(this, baReplyMemory).Length.ToString(
                                        CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (param.objValue is DateTime)
                                        sbOut.Append(((DateTime)param.objValue).ToString(CultureInfo.InvariantCulture));
                                    else
                                        sbOut.Append(((TimeSpan)param.objValue).ToString());
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.Fixed:
                            case MaxDBType.Float:
                            case MaxDBType.VFloat:
                            case MaxDBType.Number:
                            case MaxDBType.NoNumber:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("8".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((double)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.Integer:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("4".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((int)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.SmallInt:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    sbOut.Append("2".PadRight(MaxDBLogger.InputSize));
                                    sbOut.Append(((short)param.objValue).ToString(CultureInfo.InvariantCulture));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                            case MaxDBType.CharB:
                            case MaxDBType.StrB:
                            case MaxDBType.VarCharB:
                            case MaxDBType.LongB:
                            default:
                                sbOut.Append(mParseInfo.ParamInfo[i].PhysicalLength.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.LenSize));
                                if (param.objValue != null)
                                {
                                    byte[] byte_value = (byte[])param.objValue;
                                    sbOut.Append(byte_value.Length.ToString(CultureInfo.InvariantCulture).PadRight(MaxDBLogger.InputSize));
                                    if (byte_value.Length > MaxDBLogger.DataSize / 2)
                                        sbOut.Append("0X" + Consts.ToHexString(byte_value, MaxDBLogger.DataSize / 2)).Append("...");
                                    else
                                        sbOut.Append("0X" + Consts.ToHexString(byte_value));
                                }
                                else
                                    sbOut.Append(MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize));
                                break;
                        }

                        dbConnection.mLogger.SqlTrace(dt, sbOut.ToString());
                    }

                    //<<< SQL TRACE
                }
            }
        }

		private bool ParseResult(MaxDBReplyPacket replyPacket, DBTechTranslator[] infos, string[] columnNames, CommandBehavior behavior)
		{
			bool isQuery;
			bool rowNotFound = false;
			bool dataPartFound = false;
			
			iRowsAffected = -1;
			bHasRowCount  = false;
			isQuery = FunctionCode.IsQuery(replyPacket.FuncCode);

			DateTime dt = DateTime.Now;

			replyPacket.ClearPartOffset();
			for(int i = 0; i < replyPacket.PartCount; i++) 
			{
				replyPacket.NextPart();
				switch (replyPacket.PartType) 
				{
					case PartKind.ColumnNames:
						if (columnNames == null)
							columnNames = replyPacket.ParseColumnNames();
						break;
					case PartKind.ShortInfo:
						if (infos == null)
							infos = replyPacket.ParseShortFields(dbConnection.bSpaceOption, false, null, false);
						break;
					case PartKind.VardataShortInfo:
						if (infos == null)
							infos = replyPacket.ParseShortFields(dbConnection.bSpaceOption, false, null, true);
						break;
					case PartKind.ResultCount:
						// only if this is not a query
						if(!isQuery) 
						{
							iRowsAffected = replyPacket.ResultCount(true);
							bHasRowCount = true;
							//>>> SQL TRACE
							if (dbConnection.mLogger.TraceSQL)
                                dbConnection.mLogger.SqlTrace(dt, "RESULT COUNT: " + iRowsAffected.ToString(CultureInfo.InvariantCulture));
							//<<< SQL TRACE
						}
						break;
					case PartKind.ResultTableName:
						string cname = replyPacket.ReadString(replyPacket.PartDataPos, replyPacket.PartLength);
						if (cname.Length > 0)
							strCursorName = cname;
						break;
					case PartKind.Data: 
						dataPartFound = true;
						break;
					case PartKind.ErrorText:
						if (replyPacket.ReturnCode == 100) 
						{
							//>>> SQL TRACE
							if (dbConnection.mLogger.TraceSQL)
								dbConnection.mLogger.SqlTrace(dt, "*** ROW NOT FOUND ***");
							//<<< SQL TRACE
							iRowsAffected = -1;
							rowNotFound = true;
							if(!isQuery) iRowsAffected = 0;// for any select update count must be -1
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
									columnNames = replyPacket.ParseColumnNames();
								break;
							case PartKind.ShortInfo:
								if (infos == null)
									infos = replyPacket.ParseShortFields(dbConnection.bSpaceOption, false, null, false);
								break;
							case PartKind.VardataShortInfo:
								if (infos == null)
									infos = replyPacket.ParseShortFields(dbConnection.bSpaceOption, false, null, true);
								break;
							case PartKind.ErrorText:
								newSFI = false;
								break;
							default:
								break;
						}
					}
					
					if (newSFI)
						mParseInfo.SetMetaData(infos, columnNames);
				}
				
				mCurrentDataReader = CreateDataReader(infos, columnNames, rowNotFound, behavior, dataPartFound ? replyPacket : null);
			} 
			
			return isQuery;
		}

		private MaxDBDataReader CreateDataReader(DBTechTranslator[] infos, string[] columnNames,
			bool rowNotFound, CommandBehavior behavior, MaxDBReplyPacket reply)
		{
			MaxDBDataReader reader;

			try 
			{
				reader = new MaxDBDataReader(dbConnection, new FetchInfo(dbConnection, strCursorName, infos, columnNames), 
					this, ((behavior & CommandBehavior.SingleRow) != 0) ? 1 : 0, reply);
			}
			catch (MaxDBException ex) 
			{
				if (ex.ErrorCode == -4000)
					reader = new MaxDBDataReader();
				else 
					throw;
			}

			if (rowNotFound)
				reader.Empty = true;

			return reader;
		}
 
		// Parses the SQL command, or looks the parsed command up in the cache.
		private MaxDBParseInfo DoParse(string sql, bool parseAgain)
		{
			if (sql == null) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLSTATEMENT_NULL), "42000");

			if (mCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.TABLEDIRECT_UNSUPPORTED));

			MaxDBReplyPacket replyPacket;
			MaxDBParseInfo result = null;
			ParseInfoCache cache = dbConnection.mParseCache;
			string[] columnNames = null;

			if (parseAgain) 
			{
				result = mParseInfo;
#if NET20
				result.MassParseID = null;
#endif // NET20
			}
			else if (cache != null)
			{
				result = cache.FindParseInfo(sql);
				//>>> SQL TRACE
				if (dbConnection.mLogger.TraceSQL && result != null)
					dbConnection.mLogger.SqlTrace(DateTime.Now, "CACHED PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
				//<<< SQL TRACE
			}

			if (result == null || parseAgain) 
			{
				//>>> SQL TRACE
				DateTime dt = DateTime.Now;
				if (dbConnection.mLogger.TraceSQL)
				{
					dbConnection.mLogger.SqlTrace(dt, "::PARSE " + strCursorName);
					dbConnection.mLogger.SqlTrace(dt, "SQL COMMAND: " + sql);
				}
				//<<< SQL TRACE

				try 
				{
					bSetWithInfo = true;
					replyPacket = SendSqlCommand(sql, parseAgain);
				} 
				catch(MaxDBTimeoutException)
				{
					replyPacket = SendSqlCommand(sql, parseAgain);
				}

				if (!parseAgain) 
					result = new MaxDBParseInfo(dbConnection, sql, replyPacket.FuncCode);
            
				replyPacket.ClearPartOffset();
				DBTechTranslator[] shortInfos = null;
				for(int i = 0; i < replyPacket.PartCount; i++) 
				{
					replyPacket.NextPart();
					switch (replyPacket.PartType) 
					{
						case PartKind.ParseId:
							int parseidPos = replyPacket.PartDataPos;
							result.SetParseIDAndSession(replyPacket.ReadBytes(parseidPos, 12), 
								replyPacket.Clone(replyPacket.Offset, false).ReadInt32(parseidPos));//session id is always BigEndian number
							//>>> SQL TRACE
							if (dbConnection.mLogger.TraceSQL)
								dbConnection.mLogger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
							//<<< SQL TRACE
							break;
						case PartKind.ShortInfo:
							shortInfos = replyPacket.ParseShortFields(dbConnection.bSpaceOption,
								result.IsDBProc, result.mProcParamInfos, false);
							break;
						case PartKind.VardataShortInfo:
							result.bVarDataInput = true;
							shortInfos = replyPacket.ParseShortFields(dbConnection.bSpaceOption,
								result.IsDBProc, result.mProcParamInfos, true);
							break;
						case PartKind.ResultTableName:
							result.IsSelect = true;
							int cursorLength = replyPacket.PartLength;
							if (cursorLength > 0)
							{
								strCursorName = replyPacket.ReadString(replyPacket.PartDataPos, cursorLength).TrimEnd('\0');
								if (strCursorName.Length == 0)
									result.IsSelect = false;
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
				if ((cache != null) && !parseAgain) 
					cache.AddParseInfo (result);
			}
			objInputArgs = new object[result.mParamInfos.Length];
			ClearParameters();

			//>>> SQL TRACE
			if (dbConnection.mLogger.TraceSQL)
				dbConnection.mLogger.SqlTraceParseInfo(DateTime.Now, result);
			//<<< SQL TRACE

			return result;
		}

#if NET20 && SAFE
        internal void ParseMassCmd (bool parsegain)
        {
            MaxDBRequestPacket requestPacket = dbConnection.GetRequestPacket();
            requestPacket.InitParseCommand (mParseInfo.SqlCommand, true, parsegain);
            requestPacket.SetMassCommand();
            MaxDBReplyPacket replyPacket = dbConnection.Execute(requestPacket, this, GCMode.GC_ALLOWED);
            if (replyPacket.ExistsPart(PartKind.ParseId))
                mParseInfo.MassParseID = replyPacket.ReadBytes(replyPacket.PartDataPos, 12);
        }
#endif // NET20 && SAFE

		private void ClearParameters()
		{
			for (int i = 0; i < objInputArgs.Length; ++i) 
				objInputArgs[i] = strInitialParamValue;
		}

		private static void ResetPutValues(IList inpLongs)
		{
			if (inpLongs != null) 
				foreach(PutValue putval in inpLongs)
					putval.Reset();
		}

		private void HandleStreamsForExecute(DataPart dataPart, object[] args)
		{
			// get all putval objects
            lstInputLongs = new 
#if NET20
            List<PutValue>();
#else
			ArrayList();
#endif
			for (int i = 0; i < mParseInfo.mParamInfos.Length; i++) 
			{
				object inarg = args[i];
				if (inarg == null) 
					continue;
        
				try 
				{
					lstInputLongs.Add((PutValue) inarg);
				}
				catch (InvalidCastException) 
				{
					// not a long for input, ignore
				}
			}

			if(lstInputLongs.Count > 1) 
				lstInputLongs.Sort(mPutValueComparator);
			
			// write data (and patch descriptor)
			for(short i = 0; i < lstInputLongs.Count; i++) 
			{
				PutValue putval = (PutValue)lstInputLongs[i];
				if (putval.AtEnd)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STREAM_ISATEND));  
				putval.TransferStream(dataPart, i);
			}
		}

		private void HandleProcedureStreamsForExecute()
		{
            lstInputProcedureLongs = new 
#if NET20
            List<AbstractProcedurePutValue>();
#else
			ArrayList();
#endif
			for(int i=0; i < mParseInfo.mParamInfos.Length; ++i) 
			{
				object arg = objInputArgs[i];
				if(arg == null) 
					continue;

				try 
				{
					AbstractProcedurePutValue pv = (AbstractProcedurePutValue) arg;
					lstInputProcedureLongs.Add(pv);
					pv.UpdateIndex(lstInputProcedureLongs.Count - 1);
				} 
				catch(InvalidCastException) 
				{
					continue;
				}
			}
		}

		private MaxDBReplyPacket ProcessProcedureStreams(MaxDBReplyPacket packet)
		{
			if (packet.ExistsPart(PartKind.AbapIStream)) 
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.OMS_UNSUPPORTED));

			foreach (AbstractProcedurePutValue pv in lstInputProcedureLongs)
				pv.CloseStream();
			return packet;
		}

		private void HandleStreamsForPutValue(MaxDBReplyPacket replyPacket)
		{
			if (lstInputLongs.Count == 0) 
				return;

			PutValue lastStream = (PutValue) lstInputLongs[lstInputLongs.Count - 1];
			MaxDBRequestPacket requestPacket;
			DataPart dataPart;
			int descriptorPos;
			PutValue putval;
			short firstOpenStream = 0;
			int count = lstInputLongs.Count;
			bool requiresTrailingPacket = false;

			while (!lastStream.AtEnd) 
			{
				GetChangedPutValueDescriptors(replyPacket);
				requestPacket = dbConnection.GetRequestPacket();
				dataPart = requestPacket.InitPutValue(dbConnection.AutoCommit);
				
				// get all descriptors and putvals
				for (short i = firstOpenStream; (i < count) && dataPart.HasRoomFor(LongDesc.Size + 1); i++) 
				{
					putval = (PutValue) lstInputLongs[i];
					if (putval.AtEnd) 
						firstOpenStream++;
					else 
					{
						descriptorPos = dataPart.Extent;
						putval.PutDescriptor(dataPart, descriptorPos);
						dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
						if (bCanceled) 
						{
							putval.MarkErrorStream();
							firstOpenStream++;
						} 
						else 
						{
							putval.TransferStream(dataPart, i);
							if (putval.AtEnd) 
								firstOpenStream++;
						}
					}
				}
				if (lastStream.AtEnd && !bCanceled) 
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
				replyPacket = dbConnection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				
				//  write trailing end of LONGs marker
				if (requiresTrailingPacket && !bCanceled) 
				{
					requestPacket = dbConnection.GetRequestPacket();
					dataPart = requestPacket.InitPutValue(dbConnection.AutoCommit);
					lastStream.MarkAsLast(dataPart);
					dataPart.Close();
					dbConnection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				}
			}
			if (bCanceled) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.STATEMENT_CANCELLED), "42000", -102);
		}

		private void GetChangedPutValueDescriptors(MaxDBReplyPacket replyPacket)
		{
			byte[][] descriptorArray = replyPacket.ParseLongDescriptors();
			byte[] descriptor;
			ByteArray descriptorPointer;
			PutValue putval;
			int valIndex;
			if (!replyPacket.ExistsPart(PartKind.LongData)) 
				return;
			for (int i = 0; i < descriptorArray.Length; ++i) 
			{
				descriptor = descriptorArray[i];
				descriptorPointer = new ByteArray(descriptor);
				valIndex = descriptorPointer.ReadInt16(LongDesc.ValInd);
				putval = (PutValue)lstInputLongs[valIndex];
				putval.SetDescriptor(descriptor);
			}
		}

		private DBTechTranslator FindColumnInfo(int colIndex)
		{
			try 
			{
				return mParseInfo.mParamInfos[colIndex];
			}
			catch(IndexOutOfRangeException) 
			{
				throw new DataException(MaxDBMessages.Extract(MaxDBError.COLINDEX_NOTFOUND, colIndex, this));
			}
		}
		
		MaxDBConnection ISqlParameterController.Connection
		{
			get
			{
				return dbConnection;
			}
		}

		ByteArray ISqlParameterController.ReplyData
		{
			get
			{
				return baReplyMemory;
			}
		}

		#endregion
#else
		#region "Unsafe methods"

        private static unsafe int EvaluateBufferLegth(IntPtr meta, MaxDBParameterCollection[] parameters)
        {
            int bufferLength = 0;
            int paramCount = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterCount(meta);

            for (short i = 1; i <= paramCount; i++)
            {
                bool isInput = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.In;

                foreach (MaxDBParameterCollection param_array in parameters)
                {
                    MaxDBParameter param = param_array[i - 1];

                    int valLength = 0;

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            if (isInput)
                                valLength = sizeof(byte);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongA:
                        case MaxDBType.LongE:
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                {
                                    string strValue = param.objInputValue is char[] ? 
                                        new string((char[])param.objInputValue) : (string)param.objInputValue;
                                    valLength = strValue.Length * sizeof(byte);
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                {
                                    string strValue = param.objInputValue is char[] ?
                                        new string((char[])param.objInputValue) : (string)param.objInputValue;
                                    valLength = strValue.Length * sizeof(char);
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
                            }

                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    valLength = ((byte[])param.objInputValue).Length * sizeof(byte);
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.Date:
                            if (isInput)
                                valLength = sizeof(OdbcDate);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.Time:
                            if (isInput)
                                valLength = sizeof(OdbcTime);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.Timestamp:
                            if (isInput)
                                valLength = sizeof(OdbcTimeStamp);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            if (isInput)
                                valLength = sizeof(double);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i);

                            break;
                        case MaxDBType.Integer:
                            if (isInput)
                                valLength = sizeof(int);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.SmallInt:
                            if (isInput)
                                valLength = sizeof(short);
                            else
                                valLength = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                    }

                    bufferLength += valLength;
                }
            }

            return bufferLength;
        }

		[SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        private static unsafe void FillInputParameters(IntPtr stmt, IntPtr meta, MaxDBParameterCollection[] parameters,
            ByteArray paramArr, byte* bufferPtr, int* sizePtr, byte** addrPtr, int[][] valLen)
        {
            int paramCount = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterCount(meta);
            int paramLen = parameters.Length;

            if (paramCount == 0 || paramLen == 0)
                return;

            int bufferOffset = 0;
            int paramSize;

            for (short i = 0; i < paramCount; i++)
            {
                valLen[i] = new int[paramLen];

                bool isInput = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.In;
                bool isInputOutput = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.InOut;

                MaxDBParameter param;
                int paramOffset = 0;
                paramSize = 0;

                switch (parameters[0][i].dbType)
                {
                    case MaxDBType.Boolean:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(bool);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;
                                paramSize = valLen[i][k] = sizeof(bool);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
								Marshal.WriteByte(new IntPtr(bufferPtr + bufferOffset + paramOffset), (byte)((bool)param.objInputValue ? 1 : 0));

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.CharA:
                    case MaxDBType.StrA:
                    case MaxDBType.VarCharA:
                    case MaxDBType.CharE:
                    case MaxDBType.StrE:
                    case MaxDBType.VarCharE:
                    case MaxDBType.LongA:
                    case MaxDBType.LongE:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            string strValue = string.Empty;
                            
                            if (param.objInputValue != DBNull.Value)
                                strValue  = param.objInputValue is char[] ? new string((char[])param.objInputValue) : (string)param.objInputValue;
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                {
                                    sizePtr[i * paramLen + k] = strValue.Length * sizeof(byte);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);

                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if (paramSize > 0 && strValue != null && strValue.Length > paramSize)
                                strValue = strValue.Substring(0, paramSize);

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                                paramArr.WriteAscii(strValue, bufferOffset + paramOffset);

                            if (isInputOutput && param.objInputValue != DBNull.Value)
                                sizePtr[i * paramLen + k] = strValue.Length;

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Unicode:
                    case MaxDBType.StrUni:
                    case MaxDBType.VarCharUni:
                    case MaxDBType.LongUni:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];

                            string strValue = string.Empty;

                            if (param.objInputValue != DBNull.Value)
                                strValue = param.objInputValue is char[] ? new string((char[])param.objInputValue) : (string)param.objInputValue;

                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                {
                                    sizePtr[i * paramLen + k] = strValue.Length * sizeof(char);
                                    paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(char);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if (paramSize > 0 && strValue != null && strValue.Length > paramSize)
                                strValue = strValue.Substring(0, paramSize);

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                                paramArr.WriteUnicode(strValue, bufferOffset + paramOffset);

                            if (isInputOutput && param.objInputValue != DBNull.Value)
                                sizePtr[i * paramLen + k] = strValue.Length * sizeof(char);

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1),
                            BitConverter.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.CharB:
                    case MaxDBType.StrB:
                    case MaxDBType.VarCharB:
                    case MaxDBType.LongB:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                {
                                    sizePtr[i * paramLen + k] = ((byte[])param.objInputValue).Length * sizeof(byte);
                                    
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
							{
                                byte[] paramValue = (byte[])param.objInputValue;
								Marshal.Copy(paramValue, 0, new IntPtr(bufferPtr + bufferOffset + paramOffset), paramValue.Length);
							}

                            if (isInputOutput && param.objInputValue != DBNull.Value)
                                sizePtr[i * paramLen + k] = ((byte[])param.objInputValue).Length * sizeof(byte);

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Date:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(OdbcDate);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(OdbcDate);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                            {
                                DateTime dt = (DateTime)param.objInputValue;
                                //ODBC date format
                                OdbcDate dt_odbc;
                                dt_odbc.year = (short)(dt.Year % 0x10000);
                                dt_odbc.month = (ushort)(dt.Month % 0x10000);
                                dt_odbc.day = (ushort)(dt.Day % 0x10000);

								Marshal.StructureToPtr(dt_odbc, new IntPtr(bufferPtr + bufferOffset + paramOffset), false);
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));

                        break;
                    case MaxDBType.Time:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(OdbcTime);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(OdbcTime);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                            {
                                //ODBC time format
                                OdbcTime tm_odbc = new OdbcTime();

                                if (param.objInputValue is DateTime)
                                {
                                    DateTime tm = (DateTime)param.objInputValue;

                                    tm_odbc.hour = (ushort)(tm.Hour % 0x10000);
                                    tm_odbc.minute = (ushort)(tm.Minute % 0x10000);
                                    tm_odbc.second = (ushort)(tm.Second % 0x10000);
                                }
                                else
                                {
                                    TimeSpan ts = (TimeSpan)param.objInputValue;

                                    tm_odbc.hour = (ushort)(ts.Hours % 0x10000);
                                    tm_odbc.minute = (ushort)(ts.Minutes % 0x10000);
                                    tm_odbc.second = (ushort)(ts.Seconds % 0x10000);
                                }

								Marshal.StructureToPtr(tm_odbc, new IntPtr(bufferPtr + bufferOffset + paramOffset), false);
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Timestamp:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(OdbcTimeStamp);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(OdbcTimeStamp);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                            {
                                DateTime ts = (DateTime)param.objInputValue;
                                //ODBC timestamp format
                                OdbcTimeStamp ts_odbc = new OdbcTimeStamp();
                                ts_odbc.year = (short)(ts.Year % 0x10000);
                                ts_odbc.month = (ushort)(ts.Month % 0x10000);
                                ts_odbc.day = (ushort)(ts.Day % 0x10000);
                                ts_odbc.hour = (ushort)(ts.Hour % 0x10000);
                                ts_odbc.minute = (ushort)(ts.Minute % 0x10000);
                                ts_odbc.second = (ushort)(ts.Second % 0x10000);
                                ts_odbc.fraction = (uint)(((ts.Ticks % TimeSpan.TicksPerSecond) / (TimeSpan.TicksPerMillisecond / 1000)) * 1000);

								Marshal.StructureToPtr(ts_odbc, new IntPtr(bufferPtr + bufferOffset + paramOffset), false);
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Fixed:
                    case MaxDBType.Float:
                    case MaxDBType.VFloat:
                    case MaxDBType.Number:
                    case MaxDBType.NoNumber:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(double);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(double);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
                                paramArr.WriteDouble((double)param.objInputValue, bufferOffset + paramOffset);

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Integer:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(int);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(int);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
								Marshal.WriteInt32(new IntPtr(bufferPtr + bufferOffset + paramOffset), (int)param.objInputValue);

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.SmallInt:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.objInputValue != DBNull.Value)
                                    sizePtr[i * paramLen + k] = sizeof(short);
                                else
                                    sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_NULL_DATA;

                                paramSize = valLen[i][k] = sizeof(short);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                paramSize = valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            if ((isInput || isInputOutput) && param.objInputValue != DBNull.Value)
								Marshal.WriteInt16(new IntPtr(bufferPtr + bufferOffset + paramOffset), (short)param.objInputValue);

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (UnsafeNativeMethods.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, paramSize, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                                UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                }

                bufferOffset += paramOffset;
            }
        }

		[SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        private static unsafe void FillOutputParameters(IntPtr meta, MaxDBParameterCollection[] parameters,
            ByteArray paramArr, byte* bufferPtr, int[] valSize, int[][] valLen)
        {
            int paramCount = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterCount(meta);
            int paramLen = parameters.Length;
            int bufferOffset = 0;

            for (short i = 0; i < paramCount; i++)
            {
                bool isOutput = (
                    UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.InOut ||
                    UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.Out);

                for (int k = 0; k < paramLen; k++)
                {
                    MaxDBParameter param = parameters[k][i];
                    int valLength = valLen[i][k];

                    switch (param.dbType)
                    {
                        case MaxDBType.Boolean:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = (Marshal.ReadByte(new IntPtr(bufferPtr + bufferOffset)) == 1);
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = Marshal.PtrToStringAnsi(new IntPtr(bufferPtr + bufferOffset), valSize[i * paramLen + k]);
                                else
                                    param.objValue = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            if (isOutput)
                            {
                                //??? LONG ASCII parameter contains extra zeros
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = paramArr.ReadUnicode(bufferOffset, valSize[i * paramLen + k]).Replace("\0", string.Empty);
                                else
                                    param.objValue = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Date:
                            //ODBC date format
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = ODBCConverter.GetDateTime(
										(OdbcDate)Marshal.PtrToStructure(new IntPtr(bufferPtr + bufferOffset), typeof(OdbcDate)));
                                else
                                    param.objValue = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Time:
                            //ODBC time format
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = ODBCConverter.GetDateTime(
										(OdbcTime)Marshal.PtrToStructure(new IntPtr(bufferPtr + bufferOffset), typeof(OdbcTime)));
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Timestamp:
                            //ODBC timestamp format
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = ODBCConverter.GetDateTime(
										(OdbcTimeStamp)Marshal.PtrToStructure(new IntPtr(bufferPtr + bufferOffset), typeof(OdbcTimeStamp)));
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = paramArr.ReadDouble(bufferOffset);
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Integer:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
									param.objValue = Marshal.ReadInt32(new IntPtr(bufferPtr + bufferOffset));
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.SmallInt:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
									param.objValue = Marshal.ReadInt16(new IntPtr(bufferPtr + bufferOffset));
                                else
                                    param.objValue = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            if (isOutput)
                            {
                                if (valSize[i * paramLen + k] != UnsafeNativeMethods.SQLDBC_NULL_DATA)
                                    param.objValue = paramArr.ReadBytes(bufferOffset, valSize[i * paramLen + k]);
                                else
                                    param.objValue = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                    }
                }
            }
        }

        private static unsafe void BindAndExecute(IntPtr stmt, MaxDBParameterCollection[] parameters)
        {
            IntPtr meta = UnsafeNativeMethods.SQLDBC_PreparedStatement_getParameterMetaData(stmt);
            if (meta == IntPtr.Zero)
                return;

            int paramCount = UnsafeNativeMethods.SQLDBC_ParameterMetaData_getParameterCount(meta);
 
            // +1 byte to avoid zero-length array
            ByteArray paramArr = new ByteArray(EvaluateBufferLegth(meta, parameters) + 1, BitConverter.IsLittleEndian);

            UnsafeNativeMethods.SQLDBC_PreparedStatement_setBatchSize(stmt, (uint)parameters.Length);
                        
            int[] valSize = new int[paramCount * parameters.Length];
            int[][] valLen = new int[paramCount][];
            byte*[] valAddr = new byte*[paramCount * parameters.Length];

            SQLDBC_Retcode rc;

			if (paramCount > 0)
				fixed (int* sizePtr = valSize)
				fixed (byte** addrPtr = valAddr)
				fixed (byte* bufferPtr = paramArr.GetArrayData())
				{
					FillInputParameters(stmt, meta, parameters, paramArr, bufferPtr, sizePtr, addrPtr, valLen);

					rc = UnsafeNativeMethods.SQLDBC_PreparedStatement_executeASCII(stmt);

					if (rc == SQLDBC_Retcode.SQLDBC_OK)
						FillOutputParameters(meta, parameters, paramArr, bufferPtr, valSize, valLen);
				}
			else
				rc = UnsafeNativeMethods.SQLDBC_PreparedStatement_executeASCII(stmt);

            if (rc == SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
                throw new MaxDBException(
                    MaxDBMessages.Extract(MaxDBError.EXEC_FAILED) + ": " +
                    MaxDBMessages.Extract(MaxDBError.PARAMETER_TRUNC));

            if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
                MaxDBException.ThrowException(
                    MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
                    UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(stmt));
        }

		internal unsafe string UpdTableName
		{
			get
			{
				if (dbConnection.mTableNames[CommandText] == null)
				{
					byte[] buffer = new byte[1];
					int bufferSize = 0;
					SQLDBC_Retcode rc;

					fixed(byte* bufferPtr = buffer)
					{
						rc = UnsafeNativeMethods.SQLDBC_Statement_getTableName(mStmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, &bufferSize);
						if(rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC) 
							MaxDBException.ThrowException(
								MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
								UnsafeNativeMethods.SQLDBC_Statement_getError(mStmt));
					}

					bufferSize++;//increase buffer for the last zero

					buffer = new byte[bufferSize];
					fixed(byte* bufferPtr = buffer)
					{
						rc = UnsafeNativeMethods.SQLDBC_Statement_getTableName(mStmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, &bufferSize);
						if(rc != SQLDBC_Retcode.SQLDBC_OK) 
							MaxDBException.ThrowException(
								MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
								UnsafeNativeMethods.SQLDBC_Statement_getError(mStmt));
					}

					dbConnection.mTableNames[CommandText] = bufferSize > 1 ? Encoding.ASCII.GetString(buffer, 0, bufferSize - 1) : null;//skip last zero
				}

				return (string)dbConnection.mTableNames[CommandText];
			}
		}

		private void RefreshStmtHandler()
		{
			ReleaseStmtHandler();
			if (dbConnection != null)
				mStmt = UnsafeNativeMethods.SQLDBC_Connection_createPreparedStatement(dbConnection.mConnectionHandler);
		}

		internal void ReleaseStmtHandler()
		{
			if (mStmt != IntPtr.Zero)
				UnsafeNativeMethods.SQLDBC_Connection_releasePreparedStatement(dbConnection.mConnectionHandler, mStmt);
			mStmt = IntPtr.Zero;
		}

		#endregion
#endif // SAFE

        #region ICloneable Members

        public object Clone()
		{
			MaxDBCommand clone = new MaxDBCommand(strCmdText, dbConnection, dbTransaction);
			foreach (MaxDBParameter p in Parameters) 
				clone.Parameters.Add((MaxDBParameter)(p as ICloneable).Clone());
			return clone;
		}

		#endregion

		#region IDbCommand Members

#if NET20
        public override void Cancel()
#else
		public void Cancel()
#endif // NET20
        {
#if SAFE
			AssertOpen ();
			bCanceled = true;
			dbConnection.Cancel(this);
#else
			UnsafeNativeMethods.SQLDBC_Connection_cancel(dbConnection.mConnectionHandler);
#endif // SAFE
        }

#if NET20
        public override string CommandText
#else
		public string CommandText
#endif // NET20
        {
			get 
			{ 
				return strCmdText;  
			}
			set  
			{
                strCmdText = value;
#if !SAFE
				RefreshStmtHandler();
#endif
			}
		}

#if NET20
        public override int CommandTimeout
#else
		public int CommandTimeout
#endif // NET20
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

#if NET20
        public override CommandType CommandType
#else
		public CommandType CommandType
#endif // NET20
        {
			get 
			{ 
				return mCmdType; 
			}
			set 
			{ 
				mCmdType = value; 
			}
		}

#if NET20
        protected override DbConnection DbConnection
#else
		IDbConnection IDbCommand.Connection
#endif // NET20
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

#if NET20
        public new MaxDBConnection Connection
#else
		public MaxDBConnection Connection
#endif // NET20
        {
			get 
			{ 
				return dbConnection;  
			}
			set
			{
				if (dbConnection != value)
					Transaction = null;

#if !SAFE
				if (mStmt != IntPtr.Zero)
					UnsafeNativeMethods.SQLDBC_Connection_releasePreparedStatement(dbConnection.mConnectionHandler, mStmt);
#endif // !SAFE

				dbConnection = value;

#if !SAFE
				if (dbConnection != null)
					mStmt = UnsafeNativeMethods.SQLDBC_Connection_createPreparedStatement(dbConnection.mConnectionHandler);
#endif // !SAFE
			}
		}

#if NET20
		protected override DbParameter CreateDbParameter()
#else
		IDbDataParameter IDbCommand.CreateParameter()
#endif // NET20
        {
            return CreateParameter();
        }

#if NET20
        public static new MaxDBParameter CreateParameter()
#else
		public static MaxDBParameter CreateParameter()
#endif // NET20
        {
			return new MaxDBParameter();
		}

#if NET20
        public override int ExecuteNonQuery()
#else
		public int ExecuteNonQuery()
#endif // NET20
        {
			/*
			 * ExecuteNonQuery is intended for commands that do
			 * not return results, instead returning only the number
			 * of records affected.
			 */
      
			// Execute the command.

			// There must be a valid and open connection.
			AssertOpen();

			Prepare();
#if SAFE
			if (mParseInfo != null && mParseInfo.IsSelect) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLSTATEMENT_RESULTSET));

			Execute(CommandBehavior.Default);
			if(bHasRowCount) 
				return iRowsAffected;
			else 
				return 0;
#else
            BindAndExecute(mStmt, new MaxDBParameterCollection[1] { dbParameters });

			return UnsafeNativeMethods.SQLDBC_PreparedStatement_getRowsAffected(mStmt);
#endif // SAFE
        }

#if !NET20
        IDataReader IDbCommand.ExecuteReader()
		{
			return this.ExecuteReader(CommandBehavior.Default);
		}
#endif // !NET20

#if NET20
        public new MaxDBDataReader ExecuteReader()
#else
		public MaxDBDataReader ExecuteReader()
#endif // NET20
        {
			return ExecuteReader(CommandBehavior.Default);
		}

#if NET20
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
#else
		IDataReader IDbCommand.ExecuteReader(CommandBehavior behavior)
#endif // NET20
        {
            return this.ExecuteReader(behavior);
        }

#if NET20
        public new MaxDBDataReader ExecuteReader(CommandBehavior behavior)
#else
		public MaxDBDataReader ExecuteReader(CommandBehavior behavior)
#endif // NET20
		{
			// Execute the command.

			// There must be a valid and open connection.
			AssertOpen();

			Prepare();
#if SAFE
			Execute(behavior);

			if (mParseInfo.IsSelect)
			{
				if (mCurrentDataReader == null)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SQLCOMMAND_NORESULTSET));

				mCurrentDataReader.bCloseConn = ((behavior & CommandBehavior.CloseConnection) != 0);
				mCurrentDataReader.bSchemaOnly = ((behavior & CommandBehavior.SchemaOnly) != 0);
				return mCurrentDataReader;
			}
			else
				return new MaxDBDataReader(this);
#else
			if ((behavior & CommandBehavior.SingleRow) != 0 || (behavior & CommandBehavior.SchemaOnly) != 0)
				UnsafeNativeMethods.SQLDBC_Statement_setMaxRows(mStmt, 1);
			else
				UnsafeNativeMethods.SQLDBC_Statement_setMaxRows(mStmt, 0);

            BindAndExecute(mStmt, new MaxDBParameterCollection[1] { dbParameters });

			iRowsAffected = UnsafeNativeMethods.SQLDBC_PreparedStatement_getRowsAffected(mStmt);

			if (UnsafeNativeMethods.SQLDBC_Statement_isQuery(mStmt) == SQLDBC_BOOL.SQLDBC_TRUE)
			{
				IntPtr result = UnsafeNativeMethods.SQLDBC_PreparedStatement_getResultSet(mStmt);
				if(result == IntPtr.Zero) 
					MaxDBException.ThrowException(
						MaxDBMessages.Extract(MaxDBError.SQLCOMMAND_NORESULTSET),
						UnsafeNativeMethods.SQLDBC_Statement_getError(mStmt));

				return new MaxDBDataReader(result, dbConnection, this,
					(behavior & CommandBehavior.CloseConnection) != 0,
					(behavior & CommandBehavior.SchemaOnly) != 0
					);
			}
			else
				return new MaxDBDataReader(this);
#endif // SAFE
        }

#if NET20
        public override object ExecuteScalar()
#else
		public object ExecuteScalar()
#endif // NET20
        {
			IDataReader result = ExecuteReader(CommandBehavior.SingleResult);
			if (result.FieldCount > 0 && result.Read())
				return result.GetValue(0);
			else
				return null;
		}

#if NET20
        protected override DbParameterCollection DbParameterCollection
		{
            get 
            {
                MaxDBParameterCollection collection = new MaxDBParameterCollection();
                collection.AddRange(this.Parameters.ToArray());
                return collection;
            }
        }
#else
		IDataParameterCollection IDbCommand.Parameters
		{
			get
			{
				return this.Parameters;
			}
		}
#endif // NET20

#if NET20
        public new MaxDBParameterCollection Parameters
#else
		public MaxDBParameterCollection Parameters
#endif // NET20
        {
			get  
			{ 
				return dbParameters; 
			}
		}

#if NET20
        public override void Prepare()
#else
		public void Prepare()
#endif // NET20
        {
			if (mCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBError.TABLEDIRECT_UNSUPPORTED));

#if SAFE
			mParseInfo = DoParse(strCmdText, false);
#else
			UnsafeNativeMethods.SQLDBC_Connection_setSQLMode(dbConnection.mConnectionHandler, (byte)dbConnection.SqlMode);

			SQLDBC_Retcode rc;

			if (dbConnection.DatabaseEncoding is UnicodeEncoding)
				rc = UnsafeNativeMethods.SQLDBC_PreparedStatement_prepareNTS(mStmt, strCmdText,
					Consts.IsLittleEndian ? SQLDBC_StringEncodingType.UCS2Swapped : SQLDBC_StringEncodingType.UCS2);
			else
				rc = UnsafeNativeMethods.SQLDBC_PreparedStatement_prepareASCII(mStmt, strCmdText);

			if (rc != SQLDBC_Retcode.SQLDBC_OK)
				MaxDBException.ThrowException(
					MaxDBMessages.Extract(MaxDBError.EXEC_FAILED),
					UnsafeNativeMethods.SQLDBC_PreparedStatement_getError(mStmt));
#endif
		}

#if NET20
        protected override DbTransaction DbTransaction
#else
		IDbTransaction IDbCommand.Transaction
#endif // NET20
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

#if NET20
        public new MaxDBTransaction Transaction
#else
		public MaxDBTransaction Transaction
#endif // NET20
        {
			/*
			 * Set the transaction. Consider additional steps to ensure that the transaction
			 * is compatible with the connection, because the two are usually linked.
			 */
			get 
			{ 
				return dbTransaction; 
			}
			set 
			{ 
				dbTransaction = value; 
			}
		}

#if NET20
        public override UpdateRowSource UpdatedRowSource
#else
		public UpdateRowSource UpdatedRowSource
#endif // NET20
        {
			get 
			{ 
				return updatedRowSource;  
			}
			set 
			{ 
				updatedRowSource = value; 
			}
		}

		#endregion

		#region IDisposable Members

#if !NET20
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
		}
#endif // NET20

#if NET20
        protected override void Dispose(bool disposing)
#else
        private void Dispose(bool disposing)
#endif // NET20
        {
#if NET20
            base.Dispose(disposing);
#endif // NET20
            if (disposing)
            {
#if SAFE
                baReplyMemory = null;
			    if (dbConnection != null && (mParseInfo != null && !mParseInfo.IsCached)) 
			    {
                    if (mCurrentDataReader != null)
                        ((IDisposable)mCurrentDataReader).Dispose();
				    dbConnection.DropParseID(mParseInfo.ParseID);
				    mParseInfo.SetParseIDAndSession(null, -1);
#if NET20
				    dbConnection.DropParseID(mParseInfo.MassParseID);
				    mParseInfo.MassParseID = null;
#endif // NET20
				    dbConnection = null;
			    }
#else
				ReleaseStmtHandler();               
#endif // SAFE
            }
        }

		#endregion
	}
}
