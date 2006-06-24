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
using MaxDB.Data.Utils;
using System.Reflection;

namespace MaxDB.Data
{
	/// <summary>
	/// Represents a SQL statement to execute against a MaxDB database. This class cannot be inherited.
	/// </summary>
	sealed public class MaxDBCommand :
#if NET20
        DbCommand
#else // NET20
        IDbCommand
#endif // NET20
        , ICloneable, IDisposable
#if SAFE
        , ISQLParamController
#endif // SAFE
    {
		MaxDBConnection  m_connection;
		MaxDBTransaction  m_txn;
		string m_sCmdText;
		UpdateRowSource m_updatedRowSource = UpdateRowSource.None;
		MaxDBParameterCollection m_parameters = new MaxDBParameterCollection();
		CommandType m_sCmdType = CommandType.Text;
		internal int m_rowsAffected = -1;
        private bool m_fdesignTimeVisible = false;

#if SAFE
		#region "Native implementation parameters"

#if NET20
        private List<AbstractProcedurePutValue> m_inputProcedureLongs;
#else
		private ArrayList m_inputProcedureLongs;
#endif // NET20
		private bool m_setWithInfo = false;
		private bool m_hasRowCount;
		private static int m_maxParseAgainCnt = 10;
#if NET20
        private List<PutValue> m_inputLongs;
#else
		private ArrayList m_inputLongs;
#endif // NET20
		internal MaxDBParseInfo m_parseInfo;
		private MaxDBDataReader m_currentDataReader;
		private object[] m_inputArgs;
		private string m_cursorName;
		private bool m_canceled = false;
		private ByteArray m_replyMem;
		private const string m_initialParamValue = "initParam";
        private static PutValueComparator putvalComparator = new PutValueComparator();

        #endregion
#else
        #region "SQLDBC Wrapper parameters"

		internal IntPtr m_stmt = IntPtr.Zero;
		
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
			m_sCmdText = cmdText;
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection) : this(cmdText)
		{
			m_connection  = connection;
			
#if !SAFE
			RefreshStmtHandler();
#endif
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction txn) : this(cmdText, connection)
		{
			m_txn      = txn;
#if !SAFE
			RefreshStmtHandler();
#endif
		}

#if NET20
        public override bool DesignTimeVisible
#else
        public bool DesignTimeVisible
#endif // NET20
        {
            get
            {
                return m_fdesignTimeVisible;
            }
            set
            {
                m_fdesignTimeVisible = value;
            }
        }

#if SAFE && NET20
        internal int[] ExecuteBatch(MaxDBParameterCollection[] batchParams)
        {
            AssertOpen();
            m_cursorName = m_connection.NextCursorName;
            m_parseInfo = DoParse(m_sCmdText, false);

            if (m_parseInfo != null && m_parseInfo.m_isSelect)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BATCHRESULTSET, new int[0]));
            if (m_parseInfo != null
                && (m_parseInfo.FuncCode == FunctionCode.DBProcExecute || m_parseInfo.FuncCode == FunctionCode.DBProcWithResultSetExecute))
                foreach (DBTechTranslator transl in m_parseInfo.m_paramInfos)
                    if (transl.IsOutput)
                        throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BATCHPROCOUT, new int[0]));

            if (batchParams == null)
                return new int[0];

            //>>> SQL TRACE
            DateTime dt = DateTime.Now;

            if (m_connection.m_logger.TraceSQL)
            {
                m_connection.m_logger.SqlTrace(dt, "::EXECUTE BATCH " + m_cursorName);
                m_connection.m_logger.SqlTrace(dt, "BATCH SIZE " + batchParams.Length);
            }
            //<<< SQL TRACE

            List<PutValue> streamVec = null;
            bool inTrans = m_connection.IsInTransaction;

            try
            {
                m_canceled = false;
                int count = batchParams.Length;
                MaxDBRequestPacket requestPacket;
                MaxDBReplyPacket replyPacket = null;
                int inputCursor = 0;
                bool noError = true;
                int recordSize = 0;
                int insertCountPartSize = MaxDBRequestPacket.ResultCountPartSize;
                int executeCount = -1;
                int[] result = new int[count];
                m_rowsAffected = -1;

                //>>> SQL TRACE
                if (m_connection.m_logger.TraceSQL)
                {
                    m_connection.m_logger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(m_parseInfo.ParseID));
                    m_connection.m_logger.SqlTrace(dt, "SQL COMMAND: " + m_parseInfo.m_sqlCmd);
                }
                //<<< SQL TRACE

                if (m_parseInfo.MassParseID == null)
                    ParseMassCmd(false);

                if (m_parseInfo.m_paramInfos.Length > 0)
                {
                    int currentFieldEnd;

                    foreach (DBTechTranslator currentInfo in m_parseInfo.m_paramInfos)
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
                    requestPacket = m_connection.GetRequestPacket();
                    requestPacket.InitExecute(m_parseInfo.MassParseID, m_connection.AutoCommit);
                    if (executeCount == -1)
                        requestPacket.AddUndefResultCount();
                    else
                        requestPacket.AddResultCount(executeCount);
                    requestPacket.AddCursorPart(m_cursorName);
                    DataPart dataPart;
                    if (m_parseInfo.m_paramInfos.Length > 0)
                    {
                        dataPart = requestPacket.NewDataPart(m_parseInfo.m_varDataInput);
                        if (executeCount == -1)
                            dataPart.SetFirstPart();

                        do
                        {
                            object[] row = new object[batchParams[inputCursor].Count];
                            FillInputParameters(batchParams[inputCursor], ref row);
                            dataPart.AddRow(m_parseInfo.m_inputCount);
                            for (int i = 0; i < m_parseInfo.m_paramInfos.Length; i++)
                            {
                                // check whether the parameter was set by application or throw an exception
                                DBTechTranslator transl = m_parseInfo.m_paramInfos[i];

                                if (transl.IsInput && row[i] != null && m_initialParamValue == row[i].ToString())
                                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BATCHMISSINGIN,
                                                                         (inputCursor + 1).ToString(),
                                                                         (i + 1).ToString()),
                                                                         "0200");

                                transl.Put(dataPart, row[i]);
                            }
                            if (m_parseInfo.m_hasLongs)
                            {
                                HandleStreamsForExecute(dataPart, row);
                                if (streamVec == null)
									streamVec = new List<PutValue>(m_inputLongs);

                            }
                            dataPart.MoveRecordBase();
                            inputCursor++;
                        } while ((inputCursor < count) && dataPart.HasRoomFor(recordSize, insertCountPartSize) && m_parseInfo.IsMassCmd);

                        if (inputCursor == count)
                            dataPart.SetLastPart();
                        dataPart.CloseArrayPart((short)(inputCursor - firstRecordNo));
                    }
                    else
                        inputCursor++; //commands without parameters

                    try
                    {
                        replyPacket = m_connection.Execute(requestPacket, this, GCMode.GC_DELAYED);
                    }
                    catch (MaxDBException ex)
                    {
                        if (ex.VendorCode == -8)
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
                            if (!m_connection.AutoCommit)
                            {
                                if (m_rowsAffected > 0)
                                    m_rowsAffected += ex.ErrorPos - 1;
                                else
                                    m_rowsAffected = ex.ErrorPos - 1;
                            }
                            throw;
                        }
                    }
                    executeCount = replyPacket.ResultCount(false);
                    if (m_parseInfo.m_hasLongs)
                        HandleStreamsForPutValue(replyPacket);
                    if (m_parseInfo.IsMassCmd && executeCount != -1)
                    {
                        if (m_rowsAffected > 0)
                            m_rowsAffected += executeCount;
                        else
                            m_rowsAffected = executeCount;
                    }
                    else
                        m_rowsAffected = executeCount;
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
                m_canceled = false;
            }
        }
#endif // SAFE && NET20

#if !SAFE && NET20
        internal unsafe int[] ExecuteBatch(MaxDBParameterCollection[] batchParams)
        {
            string sql = m_sCmdText;

            if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
                sql = "CALL " + sql;

            if (m_sCmdType == CommandType.TableDirect)
                throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

            try
            {
                PrepareNTS(sql);
                
                BindAndExecute(m_stmt, batchParams);
            }
            catch
            {
                throw;
            }

            return new int[0];
        }
#endif // !SAFE && NET20


#if SAFE
		#region "Methods to support native protocol"

		private MaxDBReplyPacket SendCommand(MaxDBRequestPacket requestPacket, string sqlCmd, int gcFlags, bool parseAgain)
		{
			MaxDBReplyPacket replyPacket;
			requestPacket.InitParseCommand(sqlCmd, true, parseAgain);
			if (m_setWithInfo)
				requestPacket.SetWithInfo();
			replyPacket = m_connection.Execute(requestPacket, false, true, this, gcFlags);
			return replyPacket;
		}

		private MaxDBReplyPacket SendSQL(string sql, bool parseAgain)
		{
			MaxDBReplyPacket replyPacket;

			try
			{
				replyPacket = SendCommand(m_connection.GetRequestPacket(), sql, GCMode.GC_ALLOWED, parseAgain);
			}
			catch (IndexOutOfRangeException) 
			{
				// tbd: info about current length?
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_TOOLONG), "42000");
			}

			return replyPacket;
		}

		internal void AssertOpen() 
		{
			if (m_connection == null || m_connection.m_comm == null) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OBJECTISCLOSED));
		}

		private void Reparse()
		{
			object[] tmpArgs = m_inputArgs;
			DoParse(m_parseInfo.m_sqlCmd, true);
			m_inputArgs = tmpArgs;
		}

		internal bool Execute(CommandBehavior behavior)
		{
			AssertOpen();
			m_cursorName = m_connection.NextCursorName;

			return Execute(m_maxParseAgainCnt, behavior);
		}

		internal bool Execute(int afterParseAgain, CommandBehavior behavior)
		{
			if (m_connection == null) 
				throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_CONNECTIONNULL));

			//>>> SQL TRACE
			DateTime dt = DateTime.Now;

			if (m_connection.m_logger.TraceSQL)
				m_connection.m_logger.SqlTrace(dt, "::EXECUTE " + m_cursorName);
			//<<< SQL TRACE

			MaxDBRequestPacket requestPacket;
			MaxDBReplyPacket replyPacket;
			bool isQuery;
			DataPart dataPart;

			// if this is one of the statements that is executed during parse instead of execution, execute it by doing a reparse
			if(m_parseInfo.IsAlreadyExecuted) 
			{
				m_replyMem = null;
				if (m_connection == null) 
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_CONNECTIONNULL));
				Reparse();
				m_rowsAffected = 0;
				return false;
			}

			try 
			{
				m_canceled = false;
				// check if a reparse is needed.
				if (!m_parseInfo.IsValid) 
					Reparse();

				//>>> SQL TRACE
				if (m_connection.m_logger.TraceSQL)
				{
					m_connection.m_logger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(m_parseInfo.ParseID));
					m_connection.m_logger.SqlTrace(dt, "SQL COMMAND: " +m_parseInfo.m_sqlCmd);
				}
				//<<< SQL TRACE
				
				m_replyMem = null;

				requestPacket = m_connection.GetRequestPacket();
				requestPacket.InitExecute(m_parseInfo.ParseID, m_connection.AutoCommit);
				if (m_parseInfo.m_isSelect) 
					requestPacket.AddCursorPart(m_cursorName);

                FillInputParameters(m_parameters,ref m_inputArgs);

				if (m_parseInfo.m_inputCount > 0 || m_parseInfo.m_hasStreams) 
				{
					dataPart = requestPacket.NewDataPart(m_parseInfo.m_varDataInput);
					if (m_parseInfo.m_inputCount > 0) 
					{
						dataPart.AddRow(m_parseInfo.m_inputCount);
						for (int i = 0; i < m_parseInfo.m_paramInfos.Length; i++) 
						{
							if (m_parseInfo.m_paramInfos[i].IsInput && m_inputArgs[i] != null &&
								m_initialParamValue == m_inputArgs[i].ToString())
							{
								if (m_parseInfo.m_paramInfos[i].IsStreamKind)
									throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));
								else
									throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_MISSINGINOUT, i + 1, "02000"));
							}
							else 
								m_parseInfo.m_paramInfos[i].Put(dataPart, m_inputArgs[i]);
						}
						m_inputProcedureLongs = null;
						if (m_parseInfo.m_hasLongs) 
						{
							if (m_parseInfo.m_isDBProc) 
								HandleProcedureStreamsForExecute(dataPart, m_inputArgs);
							else 
								HandleStreamsForExecute(dataPart, m_inputArgs);
						}
						if (m_parseInfo.m_hasStreams) 
							throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));
					} 
					else 
						throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));
					dataPart.Close();
				}
				// add a decribe order if command rturns a resultset
				if (m_parseInfo.m_isSelect && m_parseInfo.ColumnInfo == null	&& m_parseInfo.m_funcCode != FunctionCode.DBProcWithResultSetExecute)
				{
					requestPacket.InitDbsCommand("DESCRIBE ", false, false);
					requestPacket.AddParseIdPart(m_parseInfo.ParseID);
				}

				try 
				{
					replyPacket = m_connection.Execute(requestPacket, this,                                              
						(!m_parseInfo.m_hasStreams && m_inputProcedureLongs == null) ? GCMode.GC_ALLOWED : GCMode.GC_NONE);
					// Recycling of parse infos and cursor names is not allowed
					// if streams are in the command. Even sending it just behind
					// as next packet is harmful. Same with INPUT LONG parameters of
					// DB Procedures.
				}
				catch(MaxDBException ex) 
				{
					if (ex.VendorCode == -8 && afterParseAgain > 0) 
					{
						//>>> SQL TRACE
						if (m_connection.m_logger.TraceSQL)
							m_connection.m_logger.SqlTrace(dt, "PARSE AGAIN");
						//<<< SQL TRACE
						ResetPutValues(m_inputLongs);
						Reparse();
						m_connection.FreeRequestPacket(requestPacket);
						afterParseAgain--;
						return Execute(afterParseAgain, behavior);
					}

					// The request packet has already been recycled
					throw;
				}

				// --- now it becomes difficult ...
				if (m_parseInfo.m_isSelect) 
					isQuery = ParseResult(replyPacket, null, m_parseInfo.ColumnInfo, m_parseInfo.m_columnNames, behavior);
				else 
				{
					if(m_inputProcedureLongs != null) 
						replyPacket = ProcessProcedureStreams(replyPacket);
					else if (m_parseInfo.m_hasStreams) 
						throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));
					isQuery = ParseResult(replyPacket, null, m_parseInfo.ColumnInfo, m_parseInfo.m_columnNames, behavior);
					int returnCode = replyPacket.ReturnCode;
					if (replyPacket.ExistsPart(PartKind.Data)) 
						m_replyMem = replyPacket.Clone(replyPacket.PartDataPos);
					if ((m_parseInfo.m_hasLongs && !m_parseInfo.m_isDBProc) && (returnCode == 0)) 
						HandleStreamsForPutValue(replyPacket);
				}

                FillOutputParameters(replyPacket, ref m_parameters);
 
				return isQuery;
			}
			catch (MaxDBTimeoutException timeout) 
			{
				if (m_connection.IsInTransaction) 
					throw timeout;
				else 
				{
					ResetPutValues(m_inputLongs);
					Reparse();
					return Execute(m_maxParseAgainCnt, behavior);
				}
			} 
			finally
			{
				m_canceled = false;
			}
		}

        private void FillInputParameters(MaxDBParameterCollection cmd_params, ref object[] inputArgs)
        {
            DateTime dt = DateTime.Now;

            //>>> SQL TRACE
            if (m_connection.m_logger.TraceSQL && m_parseInfo.m_inputCount > 0)
            {
                m_connection.m_logger.SqlTrace(dt, "INPUT PARAMETERS:");
                m_connection.m_logger.SqlTrace(dt, "APPLICATION");
                m_connection.m_logger.SqlTraceDataHeader(dt);
            }
            //<<< SQL TRACE

            // We must add a data part if we have input parameters or even if we have output streams.
            for (int i = 0; i < m_parseInfo.ParamInfo.Length; i++)
            {
                MaxDBParameter param = cmd_params[i];

                if (!FindColInfo(i).IsInput)
                    continue;

                //>>> SQL TRACE
                if (m_connection.m_logger.TraceSQL)
                {
                    string s_out = (i + 1).ToString().PadRight(MaxDBLogger.NumSize);
                    s_out += m_parseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize);

                    switch (param.m_dbType)
                    {
                        case MaxDBType.Boolean:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                s_out += "1".PadRight(MaxDBLogger.InputSize);
                                s_out += ((bool)param.m_inputValue).ToString();
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.LongA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                        case MaxDBType.LongE:
                            s_out += (m_parseInfo.ParamInfo[i].PhysicalLength - 1).ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                string str_value;
                                if (param.m_inputValue.GetType() == typeof(char[]))
                                    str_value = new string((char[])param.m_inputValue);
                                else
                                    str_value = (string)param.m_inputValue;
                                s_out += str_value.Length.ToString().PadRight(MaxDBLogger.InputSize);
                                if (str_value.Length > MaxDBLogger.DataSize)
                                    s_out += str_value.Substring(0, MaxDBLogger.DataSize) + "...";
                                else
                                    s_out += str_value;
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            s_out += (m_parseInfo.ParamInfo[i].PhysicalLength - 1).ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                string str_value;
                                if (param.m_inputValue.GetType() == typeof(char[]))
                                    str_value = new string((char[])param.m_inputValue);
                                else
                                    str_value = (string)param.m_inputValue;
                                s_out += (str_value.Length * Consts.UnicodeWidth).ToString().PadRight(MaxDBLogger.InputSize);
                                if (str_value.Length > MaxDBLogger.DataSize)
                                    s_out += str_value.Substring(0, MaxDBLogger.DataSize) + "...";
                                else
                                    s_out += str_value;
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.TimeStamp:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                s_out += ((byte[])FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue)).Length.ToString().PadRight(MaxDBLogger.InputSize);
                                s_out += ((DateTime)param.m_inputValue).ToString();
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.Time:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                if (param.m_inputValue is DateTime)
                                {
                                    s_out += ((byte[])FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue)).Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    s_out += ((DateTime)param.m_inputValue).ToString();
                                }
                                else
                                {
                                    s_out += ((byte[])FindColInfo(i).TransTimeSpanForInput((TimeSpan)param.m_inputValue)).Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    s_out += ((TimeSpan)param.m_inputValue).ToString();
                                }
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                s_out += "8".PadRight(MaxDBLogger.InputSize);
                                s_out += ((double)param.m_inputValue).ToString();
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.Integer:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                s_out += "4".PadRight(MaxDBLogger.InputSize);
                                s_out += ((int)param.m_inputValue).ToString();
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.SmallInt:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                s_out += "2".PadRight(MaxDBLogger.InputSize);
                                s_out += ((short)param.m_inputValue).ToString();
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                            if (param.m_inputValue != DBNull.Value)
                            {
                                byte[] byte_value = (byte[])param.m_inputValue;
                                s_out += byte_value.Length.ToString().PadRight(MaxDBLogger.InputSize);
                                if (byte_value.Length > MaxDBLogger.DataSize / 2)
                                    s_out += "0X" + Consts.ToHexString(byte_value, MaxDBLogger.DataSize / 2) + "...";
                                else
                                    s_out += "0X" + Consts.ToHexString(byte_value);
                            }
                            else
                                s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                            break;
                    }

                    m_connection.m_logger.SqlTrace(dt, s_out);
                }
                //<<< SQL TRACE

                if (param.m_inputValue != DBNull.Value)
                {
                    switch (param.m_dbType)
                    {
                        case MaxDBType.Boolean:
                            inputArgs[i] = FindColInfo(i).TransBooleanForInput((bool)param.m_inputValue);
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
                            if (param.m_inputValue != null &&
                                m_connection.SQLMode == SqlMode.Oracle && param.m_inputValue.ToString() == string.Empty)
                                // in ORACLE mode a null values will be inserted if the string value is equal to "" 
                                inputArgs[i] = null;
                            else
                            {
                                if (param.m_inputValue.GetType() == typeof(char[]))
                                    inputArgs[i] = FindColInfo(i).TransStringForInput(new string((char[])param.m_inputValue));
                                else
                                    inputArgs[i] = FindColInfo(i).TransStringForInput((string)param.m_inputValue);
                            }
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.TimeStamp:
                            inputArgs[i] = FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue);
                            break;
                        case MaxDBType.Time:
                            if (param.m_inputValue is DateTime)
                                inputArgs[i] = FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue);
                            else
                                inputArgs[i] = FindColInfo(i).TransTimeSpanForInput((TimeSpan)param.m_inputValue);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            inputArgs[i] = FindColInfo(i).TransDoubleForInput((double)param.m_inputValue);
                            break;
                        case MaxDBType.Integer:
                            inputArgs[i] = FindColInfo(i).TransInt64ForInput((int)param.m_inputValue);
                            break;
                        case MaxDBType.SmallInt:
                            inputArgs[i] = FindColInfo(i).TransInt16ForInput((short)param.m_inputValue);
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            inputArgs[i] = FindColInfo(i).TransBytesForInput((byte[])param.m_inputValue);
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
            if (m_connection.m_logger.TraceSQL && m_parseInfo.m_inputCount < m_parseInfo.ParamInfo.Length)
            {
                m_connection.m_logger.SqlTrace(dt, "OUTPUT PARAMETERS:");
                m_connection.m_logger.SqlTrace(dt, "APPLICATION");
                m_connection.m_logger.SqlTraceDataHeader(dt);
            }
            //<<< SQL TRACE

            if (replyPacket.ExistsPart(PartKind.Data))
            {
                for (int i = 0; i < m_parseInfo.ParamInfo.Length; i++)
                {
                    MaxDBParameter param = cmd_params[i];
                    if (!FindColInfo(i).IsOutput)
                        continue;

                    if (FindColInfo(i).IsDBNull(m_replyMem))
                    {
                        param.m_value = DBNull.Value;
                        continue;
                    }

                    switch (param.m_dbType)
                    {
                        case MaxDBType.Boolean:
                            param.m_value = FindColInfo(i).GetBoolean(m_replyMem);
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
                            param.m_value = FindColInfo(i).GetString(this, m_replyMem);
                            break;
                        case MaxDBType.Date:
                        case MaxDBType.Time:
                            param.m_value = FindColInfo(i).GetDateTime(m_replyMem);
                            break;
                        case MaxDBType.TimeStamp:
                            param.m_value = FindColInfo(i).GetTimeSpan(m_replyMem);
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            param.m_value = FindColInfo(i).GetDouble(m_replyMem);
                            break;
                        case MaxDBType.Integer:
                            param.m_value = FindColInfo(i).GetInt32(m_replyMem);
                            break;
                        case MaxDBType.SmallInt:
                            param.m_value = FindColInfo(i).GetInt16(m_replyMem);
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            param.m_value = FindColInfo(i).GetBytes(this, m_replyMem);
                            break;
                    }

                    //>>> SQL TRACE
                    if (m_connection.m_logger.TraceSQL)
                    {
                        string s_out = (i + 1).ToString().PadRight(MaxDBLogger.NumSize);
                        s_out += m_parseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize);

                        switch (param.m_dbType)
                        {
                            case MaxDBType.Boolean:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += "1".PadRight(MaxDBLogger.InputSize);
                                    s_out += ((bool)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.CharA:
                            case MaxDBType.StrA:
                            case MaxDBType.VarCharA:
                            case MaxDBType.LongA:
                            case MaxDBType.CharE:
                            case MaxDBType.StrE:
                            case MaxDBType.VarCharE:
                            case MaxDBType.LongE:
                                s_out += (m_parseInfo.ParamInfo[i].PhysicalLength - 1).ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    string str_value = (string)param.m_value;
                                    s_out += str_value.Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    if (str_value.Length > MaxDBLogger.DataSize)
                                        s_out += str_value.Substring(0, MaxDBLogger.DataSize) + "...";
                                    else
                                        s_out += str_value;
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.Unicode:
                            case MaxDBType.StrUni:
                            case MaxDBType.VarCharUni:
                            case MaxDBType.LongUni:
                                s_out += (m_parseInfo.ParamInfo[i].PhysicalLength - 1).ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    string str_value = (string)param.m_value;
                                    s_out += (str_value.Length * Consts.UnicodeWidth).ToString().PadRight(MaxDBLogger.InputSize);
                                    if (str_value.Length > MaxDBLogger.DataSize)
                                        s_out += str_value.Substring(0, MaxDBLogger.DataSize) + "...";
                                    else
                                        s_out += str_value;
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.Date:
                            case MaxDBType.TimeStamp:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += FindColInfo(i).GetBytes(this, m_replyMem).Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    s_out += ((DateTime)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.Time:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += FindColInfo(i).GetBytes(this, m_replyMem).Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    if (param.m_value is DateTime)
                                        s_out += ((DateTime)param.m_value).ToString();
                                    else
                                        s_out += ((TimeSpan)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.Fixed:
                            case MaxDBType.Float:
                            case MaxDBType.VFloat:
                            case MaxDBType.Number:
                            case MaxDBType.NoNumber:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += "8".PadRight(MaxDBLogger.InputSize);
                                    s_out += ((double)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.Integer:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += "4".PadRight(MaxDBLogger.InputSize);
                                    s_out += ((int)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.SmallInt:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    s_out += "2".PadRight(MaxDBLogger.InputSize);
                                    s_out += ((short)param.m_value).ToString();
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                            case MaxDBType.CharB:
                            case MaxDBType.StrB:
                            case MaxDBType.VarCharB:
                            case MaxDBType.LongB:
                            default:
                                s_out += m_parseInfo.ParamInfo[i].PhysicalLength.ToString().PadRight(MaxDBLogger.LenSize);
                                if (param.m_value != null)
                                {
                                    byte[] byte_value = (byte[])param.m_value;
                                    s_out += byte_value.Length.ToString().PadRight(MaxDBLogger.InputSize);
                                    if (byte_value.Length > MaxDBLogger.DataSize / 2)
                                        s_out += "0X" + Consts.ToHexString(byte_value, MaxDBLogger.DataSize / 2) + "...";
                                    else
                                        s_out += "0X" + Consts.ToHexString(byte_value);
                                }
                                else
                                    s_out += MaxDBLogger.Null.PadRight(MaxDBLogger.InputSize);
                                break;
                        }

                        m_connection.m_logger.SqlTrace(dt, s_out);
                    }

                    //<<< SQL TRACE
                }
            }
        }

		private bool ParseResult(MaxDBReplyPacket replyPacket, string sqlCmd, DBTechTranslator[] infos, 
			string[] columnNames, CommandBehavior behavior)
		{
			string tableName = null;
			bool isQuery;
			bool rowNotFound = false;
			bool dataPartFound = false;
			
			m_rowsAffected = -1;
			m_hasRowCount  = false;
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
							infos = replyPacket.ParseShortFields(m_connection.m_spaceOption, false, null, false);
						break;
					case PartKind.Vardata_ShortInfo:
						if (infos == null)
							infos = replyPacket.ParseShortFields(m_connection.m_spaceOption, false, null, true);
						break;
					case PartKind.ResultCount:
						// only if this is not a query
						if(!isQuery) 
						{
							m_rowsAffected = replyPacket.ResultCount(true);
							m_hasRowCount = true;
							//>>> SQL TRACE
							if (m_connection.m_logger.TraceSQL)
								m_connection.m_logger.SqlTrace(dt, "RESULT COUNT: " + m_rowsAffected.ToString());
							//<<< SQL TRACE
						}
						break;
					case PartKind.ResultTableName:
						string cname = replyPacket.ReadASCII(replyPacket.PartDataPos, replyPacket.PartLength);
						if (cname.Length > 0)
							m_cursorName = cname;
						break;
					case PartKind.Data: 
						dataPartFound = true;
						break;
					case PartKind.ErrorText:
						if (replyPacket.ReturnCode == 100) 
						{
							//>>> SQL TRACE
							if (m_connection.m_logger.TraceSQL)
								m_connection.m_logger.SqlTrace(dt, "*** ROW NOT FOUND ***");
							//<<< SQL TRACE
							m_rowsAffected = -1;
							rowNotFound = true;
							if(!isQuery) m_rowsAffected = 0;// for any select update count must be -1
						}
						break;
					case PartKind.TableName:
						tableName = replyPacket.ReadASCII(replyPacket.PartDataPos, replyPacket.PartLength);
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
									infos = replyPacket.ParseShortFields(m_connection.m_spaceOption, false, null, false);
								break;
							case PartKind.Vardata_ShortInfo:
								if (infos == null)
									infos = replyPacket.ParseShortFields(m_connection.m_spaceOption, false, null, true);
								break;
							case PartKind.ErrorText:
								newSFI = false;
								break;
							default:
								break;
						}
					}
					
					if (newSFI)
						m_parseInfo.SetMetaData(infos, columnNames);
				}
				
				if (dataPartFound)
					CreateDataReader(sqlCmd, tableName, infos, columnNames, rowNotFound, behavior, replyPacket);
				else
					CreateDataReader(sqlCmd, tableName, infos, columnNames, rowNotFound, behavior, null);
			} 
			
			return isQuery;
		}

		private void CreateDataReader(string sqlCmd, string tableName, DBTechTranslator[] infos, string[] columnNames, 
			bool rowNotFound, CommandBehavior behavior, MaxDBReplyPacket reply)
		{
			try 
			{
				FetchInfo fetchInfo = new FetchInfo(m_connection, m_cursorName, infos, columnNames);
				m_currentDataReader = new MaxDBDataReader(m_connection, fetchInfo, this, 
					((behavior & CommandBehavior.SingleRow) != 0) ? 1 : 0, reply);
			}
			catch (MaxDBException ex) 
			{
				if (ex.VendorCode == -4000) 
					m_currentDataReader = new MaxDBDataReader();
				else 
					throw;
			}

			if (rowNotFound) 
				m_currentDataReader.Empty = true;
		}
 
		// Parses the SQL command, or looks the parsed command up in the cache.
		private MaxDBParseInfo DoParse(string sql, bool parseAgain)
		{
			if (sql == null) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_NULL), "42000");

			if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
				sql = "CALL " + sql;

			if (m_sCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

			MaxDBReplyPacket replyPacket;
			MaxDBParseInfo result = null;
			ParseInfoCache cache = m_connection.m_parseCache;
			string[] columnNames = null;

			if (parseAgain) 
			{
				result = m_parseInfo;
				result.MassParseID = null;
			}
			else if (cache != null)
			{
				result = cache.FindParseInfo(sql);
				//>>> SQL TRACE
				if (m_connection.m_logger.TraceSQL)
					m_connection.m_logger.SqlTrace(DateTime.Now, "CACHED PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
				//<<< SQL TRACE
			}

			if ((result == null) || parseAgain) 
			{
				//>>> SQL TRACE
				DateTime dt = DateTime.Now;
				if (m_connection.m_logger.TraceSQL)
				{
					m_connection.m_logger.SqlTrace(dt, "::PARSE " + m_cursorName);
					m_connection.m_logger.SqlTrace(dt, "SQL COMMAND: " + sql);
				}
				//<<< SQL TRACE

				try 
				{
					m_setWithInfo = true;
					replyPacket = SendSQL(sql, parseAgain);
				} 
				catch(MaxDBTimeoutException)
				{
					replyPacket = SendSQL(sql, parseAgain);
				}

				if (!parseAgain) 
					result = new MaxDBParseInfo(m_connection, sql, replyPacket.FuncCode);
            
				replyPacket.ClearPartOffset();
				DBTechTranslator[] shortInfos = null;
				for(int i = 0; i < replyPacket.PartCount; i++) 
				{
					replyPacket.NextPart();
					switch (replyPacket.PartType) 
					{
						case PartKind.Parsid:
							int parseidPos = replyPacket.PartDataPos;
							result.SetParseIDAndSession(replyPacket.ReadBytes(parseidPos, 12), 
								replyPacket.Clone(replyPacket.Offset, false).ReadInt32(parseidPos));//session id is always BigEndian number
							//>>> SQL TRACE
							if (m_connection.m_logger.TraceSQL)
								m_connection.m_logger.SqlTrace(dt, "PARSE ID: 0x" + Consts.ToHexString(result.ParseID));
							//<<< SQL TRACE
							break;
						case PartKind.ShortInfo:
							shortInfos = replyPacket.ParseShortFields(m_connection.m_spaceOption,
								result.m_isDBProc, result.m_procParamInfos, false);
							break;
						case PartKind.Vardata_ShortInfo:
							result.m_varDataInput = true;
							shortInfos = replyPacket.ParseShortFields(m_connection.m_spaceOption,
								result.m_isDBProc, result.m_procParamInfos, true);
							break;
						case PartKind.ResultTableName:
							result.m_isSelect = true;
							int cursorLength = replyPacket.PartLength;
							if (cursorLength > 0) 
								m_cursorName = replyPacket.ReadString(replyPacket.PartDataPos, cursorLength);
							break;
						case PartKind.TableName:
							result.m_updTableName = replyPacket.ReadString(replyPacket.PartDataPos, replyPacket.PartLength);
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
					cache.addParseinfo (result);
			}
			m_inputArgs = new object[result.m_paramInfos.Length];
			ClearParameters();

			//>>> SQL TRACE
			if (m_connection.m_logger.TraceSQL)
				m_connection.m_logger.SqlTraceParseInfo(DateTime.Now, result);
			//<<< SQL TRACE

			return result;
		}

        internal void ParseMassCmd (bool parsegain)
        {
            MaxDBRequestPacket requestPacket = m_connection.GetRequestPacket();
            requestPacket.InitParseCommand (m_parseInfo.m_sqlCmd, true, parsegain);
            requestPacket.SetMassCommand();
            MaxDBReplyPacket replyPacket = m_connection.Execute(requestPacket, this, GCMode.GC_ALLOWED);
            if (replyPacket.ExistsPart(PartKind.Parsid))
                m_parseInfo.MassParseID = replyPacket.ReadBytes(replyPacket.PartDataPos, 12);
        }

		private void ClearParameters()
		{
			for (int i = 0; i < m_inputArgs.Length; ++i) 
				m_inputArgs[i] = m_initialParamValue;
		}

		private void ResetPutValues(IList inpLongs)
		{
			if (inpLongs != null) 
				foreach(PutValue putval in inpLongs)
					putval.Reset();
		}

		private void HandleStreamsForExecute(DataPart dataPart, object[] args)
		{
			// get all putval objects
            m_inputLongs = new 
#if NET20
            List<PutValue>();
#else
			ArrayList();
#endif
			for (int i = 0; i < m_parseInfo.m_paramInfos.Length; i++) 
			{
				object inarg = args[i];
				if (inarg == null) 
					continue;
        
				try 
				{
					m_inputLongs.Add((PutValue) inarg);
				}
				catch (InvalidCastException) 
				{
					// not a long for input, ignore
				}
			}

			if(m_inputLongs.Count > 1) 
				m_inputLongs.Sort(putvalComparator);
			
			// write data (and patch descriptor)
			for(short i = 0; i < m_inputLongs.Count; i++) 
			{
				PutValue putval = (PutValue)m_inputLongs[i];
				if (putval.AtEnd)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_ISATEND));  
				putval.TransferStream(dataPart, i);
			}
		}

		private void HandleProcedureStreamsForExecute(DataPart dataPart, object[] objects)
		{
            m_inputProcedureLongs = new 
#if NET20
            List<AbstractProcedurePutValue>();
#else
			ArrayList();
#endif
			for(int i=0; i < m_parseInfo.m_paramInfos.Length; ++i) 
			{
				object arg = m_inputArgs[i];
				if(arg == null) 
					continue;

				try 
				{
					AbstractProcedurePutValue pv = (AbstractProcedurePutValue) arg;
					m_inputProcedureLongs.Add(pv);
					pv.UpdateIndex(m_inputProcedureLongs.Count - 1);
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
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));

			foreach (AbstractProcedurePutValue pv in m_inputProcedureLongs)
				pv.CloseStream();
			return packet;
		}

		private void HandleStreamsForPutValue(MaxDBReplyPacket replyPacket)
		{
			if (m_inputLongs.Count == 0) 
				return;

			PutValue lastStream = (PutValue) m_inputLongs[m_inputLongs.Count - 1];
			MaxDBRequestPacket requestPacket;
			DataPart dataPart;
			int descriptorPos;
			PutValue putval;
			short firstOpenStream = 0;
			int count = m_inputLongs.Count;
			bool requiresTrailingPacket = false;

			while (!lastStream.AtEnd) 
			{
				GetChangedPutValueDescriptors(replyPacket);
				requestPacket = m_connection.GetRequestPacket();
				dataPart = requestPacket.InitPutValue(m_connection.AutoCommit);
				
				// get all descriptors and putvals
				for (short i = firstOpenStream; (i < count) && dataPart.HasRoomFor(LongDesc.Size + 1); i++) 
				{
					putval = (PutValue) m_inputLongs[i];
					if (putval.AtEnd) 
						firstOpenStream++;
					else 
					{
						descriptorPos = dataPart.Extent;
						putval.PutDescriptor(dataPart, descriptorPos);
						dataPart.AddArg(descriptorPos, LongDesc.Size + 1);
						if (m_canceled) 
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
				if (lastStream.AtEnd && !m_canceled) 
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
				replyPacket = m_connection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				
				//  write trailing end of LONGs marker
				if (requiresTrailingPacket && !m_canceled) 
				{
					requestPacket = m_connection.GetRequestPacket();
					dataPart = requestPacket.InitPutValue(m_connection.AutoCommit);
					lastStream.MarkAsLast(dataPart);
					dataPart.Close();
					m_connection.Execute(requestPacket, this, GCMode.GC_DELAYED);
				}
			}
			if (m_canceled) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STATEMENT_CANCELLED), "42000", -102);
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
				putval = (PutValue)m_inputLongs[valIndex];
				putval.SetDescriptor(descriptor);
			}
		}

#if NET20
        private void ResetPutValues(List<PutValue> inpLongs)
#else
		private void ResetPutValues(ArrayList inpLongs)
#endif // NET20
        {
            if (inpLongs != null)
                foreach (PutValue val in inpLongs)
                    val.Reset();
        }

		private DBTechTranslator FindColInfo(int colIndex)
		{
			try 
			{
				return m_parseInfo.m_paramInfos[colIndex];
			}
			catch(IndexOutOfRangeException) 
			{
				throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_COLINDEX_NOTFOUND, colIndex, this));
			}
		}
		
		MaxDBConnection ISQLParamController.Connection
		{
			get
			{
				return m_connection;
			}
		}

		ByteArray ISQLParamController.ReplyData
		{
			get
			{
				return m_replyMem;
			}
		}

		#endregion
#else
		#region "Unsafe methods"

        private unsafe int EvaluateBufferLegth(IntPtr meta, MaxDBParameterCollection[] parameters)
        {
            int bufferLength = 0;
            int paramCount = SQLDBC.SQLDBC_ParameterMetaData_getParameterCount(meta);

            for (short i = 1; i <= paramCount; i++)
            {
                bool input_val = (
                       SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.In ||
                       SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.InOut);

                foreach (MaxDBParameterCollection param_array in parameters)
                {
                    MaxDBParameter param = param_array[i - 1];

                    int valLength = 0;

                    switch (param.m_dbType)
                    {
                        case MaxDBType.Boolean:
                            if (input_val)
                                valLength = sizeof(byte);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.CharA:
                        case MaxDBType.StrA:
                        case MaxDBType.VarCharA:
                        case MaxDBType.CharE:
                        case MaxDBType.StrE:
                        case MaxDBType.VarCharE:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    valLength = Math.Min(strValue.Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.LongA:
                        case MaxDBType.LongE:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    valLength = strValue.Length;
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    valLength = Math.Min(strValue.Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(char);
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
                            }

                            break;
                        case MaxDBType.LongUni:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    valLength = strValue.Length;
                                }
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
                            }

                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                    valLength = Math.Min(((byte[])param.m_inputValue).Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.LongB:
                            if (input_val)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                    valLength = ((byte[])param.m_inputValue).Length;
                                else
                                    valLength = 0;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    valLength = param.Size;
                                else
                                    valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
                            }

                            break;
                        case MaxDBType.Date:
                            if (input_val)
                                valLength = sizeof(ODBCDATE);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.Time:
                            if (input_val)
                                valLength = sizeof(ODBCTIME);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.TimeStamp:
                            if (input_val)
                                valLength = sizeof(ODBCTIMESTAMP);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            if (input_val)
                                valLength = sizeof(double);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i);

                            break;
                        case MaxDBType.Integer:
                            if (input_val)
                                valLength = sizeof(int);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                        case MaxDBType.SmallInt:
                            if (input_val)
                                valLength = sizeof(short);
                            else
                                valLength = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

                            break;
                    }

                    bufferLength += valLength;
                }
            }

            return bufferLength;
        }

        private unsafe void FillInputParameters(IntPtr stmt, IntPtr meta, MaxDBParameterCollection[] parameters,
            ByteArray paramArr, byte* bufferPtr, int* sizePtr, byte** addrPtr, int[][] valLen)
        {
            int paramCount = SQLDBC.SQLDBC_ParameterMetaData_getParameterCount(meta);
            int paramLen = parameters.Length;

            if (paramCount == 0 || paramLen == 0)
                return;

            int bufferOffset = 0;

            for (short i = 0; i < paramCount; i++)
            {
                valLen[i] = new int[paramLen];

                bool isInput = (
                        SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.In ||
                        SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.InOut);

                MaxDBParameter param;
                int paramOffset = 0;

                switch (parameters[0][i].m_dbType)
                {
                    case MaxDBType.Boolean:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    paramArr.WriteByte((byte)((bool)param.m_inputValue ? 1 : 0), bufferOffset + paramOffset);
                                    sizePtr[i * paramLen + k] = sizeof(bool);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                                valLen[i][k] += sizeof(byte);

                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] += sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.CharA:
                    case MaxDBType.StrA:
                    case MaxDBType.VarCharA:
                    case MaxDBType.CharE:
                    case MaxDBType.StrE:
                    case MaxDBType.VarCharE:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    sizePtr[i * paramLen + k] = Math.Min(strValue.Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1))) * sizeof(byte);
                                    paramArr.WriteASCII(strValue, bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);

                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.LongA:
                    case MaxDBType.LongE:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    sizePtr[i * paramLen + k] = strValue.Length;
                                    paramArr.WriteASCII(strValue, bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);

                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Unicode:
                    case MaxDBType.StrUni:
                    case MaxDBType.VarCharUni:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    sizePtr[i * paramLen + k] = Math.Min(strValue.Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1))) * sizeof(char);
                                    paramArr.WriteUnicode(strValue, bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(char);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1),
                            BitConverter.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.LongUni:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    string strValue = param.m_inputValue.GetType() == typeof(char[]) ?
                                        new string((char[])param.m_inputValue) : (string)param.m_inputValue;
                                    sizePtr[i * paramLen + k] = strValue.Length;
                                    paramArr.WriteUnicode(strValue, bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(char);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1),
                            BitConverter.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.CharB:
                    case MaxDBType.StrB:
                    case MaxDBType.VarCharB:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    sizePtr[i * paramLen + k] = Math.Min(((byte[])param.m_inputValue).Length,
                                        SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1))) * sizeof(byte);
                                    paramArr.WriteBytes(((byte[])param.m_inputValue), bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.LongB:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    sizePtr[i * paramLen + k] = ((byte[])param.m_inputValue).Length;
                                    paramArr.WriteBytes(((byte[])param.m_inputValue), bufferOffset + paramOffset);
                                    valLen[i][k] = sizePtr[i * paramLen + k];
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;
                            }
                            else
                            {
                                if (param.Size > 0)
                                    sizePtr[i * paramLen + k] = param.Size;
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Date:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    DateTime dt = (DateTime)param.m_inputValue;
                                    //ODBC date format
                                    ODBCDATE dt_odbc;
                                    dt_odbc.year = (short)(dt.Year % 0x10000);
                                    dt_odbc.month = (ushort)(dt.Month % 0x10000);
                                    dt_odbc.day = (ushort)(dt.Day % 0x10000);

                                    paramArr.WriteBytes(ODBCConverter.GetBytes(dt_odbc), bufferOffset + paramOffset);

                                    sizePtr[i * paramLen + k] = sizeof(ODBCDATE);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(ODBCDATE);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));

                        break;
                    case MaxDBType.Time:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    //ODBC time format
                                    ODBCTIME tm_odbc = new ODBCTIME();

                                    if (param.m_inputValue is DateTime)
                                    {
                                        DateTime tm = (DateTime)param.m_inputValue;

                                        tm_odbc.hour = (ushort)(tm.Hour % 0x10000);
                                        tm_odbc.minute = (ushort)(tm.Minute % 0x10000);
                                        tm_odbc.second = (ushort)(tm.Second % 0x10000);
                                    }
                                    else
                                    {
                                        TimeSpan ts = (TimeSpan)param.m_inputValue;

                                        tm_odbc.hour = (ushort)(ts.Hours % 0x10000);
                                        tm_odbc.minute = (ushort)(ts.Minutes % 0x10000);
                                        tm_odbc.second = (ushort)(ts.Seconds % 0x10000);
                                    }

                                    paramArr.WriteBytes(ODBCConverter.GetBytes(tm_odbc), bufferOffset + paramOffset);

                                    sizePtr[i * paramLen + k] = sizeof(ODBCTIME);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(ODBCTIME);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.TimeStamp:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    DateTime ts = (DateTime)param.m_inputValue;
                                    //ODBC timestamp format
                                    ODBCTIMESTAMP ts_odbc = new ODBCTIMESTAMP();
                                    ts_odbc.year = (short)(ts.Year % 0x10000);
                                    ts_odbc.month = (ushort)(ts.Month % 0x10000);
                                    ts_odbc.day = (ushort)(ts.Day % 0x10000);
                                    ts_odbc.hour = (ushort)(ts.Hour % 0x10000);
                                    ts_odbc.minute = (ushort)(ts.Minute % 0x10000);
                                    ts_odbc.second = (ushort)(ts.Second % 0x10000);
                                    ts_odbc.fraction = (uint)(((ts.Ticks % TimeSpan.TicksPerSecond)
                                        / (TimeSpan.TicksPerMillisecond / 1000)) * 1000);

                                    paramArr.WriteBytes(ODBCConverter.GetBytes(ts_odbc), bufferOffset + paramOffset);
                                    sizePtr[i * paramLen + k] = sizeof(ODBCTIMESTAMP);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(ODBCTIMESTAMP);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
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
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    paramArr.WriteDouble((double)param.m_inputValue, bufferOffset + paramOffset);
                                    sizePtr[i * paramLen + k] = sizeof(double);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(double);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.Integer:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    paramArr.WriteInt32((int)param.m_inputValue, bufferOffset + paramOffset);
                                    sizePtr[i * paramLen + k] = sizeof(int);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(int);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                    case MaxDBType.SmallInt:
                        for (int k = 0; k < paramLen; k++)
                        {
                            param = parameters[k][i];
                            if (isInput)
                            {
                                if (param.m_inputValue != DBNull.Value)
                                {
                                    paramArr.WriteInt16((short)param.m_inputValue, bufferOffset + paramOffset);
                                    sizePtr[i * paramLen + k] = sizeof(short);
                                }
                                else
                                    sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_NULL_DATA;

                                valLen[i][k] = sizeof(short);
                            }
                            else
                            {
                                sizePtr[i * paramLen + k] = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, (short)(i + 1)) * sizeof(byte);
                                valLen[i][k] = sizePtr[i * paramLen + k];
                            }

                            addrPtr[i * paramLen + k] = bufferPtr + bufferOffset + paramOffset;

                            paramOffset += valLen[i][k];
                        }

                        if (SQLDBC.SQLDBC_PreparedStatement_bindParameterAddr(stmt, (short)(i + 1), SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2,
                            addrPtr + i * paramLen, sizePtr + i * paramLen, 0, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK)
                            MaxDBException.ThrowException(
                                MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                                SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
                        break;
                }

                bufferOffset += paramOffset;
            }
        }

        private unsafe void FillOutputParameters(IntPtr meta, MaxDBParameterCollection[] parameters,
            ByteArray paramArr, int[] valSize, int[][] valLen)
        {
            int paramCount = SQLDBC.SQLDBC_ParameterMetaData_getParameterCount(meta);
            int paramLen = parameters.Length;
            int bufferOffset = 0;

            for (short i = 0; i < paramCount; i++)
            {
                bool output_val = (
                    SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.InOut ||
                    SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, (short)(i + 1)) == SQLDBC_ParameterMode.Out);

                for (int k = 0; k < paramLen; k++)
                {
                    MaxDBParameter param = parameters[k][i];
                    int valLength = valLen[i][k];

                    switch (param.m_dbType)
                    {
                        case MaxDBType.Boolean:
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = (paramArr.ReadByte(bufferOffset) == 1);
                                else
                                    param.m_value = DBNull.Value;
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
                            if (output_val)
                            {
                                //??? LONG ASCII parameter contains extra zeros
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadASCII(bufferOffset, valSize[i * paramLen + k]).Replace("\0", string.Empty);
                                else
                                    param.m_value = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Unicode:
                        case MaxDBType.StrUni:
                        case MaxDBType.VarCharUni:
                        case MaxDBType.LongUni:
                            if (output_val)
                            {
                                //??? LONG ASCII parameter contains extra zeros
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadUnicode(bufferOffset, valSize[i * paramLen + k]).Replace("\0", string.Empty);
                                else
                                    param.m_value = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Date:
                            //ODBC date format
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = ODBCConverter.GetDateTime(
                                        ODBCConverter.GetDate(paramArr.ReadBytes(bufferOffset, valSize[i * paramLen + k])));
                                else
                                    param.m_value = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Time:
                            //ODBC time format
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = ODBCConverter.GetTimeSpan(
                                        ODBCConverter.GetTime(paramArr.ReadBytes(bufferOffset, valSize[i * paramLen + k])));
                                else
                                    param.m_value = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.TimeStamp:
                            //ODBC timestamp format
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = ODBCConverter.GetDateTime(
                                        ODBCConverter.GetTimeStamp(paramArr.ReadBytes(bufferOffset, valSize[i * paramLen + k])));
                                else
                                    param.m_value = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Fixed:
                        case MaxDBType.Float:
                        case MaxDBType.VFloat:
                        case MaxDBType.Number:
                        case MaxDBType.NoNumber:
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadDouble(bufferOffset);
                                else
                                    param.m_value = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.Integer:
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadInt32(bufferOffset);
                                else
                                    param.m_value = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.SmallInt:
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadInt16(bufferOffset);
                                else
                                    param.m_value = DBNull.Value;
                            }
                            bufferOffset += valLength;
                            break;
                        case MaxDBType.CharB:
                        case MaxDBType.StrB:
                        case MaxDBType.VarCharB:
                        case MaxDBType.LongB:
                        default:
                            if (output_val)
                            {
                                if (valSize[i * paramLen + k] != SQLDBC.SQLDBC_NULL_DATA)
                                    param.m_value = paramArr.ReadBytes(bufferOffset, valSize[i * paramLen + k]);
                                else
                                    param.m_value = DBNull.Value;
                            }

                            bufferOffset += valLength;
                            break;
                    }
                }
            }
        }

        private unsafe void BindAndExecute(IntPtr stmt, MaxDBParameterCollection[] parameters)
        {
            IntPtr meta = SQLDBC.SQLDBC_PreparedStatement_getParameterMetaData(stmt);
            if (meta == IntPtr.Zero)
                return;

            int paramCount = SQLDBC.SQLDBC_ParameterMetaData_getParameterCount(meta);
 
            // +1 byte to avoid zero-length array
            ByteArray paramArr = new ByteArray(EvaluateBufferLegth(meta, parameters) + 1, BitConverter.IsLittleEndian);

            SQLDBC.SQLDBC_PreparedStatement_setBatchSize(stmt, (uint)parameters.Length);
                        
            int[] valSize = new int[paramCount * parameters.Length];
            int[][] valLen = new int[paramCount][];
            byte*[] valAddr = new byte*[paramCount * parameters.Length];

            SQLDBC_Retcode rc;

			if (paramCount > 0)
				fixed (int* sizePtr = valSize)
				fixed (byte** addrPtr = valAddr)
				fixed (byte* bufferPtr = paramArr.arrayData)
				{
					FillInputParameters(stmt, meta, parameters, paramArr, bufferPtr, sizePtr, addrPtr, valLen);

					rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

					if (rc == SQLDBC_Retcode.SQLDBC_OK)
						FillOutputParameters(meta, parameters, paramArr, valSize, valLen);
				}
			else
				rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

            if (rc == SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
                throw new MaxDBException(
                    MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " +
                    MaxDBMessages.Extract(MaxDBMessages.ERROR_PARAM_TRUNC));

            if (rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND)
                MaxDBException.ThrowException(
                    MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                    SQLDBC.SQLDBC_PreparedStatement_getError(stmt));
        }

		internal unsafe string UpdTableName
		{
			get
			{
				if (m_connection.m_tableNames[CommandText] == null)
				{
					byte[] buffer = new byte[1];
					int bufferSize = 0;
					SQLDBC_Retcode rc;

					fixed(byte* bufferPtr = buffer)
					{
						rc = SQLDBC.SQLDBC_Statement_getTableName(m_stmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, &bufferSize);
						if(rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC) 
							MaxDBException.ThrowException(
								MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
								SQLDBC.SQLDBC_Statement_getError(m_stmt));
					}

					bufferSize++;//increase buffer for the last zero

					buffer = new byte[bufferSize];
					fixed(byte* bufferPtr = buffer)
					{
						rc = SQLDBC.SQLDBC_Statement_getTableName(m_stmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, &bufferSize);
						if(rc != SQLDBC_Retcode.SQLDBC_OK) 
							MaxDBException.ThrowException(
								MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
								SQLDBC.SQLDBC_Statement_getError(m_stmt));
					}

					m_connection.m_tableNames[CommandText] = bufferSize > 1 ? Encoding.ASCII.GetString(buffer, 0, bufferSize - 1) : null;//skip last zero
				}

				return (string)m_connection.m_tableNames[CommandText];
			}
		}

		private void RefreshStmtHandler()
		{
			if (m_stmt != IntPtr.Zero)
				SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.m_connHandler, m_stmt);
			if (m_connection != null)
				m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
		}

		#endregion
#endif // SAFE

        #region ICloneable Members

        public object Clone()
		{
			MaxDBCommand clone = new MaxDBCommand(m_sCmdText, m_connection, m_txn);
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
			m_canceled = true;
			m_connection.Cancel(this);
#else
			SQLDBC.SQLDBC_Connection_cancel(m_connection.m_connHandler);
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
				return m_sCmdText;  
			}
			set  
			{ 
				m_sCmdText = value;
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
				return m_sCmdType; 
			}
			set 
			{ 
				m_sCmdType = value; 
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
				return m_connection;  
			}
			set
			{
				if (m_connection != value)
					Transaction = null;

#if !SAFE
				if (m_stmt != IntPtr.Zero)
					SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.m_connHandler, m_stmt);
#endif // !SAFE

				m_connection = value;

#if !SAFE
				if (m_connection != null)
					m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
#endif // !SAFE
			}
		}

#if NET20
		protected override DbParameter CreateDbParameter()
#else
		IDbDataParameter IDbCommand.CreateParameter()
#endif // NET20
        {
            return this.CreateParameter();
        }

#if NET20
        public new MaxDBParameter CreateParameter()
#else
		public MaxDBParameter CreateParameter()
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

#if SAFE
			// There must be a valid and open connection.
			AssertOpen();

			m_parseInfo = DoParse(m_sCmdText, false);

			if (m_parseInfo != null && m_parseInfo.m_isSelect) 
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_RESULTSET));
			else 
			{
				Execute(CommandBehavior.Default);
				if(m_hasRowCount) 
					return m_rowsAffected;
				else 
					return 0;
			}
#else
			string sql = m_sCmdText;

			if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
				sql = "CALL " + sql;

			if (m_sCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

			try
			{
                PrepareNTS(sql);

                BindAndExecute(m_stmt, new MaxDBParameterCollection[1] { m_parameters });
			}
			catch
			{
				throw;
			}

			return SQLDBC.SQLDBC_PreparedStatement_getRowsAffected(m_stmt);
#endif // SAFE
        }

#if !SAFE
        private unsafe void PrepareNTS(string sql)
        {
            SQLDBC_Retcode rc;

            if (m_connection.DatabaseEncoding is UnicodeEncoding)
                fixed(byte* bytePtr = m_connection.DatabaseEncoding.GetBytes(sql))
                rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(m_stmt, bytePtr, SQLDBC_StringEncodingType.UCS2Swapped);
            else
                rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(m_stmt, sql);

            if (rc != SQLDBC_Retcode.SQLDBC_OK)
                MaxDBException.ThrowException(
                    MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED),
                    SQLDBC.SQLDBC_PreparedStatement_getError(m_stmt));
        }
#endif

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

#if SAFE
			// There must be a valid and open connection.
			AssertOpen();

			m_parseInfo = DoParse(m_sCmdText, false);

			Execute(behavior);

			if (m_parseInfo.m_isSelect)
			{
				if (m_currentDataReader == null)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLCOMMAND_NORESULTSET));

				m_currentDataReader.m_fCloseConn = ((behavior & CommandBehavior.CloseConnection) != 0);
				m_currentDataReader.m_fSchemaOnly = ((behavior & CommandBehavior.SchemaOnly) != 0);
				return m_currentDataReader;
			}
			else
				return new MaxDBDataReader(this);
#else
			string sql = m_sCmdText;

			if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
				sql = "CALL " + sql;

			if (m_sCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

			try
			{
                PrepareNTS(sql);

				if ((behavior & CommandBehavior.SingleRow) != 0 || (behavior & CommandBehavior.SchemaOnly) != 0)
					SQLDBC.SQLDBC_Statement_setMaxRows(m_stmt, 1);
				else
					SQLDBC.SQLDBC_Statement_setMaxRows(m_stmt, 0);

                BindAndExecute(m_stmt, new MaxDBParameterCollection[1] { m_parameters });

				m_rowsAffected = SQLDBC.SQLDBC_PreparedStatement_getRowsAffected(m_stmt);

				if (SQLDBC.SQLDBC_Statement_isQuery(m_stmt) == SQLDBC_BOOL.SQLDBC_TRUE)
				{
					IntPtr result = SQLDBC.SQLDBC_PreparedStatement_getResultSet(m_stmt);
					if(result == IntPtr.Zero) 
						MaxDBException.ThrowException(
							MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLCOMMAND_NORESULTSET),
							SQLDBC.SQLDBC_Statement_getError(m_stmt));

					return new MaxDBDataReader(result, m_connection, this,
						(behavior & CommandBehavior.CloseConnection) != 0,
						(behavior & CommandBehavior.SchemaOnly) != 0
						);
				}
				else
					return new MaxDBDataReader(this);
			}
			catch
			{
				throw;
			}
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
				return m_parameters; 
			}
		}

#if NET20
        public override void Prepare()
#else
		public void Prepare()
#endif // NET20
        {
			// The Prepare is a no-op since parameter preparing and query execution
			// has to belong to the common block of "fixed" code
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
				return m_txn; 
			}
			set 
			{ 
				m_txn = value; 
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
				return m_updatedRowSource;  
			}
			set 
			{ 
				m_updatedRowSource = value; 
			}
		}

		#endregion

		#region IDisposable Members

        void IDisposable.Dispose()
        {
#if SAFE
            m_replyMem = null;
			if (m_connection != null && (m_parseInfo != null && !m_parseInfo.IsCached)) 
			{
				m_connection.DropParseID(m_parseInfo.ParseID);
				m_parseInfo.SetParseIDAndSession(null, -1);
				m_connection.DropParseID(m_parseInfo.MassParseID);
				m_parseInfo.MassParseID = null;
				m_connection = null;
			}
#else
			if (m_stmt != IntPtr.Zero)
				SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.m_connHandler, m_stmt);
#endif // SAFE
            GC.SuppressFinalize(this);
		}

		#endregion
	}
}
