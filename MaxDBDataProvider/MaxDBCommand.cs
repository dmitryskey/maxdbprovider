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
using System.Text;
using System.Collections;
using System.ComponentModel;
using MaxDBDataProvider.MaxDBProtocol;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Represents a SQL statement to execute against a MaxDB database. This class cannot be inherited.
	/// </summary>
	sealed public class MaxDBCommand : IDbCommand, ICloneable, IDisposable
#if SAFE 
		, ISQLParamController 
#endif
	{
		MaxDBConnection  m_connection;
		MaxDBTransaction  m_txn;
		string m_sCmdText;
		UpdateRowSource m_updatedRowSource = UpdateRowSource.None;
		MaxDBParameterCollection m_parameters = new MaxDBParameterCollection();
		CommandType m_sCmdType = CommandType.Text;
		internal int m_rowsAffected = -1;

#if SAFE
		#region "Native implementation parameters"

		private ArrayList m_inputProcedureLongs;
		private bool m_setWithInfo = false;
		private bool m_hasRowCount;
		private static int m_maxParseAgainCnt = 10;
		private ArrayList m_inputLongs;
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

		IntPtr m_stmt = IntPtr.Zero;
		
		#endregion
#endif

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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_TOOLONG), "42000");
			}

			return replyPacket;
		}

		private void AssertOpen() 
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

				//>>> SQL TRACE
				if (m_connection.m_logger.TraceSQL && m_parseInfo.m_inputCount > 0)
				{
					m_connection.m_logger.SqlTrace(dt, "INPUT PARAMETERS:");
					m_connection.m_logger.SqlTrace(dt, "APPLICATION");
					m_connection.m_logger.SqlTraceDataHeader(dt);
				}
				//<<< SQL TRACE
				
				// We must add a data part if we have input parameters or even if we have output streams.
				for(int i = 0; i < m_parseInfo.ParamInfo.Length; i++)
				{
					MaxDBParameter param = m_parameters[i];

					if (!FindColInfo(i).IsInput)
						continue;

					//>>> SQL TRACE
					if (m_connection.m_logger.TraceSQL)
					{
						string s_out = (i + 1).ToString().PadRight(MaxDBLogger.NumSize);
						s_out += m_parseInfo.ParamInfo[i].ColumnTypeName.PadRight(MaxDBLogger.TypeSize);

						switch(param.m_dbType)
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
						switch(param.m_dbType)
						{
							case MaxDBType.Boolean:
								m_inputArgs[i] = FindColInfo(i).TransBooleanForInput((bool)param.m_inputValue);
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
									m_connection.SQLMode == SqlMode.Oracle && param.m_inputValue.ToString() == string.Empty ) 
									// in ORACLE mode a null values will be inserted if the string value is equal to "" 
									m_inputArgs[i] = null;
								else
								{
									if (param.m_inputValue.GetType() == typeof(char[]))
										m_inputArgs[i] = FindColInfo(i).TransStringForInput(new string((char[])param.m_inputValue));
									else
										m_inputArgs[i] = FindColInfo(i).TransStringForInput((string)param.m_inputValue);
								}
								break;
							case MaxDBType.Date:
							case MaxDBType.TimeStamp:
								m_inputArgs[i] = FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue);
								break;
							case MaxDBType.Time:
								if (param.m_inputValue is DateTime)
									m_inputArgs[i] = FindColInfo(i).TransDateTimeForInput((DateTime)param.m_inputValue);
								else
									m_inputArgs[i] = FindColInfo(i).TransTimeSpanForInput((TimeSpan)param.m_inputValue);
								break;
							case MaxDBType.Fixed: 
							case MaxDBType.Float: 
							case MaxDBType.VFloat:
							case MaxDBType.Number:
							case MaxDBType.NoNumber:
								m_inputArgs[i] = FindColInfo(i).TransDoubleForInput((double)param.m_inputValue);
								break;
							case MaxDBType.Integer:
								m_inputArgs[i] = FindColInfo(i).TransInt64ForInput((int)param.m_inputValue);
								break;
							case MaxDBType.SmallInt:
								m_inputArgs[i] = FindColInfo(i).TransInt16ForInput((short)param.m_inputValue);
								break;
							case MaxDBType.CharB: 
							case MaxDBType.StrB: 
							case MaxDBType.VarCharB:
							case MaxDBType.LongB:
							default:
								m_inputArgs[i] = FindColInfo(i).TransBytesForInput((byte[])param.m_inputValue);
								break;
						}
					}
					else
						m_inputArgs[i] = null;
				}

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
					if (ex.InnerException != null && 
						ex.InnerException is MaxDBSQLException && 
						((MaxDBSQLException)ex.InnerException).VendorCode == -8 && 
						afterParseAgain > 0) 
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
					for(int i = 0; i < m_parseInfo.ParamInfo.Length; i++)
					{
						MaxDBParameter param = m_parameters[i];
						if (!FindColInfo(i).IsOutput)
							continue;

						if (FindColInfo(i).IsDBNull(m_replyMem))
						{
							param.m_value = DBNull.Value;
							continue;
						}

						switch(param.m_dbType)
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

							switch(param.m_dbType)
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
			catch (MaxDBSQLException sqlExc) 
			{
				if (sqlExc.VendorCode == -4000) 
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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_NULL), "42000");

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

		private void ClearParameters()
		{
			for (int i = 0; i < m_inputArgs.Length; ++i) 
				m_inputArgs [i] = m_initialParamValue;
		}

		private void ResetPutValues(ArrayList inpLongs)
		{
			if (inpLongs != null) 
				foreach(PutValue putval in inpLongs)
					putval.Reset();
		}

		private void HandleStreamsForExecute(DataPart dataPart, object[] args)
		{
			// get all putval objects
			m_inputLongs = new ArrayList();
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
					throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STREAM_ISATEND));  
				putval.TransferStream(dataPart, i);
			}
		}

		private void HandleProcedureStreamsForExecute(DataPart dataPart, object[] objects)
		{
			m_inputProcedureLongs = new ArrayList();
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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_STATEMENT_CANCELLED), "42000", -102);
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

		private unsafe void BindAndExecute(IntPtr stmt, MaxDBParameterCollection parameters)
		{
			int buffer_length = 0;
			int buffer_offset = 0;

			IntPtr meta = SQLDBC.SQLDBC_PreparedStatement_getParameterMetaData(stmt);
			if (meta == IntPtr.Zero)
				return;

			int paramCount = SQLDBC.SQLDBC_ParameterMetaData_getParameterCount(meta);

			for(short i = 1; i <= paramCount; i++)
			{
				MaxDBParameter param = parameters[i - 1];

				int val_length = 0;
					
				bool input_val = (
					SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.In || 
					SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.InOut);
					
				switch(param.m_dbType)
				{
					case MaxDBType.Boolean:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(byte);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

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
								string strValue = param.m_inputValue.GetType() == typeof(char[])?
									new string((char[])param.m_inputValue):(string)param.m_inputValue;
								val_length = Math.Min(strValue.Length, 
									SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
							}
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
						}
							
						break;
					case MaxDBType.LongA:
					case MaxDBType.LongE:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
							{
								string strValue = param.m_inputValue.GetType() == typeof(char[])?
									new string((char[])param.m_inputValue):(string)param.m_inputValue;
								val_length = strValue.Length;
							}
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
						}
							
						break;
					case MaxDBType.Unicode: 
					case MaxDBType.StrUni: 
					case MaxDBType.VarCharUni:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
							{
								string strValue = param.m_inputValue.GetType() == typeof(char[])?
									new string((char[])param.m_inputValue):(string)param.m_inputValue;
								val_length = Math.Min(strValue.Length , 
									SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(char);
							}
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
						}

						break;
					case MaxDBType.LongUni:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
							{
								string strValue = param.m_inputValue.GetType() == typeof(char[])?
									new string((char[])param.m_inputValue):(string)param.m_inputValue;
								val_length = strValue.Length;
							}
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
						}

						break;
					case MaxDBType.CharB: 
					case MaxDBType.StrB: 
					case MaxDBType.VarCharB:
						if (input_val) 
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = Math.Min(((byte[])param.m_inputValue).Length , 
									SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
						}
							
						break;
					case MaxDBType.LongB:
						if (input_val) 
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = ((byte[])param.m_inputValue).Length;
							else
								val_length = 0;
						}
						else
						{
							if (param.Size > 0)
								val_length = param.Size;
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
						}
							
						break;
					case MaxDBType.Date:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(ODBCDATE);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
							
						break;
					case MaxDBType.Time:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(ODBCTIME);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

						break;
					case MaxDBType.TimeStamp:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(ODBCTIMESTAMP);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
							
						break;
					case MaxDBType.Fixed: 
					case MaxDBType.Float: 
					case MaxDBType.VFloat:
					case MaxDBType.Number:
					case MaxDBType.NoNumber:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(double);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i);

						break;
					case MaxDBType.Integer:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(int);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

						break;
					case MaxDBType.SmallInt:
						if (input_val)
						{
							if (param.m_inputValue != DBNull.Value)
								val_length = sizeof(short);
							else
								val_length = 0;
						}
						else
							val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
							
						break;
				}

				buffer_length += val_length;
			}

			// +1 byte to avoid zero-length array
			ByteArray paramArr = new ByteArray(buffer_length + 1, BitConverter.IsLittleEndian);

			fixed(byte *buffer_ptr = paramArr.arrayData)
			{
				int[] val_size = new int[parameters.Count];
				int[] val_lens = new int[parameters.Count];
				for(short i = 1; i <= paramCount; i++)
				{
					MaxDBParameter param = parameters[i - 1];

					int val_length = 0; 
					
					bool input_val = (
						SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.In || 
						SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.InOut);
					
					switch(param.m_dbType)
					{
						case MaxDBType.Boolean:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									paramArr.WriteByte((byte)((bool)param.m_inputValue ? 1 : 0), buffer_offset);
									val_length = sizeof(byte);
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], sizeof(byte), SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
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
									string strValue = param.m_inputValue.GetType() == typeof(char[])?
										new string((char[])param.m_inputValue):(string)param.m_inputValue;
									val_length = Math.Min(strValue.Length, 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
									paramArr.WriteASCII(strValue, buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.LongA:
						case MaxDBType.LongE:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									string strValue = param.m_inputValue.GetType() == typeof(char[])?
										new string((char[])param.m_inputValue):(string)param.m_inputValue;
									val_length = strValue.Length;
									paramArr.WriteASCII(strValue, buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Unicode: 
						case MaxDBType.StrUni: 
						case MaxDBType.VarCharUni:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									string strValue = param.m_inputValue.GetType() == typeof(char[])?
										new string((char[])param.m_inputValue):(string)param.m_inputValue;
									val_length = Math.Min(strValue.Length , 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(char);
									paramArr.WriteUnicode(strValue, buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, 
								BitConverter.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.LongUni:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									string strValue = param.m_inputValue.GetType() == typeof(char[])?
										new string((char[])param.m_inputValue):(string)param.m_inputValue;
									val_length = strValue.Length;
									paramArr.WriteUnicode(strValue, buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, 
								BitConverter.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.CharB: 
						case MaxDBType.StrB: 
						case MaxDBType.VarCharB:
							if (input_val) 
							{
								if (param.m_inputValue != DBNull.Value)
								{
									val_length = Math.Min(((byte[])param.m_inputValue).Length , 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
									paramArr.WriteBytes(((byte[])param.m_inputValue), buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.LongB:
							if (input_val) 
							{
								if (param.m_inputValue != DBNull.Value)
								{
									val_length = ((byte[])param.m_inputValue).Length;
									paramArr.WriteBytes(((byte[])param.m_inputValue), buffer_offset, val_length);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								if (param.Size > 0)
									val_length = param.Size;
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Date:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									DateTime dt = (DateTime)param.m_inputValue;
									//ODBC date format
									ODBCDATE dt_odbc;
									dt_odbc.year = (short)(dt.Year % 0x10000);
									dt_odbc.month = (ushort)(dt.Month % 0x10000);
									dt_odbc.day = (ushort)(dt.Day % 0x10000);
								
									paramArr.WriteBytes(ODBCConverter.GetBytes(dt_odbc), buffer_offset);
 
									val_length = sizeof(ODBCDATE);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							
							break;
						case MaxDBType.Time:
							if (input_val)
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
							
									paramArr.WriteBytes(ODBCConverter.GetBytes(tm_odbc), buffer_offset);

									val_length = sizeof(ODBCTIME);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.TimeStamp:
							if (input_val)
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
									ts_odbc.fraction = (uint) (((ts.Ticks % TimeSpan.TicksPerSecond) 
										/ (TimeSpan.TicksPerMillisecond / 1000)) * 1000);

									paramArr.WriteBytes(ODBCConverter.GetBytes(ts_odbc), buffer_offset);
									val_length = sizeof(ODBCTIMESTAMP);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Fixed: 
						case MaxDBType.Float: 
						case MaxDBType.VFloat:
						case MaxDBType.Number:
						case MaxDBType.NoNumber:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									paramArr.WriteDouble((double)param.m_inputValue, buffer_offset);
									val_length = sizeof(double);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Integer:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									paramArr.WriteInt32((int)param.m_inputValue, buffer_offset);
									val_length = sizeof(int);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.SmallInt:
							if (input_val)
							{
								if (param.m_inputValue != DBNull.Value)
								{
									paramArr.WriteInt16((short)param.m_inputValue, buffer_offset);
									val_length = sizeof(short);
									val_size[i - 1] = val_length;
								}
								else
								{
									val_length = 0;
									val_size[i - 1] = SQLDBC.SQLDBC_NULL_DATA;
								}
							}
							else
							{
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
								val_size[i - 1] = val_length;
							}
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
					}

					buffer_offset += val_length;
					val_lens[i - 1] = val_length;
				}

				SQLDBC_Retcode rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

				buffer_offset = 0;

				if (rc == SQLDBC_Retcode.SQLDBC_OK)
					for(short i = 1; i <= paramCount; i++)
					{
						MaxDBParameter param = parameters[i - 1];

						int val_length = val_lens[i - 1];
						bool output_val = (
							SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.InOut || 
							SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.Out);
											
						switch(param.m_dbType)
						{
							case MaxDBType.Boolean:
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = (paramArr.ReadByte(buffer_offset) == 1);
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
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
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadASCII(buffer_offset, val_size[i - 1]).Replace("\0", string.Empty);
									else
										param.m_value = DBNull.Value;
								}

								buffer_offset += val_length;
								break;
							case MaxDBType.Unicode: 
							case MaxDBType.StrUni: 
							case MaxDBType.VarCharUni:
							case MaxDBType.LongUni:
								if (output_val)
								{
									//??? LONG ASCII parameter contains extra zeros
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadUnicode(buffer_offset, val_size[i - 1]).Replace("\0", string.Empty);
									else
										param.m_value = DBNull.Value;
								}								

								buffer_offset += val_length;
								break;
							case MaxDBType.CharB: 
							case MaxDBType.StrB: 
							case MaxDBType.VarCharB:
							case MaxDBType.LongB:
							default:
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadBytes(buffer_offset, val_size[i - 1]);
									else
										param.m_value = DBNull.Value;
								}

								buffer_offset += val_length;
								break;
							case MaxDBType.Date:
								//ODBC date format
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = ODBCConverter.GetDateTime(
											ODBCConverter.GetDate(paramArr.ReadBytes(buffer_offset, val_size[i - 1])));
									else
										param.m_value = DBNull.Value;
								}

								buffer_offset += val_length;
								break;
							case MaxDBType.Time:
								//ODBC time format
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = ODBCConverter.GetTimeSpan(
											ODBCConverter.GetTime(paramArr.ReadBytes(buffer_offset, val_size[i - 1])));
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
								break;
							case MaxDBType.TimeStamp:
								//ODBC timestamp format
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = ODBCConverter.GetDateTime(
											ODBCConverter.GetTimeStamp(paramArr.ReadBytes(buffer_offset, val_size[i - 1])));
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
								break;
							case MaxDBType.Fixed: 
							case MaxDBType.Float: 
							case MaxDBType.VFloat:
							case MaxDBType.Number:
							case MaxDBType.NoNumber:
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadDouble(buffer_offset);
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
								break;
							case MaxDBType.Integer:
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadInt32(buffer_offset);
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
								break;
							case MaxDBType.SmallInt:
								if (output_val)
								{
									if (val_size[i - 1] != SQLDBC.SQLDBC_NULL_DATA)
										param.m_value = paramArr.ReadInt16(buffer_offset);
									else
										param.m_value = DBNull.Value;
								}
								buffer_offset += val_length;
								break;
						}
						buffer_offset += val_length;
					}

				if (rc == SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
						MaxDBMessages.Extract(MaxDBMessages.ERROR_PARAM_TRUNC));

				if(rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND) 
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
						SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
			}
		}

		internal unsafe string UpdTableName
		{
			get
			{
				byte[] buffer = new byte[1];
				int bufferSize = 0;
				SQLDBC_Retcode rc;

				fixed(byte* bufferPtr = buffer)
				{
					rc = SQLDBC.SQLDBC_Statement_getTableName(m_stmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, ref bufferSize);
					if(rc != SQLDBC_Retcode.SQLDBC_DATA_TRUNC) 
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_Statement_getError(m_stmt)));
				}

				bufferSize++;//increase buffer for the last zero

				buffer = new byte[bufferSize];
				fixed(byte* bufferPtr = buffer)
				{
					rc = SQLDBC.SQLDBC_Statement_getTableName(m_stmt, (IntPtr)bufferPtr, SQLDBC_StringEncodingType.Ascii, bufferSize, ref bufferSize);
					if(rc != SQLDBC_Retcode.SQLDBC_OK) 
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
							SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_Statement_getError(m_stmt)));
				}

				return bufferSize > 1 ? Encoding.ASCII.GetString(buffer, 0, bufferSize - 1) : null;//skip last zero
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
#endif

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

		public void Cancel()
		{
#if SAFE
			AssertOpen ();
			m_canceled = true;
			m_connection.Cancel(this);
#else
			SQLDBC.SQLDBC_Connection_cancel(m_connection.m_connHandler);
#endif
		}

		public string CommandText
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

		public int CommandTimeout
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

		public CommandType CommandType
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

		public MaxDBConnection Connection
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
#endif

				m_connection = value;

#if !SAFE
				if (m_connection != null)
					m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
#endif
			}
		}

		IDbConnection IDbCommand.Connection
		{
			get
			{
				return Connection;
			}
			set
			{
				Connection = (MaxDBConnection)value;
			}
		}

		public MaxDBParameter CreateParameter()
		{
			return new MaxDBParameter();
		}

		IDbDataParameter IDbCommand.CreateParameter()
		{
			return (IDbDataParameter)CreateParameter();
		}

		public int ExecuteNonQuery()
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
			SQLDBC_Retcode rc;

			string sql = m_sCmdText;

			if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
				sql = "CALL " + sql;

			if (m_sCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

			try
			{
				if (m_connection.DatabaseEncoding is UnicodeEncoding)
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(m_stmt, m_connection.DatabaseEncoding.GetBytes(sql), 
						SQLDBC_StringEncodingType.UCS2Swapped);
				else
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(m_stmt, sql);
			
				if(rc != SQLDBC_Retcode.SQLDBC_OK) 
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
						SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(m_stmt)));

				BindAndExecute(m_stmt, m_parameters);
			}
			catch
			{
				throw;
			}

			return SQLDBC.SQLDBC_PreparedStatement_getRowsAffected(m_stmt);
#endif
		}

		int IDbCommand.ExecuteNonQuery()
		{
			return ExecuteNonQuery();
		}

		public MaxDBDataReader ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		IDataReader IDbCommand.ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		public MaxDBDataReader ExecuteReader(CommandBehavior behavior)
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
			SQLDBC_Retcode rc;

			string sql = m_sCmdText;

			if (m_sCmdType == CommandType.StoredProcedure && !sql.Trim().ToUpper().StartsWith("CALL"))
				sql = "CALL " + sql;

			if (m_sCmdType == CommandType.TableDirect)
				throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_TABLEDIRECT_UNSUPPORTED));

			try
			{
				if (m_connection.DatabaseEncoding is UnicodeEncoding)
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(m_stmt, m_connection.DatabaseEncoding.GetBytes(sql), 
						SQLDBC_StringEncodingType.UCS2Swapped);
				else
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(m_stmt, sql);
			
				if(rc != SQLDBC_Retcode.SQLDBC_OK) 
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
						SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(m_stmt)));

				if ((behavior & CommandBehavior.SingleRow) != 0 || (behavior & CommandBehavior.SchemaOnly) != 0)
					SQLDBC.SQLDBC_Statement_setMaxRows(m_stmt, 1);
				else
					SQLDBC.SQLDBC_Statement_setMaxRows(m_stmt, 0);

				BindAndExecute(m_stmt, m_parameters);

				m_rowsAffected = SQLDBC.SQLDBC_PreparedStatement_getRowsAffected(m_stmt);

				if (SQLDBC.SQLDBC_Statement_isQuery(m_stmt) == SQLDBC_BOOL.SQLDBC_TRUE)
				{
					IntPtr result = SQLDBC.SQLDBC_PreparedStatement_getResultSet(m_stmt);
					if(result == IntPtr.Zero) 
						throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLCOMMAND_NORESULTSET) + " " 
							+ SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(m_stmt)));

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
#endif
		}

		IDataReader IDbCommand.ExecuteReader(CommandBehavior behavior)
		{
			return ExecuteReader(behavior);
		}

		public object ExecuteScalar()
		{
			IDataReader result = ExecuteReader(CommandBehavior.SingleResult);
			if (result.FieldCount > 0 && result.Read())
				return result.GetValue(0);
			else
				return null;
		}

		object IDbCommand.ExecuteScalar()
		{
			return ExecuteScalar();
		}

		public MaxDBParameterCollection Parameters
		{
			get  
			{ 
				return m_parameters; 
			}
		}

		IDataParameterCollection IDbCommand.Parameters
		{
			get  
			{ 
				return m_parameters; 
			}
		}

		void IDbCommand.Prepare()
		{
			// The Prepare is a no-op since parameter preparing and query execution
			// has to belong to the common block of "fixed" code
		}

		public MaxDBTransaction Transaction
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

		IDbTransaction IDbCommand.Transaction
		{
			get
			{
				return Transaction;
			}
			set
			{
				Transaction = (MaxDBTransaction)value;
			}
		}

		public UpdateRowSource UpdatedRowSource
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

		UpdateRowSource IDbCommand.UpdatedRowSource
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

		public void Dispose()
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
#endif
			GC.SuppressFinalize(this);
		}

		#endregion
	}
}
