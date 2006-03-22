using System;
using System.Data;
using System.Text;
using System.Collections;
using System.ComponentModel;
using MaxDBDataProvider.MaxDBProtocol;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	public class MaxDBCommand : IDbCommand
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

#if SAFE
		#region "Native implementation parameters"

		private ArrayList m_inputProcedureLongs;
		private int m_rowsAffected = -1;
		private bool m_setWithInfo = false;
		private bool m_hasRowCount;
		private static int m_maxParseAgainCnt = 10;
		private ArrayList m_inputLongs;
		private MaxDBParseInfo m_parseInfo;
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

		// Implement the default constructor here.
		public MaxDBCommand()
		{
#if !SAFE
			m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
#endif
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
			m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
#endif
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction txn) : this(cmdText, connection)
		{
			m_txn      = txn;
#if !SAFE
			m_stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.m_connHandler);
#endif
		}

		/****
		 * IMPLEMENT THE REQUIRED PROPERTIES.
		 ****/
		public string CommandText
		{
			get 
			{ 
				return m_sCmdText;  
			}
			set  
			{ 
				m_sCmdText = value;  
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

		public IDbConnection Connection
		{
			/*
			 * The user should be able to set or change the connection at any time.
			 */
			get 
			{ 
				return m_connection;  
			}
			set
			{
				/*
				 * The connection is associated with the transaction
				 * so set the transaction object to return a null reference if the connection 
				 * is reset.
				 */
				if (m_connection != value)
					this.Transaction = null;

				m_connection = (MaxDBConnection)value;
			}
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

		public IDbTransaction Transaction
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
				m_txn = (MaxDBTransaction)value; 
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

		/****
		 * IMPLEMENT THE REQUIRED METHODS.
		 ****/
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

		public IDbDataParameter CreateParameter()
		{
			return (IDbDataParameter)(new MaxDBParameter());
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
				throw new MaxDBSQLException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SQLSTATEMENT_RESULTSET));
			else 
			{
				Execute();
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

		IDataReader IDbCommand.ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		public MaxDBDataReader ExecuteReader()
		{
			return (MaxDBDataReader)ExecuteReader(CommandBehavior.Default);
		}

		public IDataReader ExecuteReader(CommandBehavior behavior)
		{
			// Execute the command.

#if SAFE
			// There must be a valid and open connection.
			AssertOpen();

			m_parseInfo = DoParse(m_sCmdText, false);

			Execute();
			return m_currentDataReader;
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

				/*
				* Check if the SQL command return a resultset and get a result set object.
				*/  
				IntPtr result = SQLDBC.SQLDBC_PreparedStatement_getResultSet(m_stmt);
				if(result == IntPtr.Zero) 
					throw new Exception("SQL command doesn't return a result set " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
						SQLDBC.SQLDBC_PreparedStatement_getError(m_stmt)));

				return new MaxDBDataReader(result);				
			}
			catch
			{
				throw;
			}
#endif
		}

		public object ExecuteScalar()
		{
			IDataReader result = ExecuteReader(CommandBehavior.SingleResult);
			if (result.FieldCount > 0 && result.Read())
				return result.GetValue(0);
			else
				return null;
		}

		void IDbCommand.Prepare()
		{
			// The Prepare is a no-op since parameter preparing and query execution
			// has to be in the single "fixed" block of code
		}

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
#endif
			System.GC.SuppressFinalize(this);
		}

#if SAFE
		#region "Methods to support native protocol"

		private MaxDBReplyPacket SendCommand(MaxDBRequestPacket requestPacket, string sqlCmd, int gcFlags, bool parseAgain)
		{
			MaxDBReplyPacket replyPacket;
			requestPacket.InitParseCommand(sqlCmd, true, parseAgain);
			if (m_setWithInfo)
				requestPacket.SetWithInfo();
			replyPacket = m_connection.Exec(requestPacket, false, true, this, gcFlags);
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
				throw new ObjectIsClosedException();
		}

		private void Reparse()
		{
			object[] tmpArgs = m_inputArgs;
			DoParse(m_parseInfo.m_sqlCmd, true);
			m_inputArgs = tmpArgs;
		}

		public bool Execute()
		{
			AssertOpen();
			m_cursorName = m_connection.NextCursorName;

			return Execute(m_maxParseAgainCnt);
		}

		public bool Execute(int afterParseAgain)
		{
			if (m_connection == null) 
				throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_CONNECTIONNULL));

			MaxDBRequestPacket requestPacket;
			MaxDBReplyPacket replyPacket;
			bool isQuery;
			DataPart dataPart;

			// if this is one of the statements that is executed during parse instead of execution, execute it
			// by doing a reparse
			if(m_parseInfo.IsAlreadyExecuted) 
			{
				m_replyMem = null;
				if (m_connection == null) 
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_CONNECTIONNULL));
				Reparse();
				m_rowsAffected=0;
				return false;
			}

			if (!m_parseInfo.IsValid) 
				Reparse();

			try 
			{
				m_canceled = false;
				// check if a reparse is needed.
				if (!m_parseInfo.IsValid) 
					Reparse();
				
				m_replyMem = null;

				requestPacket = m_connection.GetRequestPacket();
				requestPacket.InitExecute(m_parseInfo.ParseID, m_connection.AutoCommit);
				if (m_parseInfo.m_isSelect) 
					requestPacket.AddCursorPart(m_cursorName);
				
				// We must add a data part if we have input parameters or even if we have output streams.
				for(int i = 0; i < m_parseInfo.m_paramInfos.Length; i++)
				{
					MaxDBParameter param = m_parameters[i];
					
					if (!FindColInfo(i).IsInput)
						continue;
					
					switch(param.m_dbType)
					{
						case MaxDBType.Boolean:
							m_inputArgs[i] = FindColInfo(i).TransBooleanForInput(bool.Parse(param.Value.ToString()));
							break;
						case MaxDBType.CharA: 
						case MaxDBType.StrA: 
						case MaxDBType.VarCharA:
						case MaxDBType.CharE: 
						case MaxDBType.StrE: 
						case MaxDBType.VarCharE:
						case MaxDBType.Unicode: 
						case MaxDBType.StrUni: 
						case MaxDBType.VarCharUni:
							if (param.Value != null && 
								m_connection.SQLMode == SqlMode.Oracle && param.Value.ToString() == string.Empty ) 
								// in ORACLE mode a null values will be inserted if the string value is equal to "" 
								m_inputArgs[i] = null;
							else
								m_inputArgs[i] = FindColInfo(i).TransStringForInput(param.Value.ToString());
							break;
						case MaxDBType.CharB: 
						case MaxDBType.StrB: 
						case MaxDBType.VarCharB:
							m_inputArgs[i] = FindColInfo(i).TransBytesForInput((byte[])param.Value);
							break;
						case MaxDBType.Date:
						case MaxDBType.Time:
						case MaxDBType.TimeStamp:
							m_inputArgs[i] = FindColInfo(i).TransDateTimeForInput((DateTime)param.Value);
							break;
						case MaxDBType.Fixed: 
						case MaxDBType.Float: 
						case MaxDBType.VFloat:
						case MaxDBType.Number:
						case MaxDBType.NoNumber:
							m_inputArgs[i] = FindColInfo(i).TransDoubleForInput((double)param.Value);
							break;
						case MaxDBType.Integer:
							m_inputArgs[i] = FindColInfo(i).TransInt64ForInput((long)param.Value);
							break;
						case MaxDBType.SmallInt:
							m_inputArgs[i] = FindColInfo(i).TransInt16ForInput((short)param.Value);
							break;
					}
				}

				if (m_parseInfo.m_inputCount > 0 || m_parseInfo.m_hasStreams) 
				{
					dataPart = requestPacket.NewDataPart(m_parseInfo.m_varDataInput);
					if (m_parseInfo.m_inputCount > 0) 
					{
						dataPart.AddRow(m_parseInfo.m_inputCount);
						for (int i = 0; i < m_parseInfo.m_paramInfos.Length; ++i) 
						{
							if (m_parseInfo.m_paramInfos[i].IsInput && m_initialParamValue == m_inputArgs[i].ToString())
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
				if (m_parseInfo.m_isSelect && m_parseInfo.m_columnInfos == null
					&& m_parseInfo.m_funcCode != FunctionCode.DBProcWithResultSetExecute)
				{
					requestPacket.InitDbsCommand("DESCRIBE ", false, false);
					requestPacket.AddParseIdPart(m_parseInfo.ParseID);
				}

				try 
				{
					replyPacket = m_connection.Exec(requestPacket, this,                                              
						(!m_parseInfo.m_hasStreams && m_inputProcedureLongs == null) ? GCMode.GC_ALLOWED : GCMode.GC_NONE);
					// Recycling of parse infos and cursor names is not allowed
					// if streams are in the command. Even sending it just behind
					// as next packet is harmful. Same with INPUT LONG parameters of
					// DB Procedures.
				}
				catch(MaxDBSQLException dbExc) 
				{
					if ((dbExc.VendorCode == -8) && afterParseAgain > 0) 
					{
						ResetPutValues(m_inputLongs);
						Reparse();
						m_connection.FreeRequestPacket(requestPacket);
						afterParseAgain--;
						return Execute(afterParseAgain);
					}
					else 
					{
						// The request packet has already been recycled in
						// Connection.execute()
						throw dbExc;
					}
				}

				// --- now it becomes difficult ...
				if (m_parseInfo.m_isSelect) 
					isQuery = ParseResult(replyPacket, null, m_parseInfo.m_columnInfos, m_parseInfo.m_columnNames);
				else 
				{
					if(m_inputProcedureLongs != null) 
						replyPacket = ProcessProcedureStreams(replyPacket);
					else if (m_parseInfo.m_hasStreams) 
						throw new NotSupportedException(MaxDBMessages.Extract(MaxDBMessages.ERROR_OMS_UNSUPPORTED));
					isQuery = ParseResult(replyPacket, null, m_parseInfo.m_columnInfos, m_parseInfo.m_columnNames);
					int returnCode = replyPacket.ReturnCode;
					if (replyPacket.ExistsPart(PartKind.Data)) 
						m_replyMem = replyPacket.Clone(replyPacket.PartDataPos);
					if ((m_parseInfo.m_hasLongs && !m_parseInfo.m_isDBProc) && (returnCode == 0)) 
						HandleStreamsForPutValue(replyPacket);
				}

				for(int i = 0; i < m_parseInfo.m_paramInfos.Length; i++)
				{
					MaxDBParameter param = m_parameters[i];
					if (!FindColInfo(i).IsOutput)
						continue;

					switch(param.m_dbType)
					{
						case MaxDBType.Boolean:
							param.Value = FindColInfo(i).GetBoolean(m_replyMem);
							break;
						case MaxDBType.CharA: 
						case MaxDBType.StrA: 
						case MaxDBType.VarCharA:
						case MaxDBType.CharE: 
						case MaxDBType.StrE: 
						case MaxDBType.VarCharE:
						case MaxDBType.Unicode: 
						case MaxDBType.StrUni: 
						case MaxDBType.VarCharUni:
							param.Value = FindColInfo(i).GetString(this, m_replyMem);
							break;
						case MaxDBType.CharB: 
						case MaxDBType.StrB: 
						case MaxDBType.VarCharB:
							param.Value = FindColInfo(i).GetBytes(this, m_replyMem);
							break;
						case MaxDBType.Date:
						case MaxDBType.Time:
						case MaxDBType.TimeStamp:
							param.Value = FindColInfo(i).GetDateTime(m_replyMem);
							break;
						case MaxDBType.Fixed: 
						case MaxDBType.Float: 
						case MaxDBType.VFloat:
						case MaxDBType.Number:
						case MaxDBType.NoNumber:
							param.Value = FindColInfo(i).GetDouble(m_replyMem);
							break;
						case MaxDBType.Integer:
							param.Value = FindColInfo(i).GetInt64(m_replyMem);
							break;
						case MaxDBType.SmallInt:
							param.Value = FindColInfo(i).GetInt16(m_replyMem);
							break;
					}
				}

				return isQuery;
			}
			catch (TimeoutException timeout) 
			{
				if (m_connection.IsInTransaction) 
					throw timeout;
				else 
				{
					ResetPutValues(m_inputLongs);
					Reparse();
					return Execute(m_maxParseAgainCnt);
				}
			} 
			finally
			{
				m_canceled = false;
			}
		}

		private bool ParseResult(MaxDBReplyPacket replyPacket, string sqlCmd, DBTechTranslator[] infos, string[] columnNames)
		{
			string tableName = null;
			bool isQuery = false;
			bool rowNotFound = false;
			bool dataPartFound = false;
			
			m_rowsAffected = -1;
			m_hasRowCount  = false;
			int functionCode = replyPacket.FuncCode;
			if (functionCode == FunctionCode.Select || functionCode == FunctionCode.Show || 
				functionCode == FunctionCode.DBProcWithResultSetExecute || functionCode == FunctionCode.Explain) 
				isQuery = true;

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
						if (infos==null)
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
					CreateDataReader(sqlCmd, tableName, infos, columnNames, rowNotFound, replyPacket);
				else
					CreateDataReader(sqlCmd, tableName, infos, columnNames, rowNotFound, null);
			} 
			
			return isQuery;
		}

		private void CreateDataReader(string sqlCmd, string tableName, DBTechTranslator[] infos, string[] columnNames, 
			bool rowNotFound, MaxDBReplyPacket reply)
		{
			try 
			{
				FetchInfo fetchInfo = new FetchInfo(m_connection, this.m_cursorName, infos, columnNames);
				m_currentDataReader = new MaxDBDataReader(m_connection, fetchInfo, this, reply);
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
				result = cache.FindParseInfo(sql);

			if ((result == null) || parseAgain) 
			{
				try 
				{
					m_setWithInfo = true;
					replyPacket = SendSQL(sql, parseAgain);
				} 
				catch(TimeoutException)
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
							result.SetParseIDAndSession(replyPacket.ReadBytes(parseidPos, 12), replyPacket.ReadInt32(parseidPos));
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
							result.updTableName = replyPacket.ReadString(replyPacket.PartDataPos, replyPacket.PartLength);
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
						putval.putDescriptor(dataPart, descriptorPos);
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
				replyPacket = m_connection.Exec(requestPacket, this, GCMode.GC_DELAYED);
				
				//  write trailing end of LONGs marker
				if (requiresTrailingPacket && !m_canceled) 
				{
					requestPacket = m_connection.GetRequestPacket();
					dataPart = requestPacket.InitPutValue(m_connection.AutoCommit);
					lastStream.MarkAsLast(dataPart);
					dataPart.Close();
					m_connection.Exec(requestPacket, this, GCMode.GC_DELAYED);
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
				putval.setDescriptor(descriptor);
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
				buffer_length += SQLDBC.SQLDBC_ParameterMetaData_getPhysicalLength(meta, i) * sizeof(char);

			// +1 byte to avoid zero-length array
			ByteArray paramArr = new ByteArray(buffer_length + 1);

			fixed(byte *buffer_ptr = paramArr.arrayData)
			{
				int[] val_size = new int[parameters.Count];
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
								paramArr.WriteByte((byte)((bool)param.Value ? 1 : 0), buffer_offset);

							val_length = sizeof(byte);

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
								val_length = Math.Min(((string)param.Value).Length, 
									SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
								paramArr.WriteASCII((string)param.Value, buffer_offset, val_length);
							}
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

							val_size[i - 1] = val_length;
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.CharB: 
						case MaxDBType.StrB: 
						case MaxDBType.VarCharB:
							if (input_val) 
							{
								val_length = Math.Min(((byte[])param.Value).Length , 
									SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
								paramArr.WriteBytes(((byte[])param.Value), buffer_offset, val_length);
							}
							else
								val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Date:
							if (input_val)
							{
								DateTime dt = (DateTime)param.Value;
								//ODBC date format
								ODBCDATE dt_odbc;
								dt_odbc.year = (short)(dt.Year % 0x10000);
								dt_odbc.month = (ushort)(dt.Month % 0x10000);
								dt_odbc.day = (ushort)(dt.Day % 0x10000);
								
								paramArr.WriteBytes(ODBCConverter.GetBytes(dt_odbc), buffer_offset); 
							}

							val_length = sizeof(ODBCDATE);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							
							break;
						case MaxDBType.Fixed: 
						case MaxDBType.Float: 
						case MaxDBType.VFloat:
						case MaxDBType.Number:
						case MaxDBType.NoNumber:
							if (input_val) 
								paramArr.WriteDouble((double)param.Value, buffer_offset);
								
							val_length = sizeof(double);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Integer:
							if (input_val) 
								paramArr.WriteInt32((int)param.Value, buffer_offset);

							val_length = sizeof(int);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.SmallInt:
							if (input_val) 
								paramArr.WriteInt16((short)param.Value, buffer_offset);

							val_length = sizeof(short);
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Time:
							if (input_val)
							{
								DateTime tm = (DateTime)param.Value;
								//ODBC time format
								ODBCTIME tm_odbc = new ODBCTIME();
								tm_odbc.hour = (ushort)(tm.Hour % 0x10000);
								tm_odbc.minute = (ushort)(tm.Minute % 0x10000);
								tm_odbc.second = (ushort)(tm.Second % 0x10000);
							
								paramArr.WriteBytes(ODBCConverter.GetBytes(tm_odbc), buffer_offset);
							}

							val_length = sizeof(ODBCTIME);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.TimeStamp:
							if (input_val)
							{
								DateTime ts = (DateTime)param.Value;
								//ODBC timestamp format
								ODBCTIMESTAMP ts_odbc = new ODBCTIMESTAMP();
								ts_odbc.year = (short)(ts.Year % 0x10000);
								ts_odbc.month = (ushort)(ts.Month % 0x10000);
								ts_odbc.day = (ushort)(ts.Day % 0x10000);
								ts_odbc.hour = (ushort)(ts.Hour % 0x10000);
								ts_odbc.minute = (ushort)(ts.Minute % 0x10000);
								ts_odbc.second = (ushort)(ts.Second % 0x10000);
								ts_odbc.fraction = (uint) ts.Millisecond * 1000000;

								paramArr.WriteBytes(ODBCConverter.GetBytes(ts_odbc), buffer_offset);
							}

							val_length = sizeof(ODBCTIMESTAMP);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
									SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							break;
						case MaxDBType.Unicode: 
						case MaxDBType.StrUni: 
						case MaxDBType.VarCharUni:
							if (m_connection.DatabaseEncoding == Encoding.ASCII)
							{
								if (input_val)
								{
									val_length = Math.Min(((string)param.Value).Length, 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(byte);
									paramArr.WriteASCII((string)param.Value, buffer_offset, val_length);
								}
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);

								val_size[i - 1] = val_length;
							
								if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
									new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
									throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
										SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							}
							else
							{
								if (input_val)
								{
									val_length = Math.Min(((string)param.Value).Length , 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(char);
									paramArr.WriteUnicode((string)param.Value, buffer_offset, val_length);
								}
								else
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);

								val_size[i - 1] = val_length;

								if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, 
									Consts.IsLittleEndian ? SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED : SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
									throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
										SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							}
							break;
					}

					buffer_offset += val_length;
				}

				SQLDBC_Retcode rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

				buffer_offset = 0;

				if (rc == SQLDBC_Retcode.SQLDBC_OK || rc == SQLDBC_Retcode.SQLDBC_DATA_TRUNC)
					for(short i = 1; i <= paramCount; i++)
					{
						MaxDBParameter param = parameters[i - 1];

						int val_length = 0;
						bool output_val = (
							SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.InOut || 
							SQLDBC.SQLDBC_ParameterMetaData_getParameterMode(meta, i) == SQLDBC_ParameterMode.Out);
											
						switch(param.m_dbType)
						{
							case MaxDBType.Boolean:
								param.Value = (paramArr.ReadByte(buffer_offset) == 1);
								buffer_offset += sizeof(byte);
								break;
							case MaxDBType.CharA: 
							case MaxDBType.StrA: 
							case MaxDBType.VarCharA: 
							case MaxDBType.CharE: 
							case MaxDBType.StrE: 
							case MaxDBType.VarCharE:
								if (output_val)
								{
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
									param.Value = paramArr.ReadASCII(buffer_offset, val_length - 1);
								}
								else
									val_length = Math.Min(((string)param.Value).Length * sizeof(byte), 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i));

								buffer_offset += val_length;
								break;
							case MaxDBType.CharB: case MaxDBType.StrB: case MaxDBType.VarCharB:
								if (output_val)
								{
									val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i);
									param.Value = paramArr.ReadBytes(buffer_offset, val_length - 1);
								}
								else
									val_length = Math.Min(((byte[])param.Value).Length * sizeof(byte), 
										SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i));

								buffer_offset += val_length;
								break;
							case MaxDBType.Date:
								//ODBC date format
								val_length = sizeof(ODBCDATE);
								ODBCDATE dt_odbc = ODBCConverter.GetDate(paramArr.ReadBytes(buffer_offset, val_length));
								param.Value = new DateTime(dt_odbc.year, dt_odbc.month, dt_odbc.day);
								buffer_offset += val_length;
								break;
							case MaxDBType.Fixed: 
							case MaxDBType.Float: 
							case MaxDBType.VFloat:
							case MaxDBType.Number:
							case MaxDBType.NoNumber:
								val_length = sizeof(double);
								param.Value = paramArr.ReadDouble(buffer_offset);
								buffer_offset += val_length;
								break;
							case MaxDBType.Integer:
								val_length = sizeof(int);
								param.Value = paramArr.ReadInt32(buffer_offset);
								buffer_offset += val_length;
								break;
							case MaxDBType.SmallInt:
								val_length = sizeof(int);
								param.Value = paramArr.ReadInt16(buffer_offset);
								buffer_offset += val_length;
								break;
							case MaxDBType.Time:
								val_length = sizeof(ODBCTIME);
								ODBCTIME tm_odbc = ODBCConverter.GetTime(paramArr.ReadBytes(buffer_offset, val_length));
								param.Value = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, 
									tm_odbc.hour, tm_odbc.minute, tm_odbc.second);
								buffer_offset += val_length;
								break;
							case MaxDBType.TimeStamp:
								val_length = sizeof(ODBCTIMESTAMP);
								ODBCTIMESTAMP ts_odbc = ODBCConverter.GetTimeStamp(paramArr.ReadBytes(buffer_offset, val_length));
								param.Value = new DateTime(ts_odbc.year, ts_odbc.month, ts_odbc.day, 
									ts_odbc.hour, ts_odbc.minute, ts_odbc.second, (int)(ts_odbc.fraction / 1000000));
								buffer_offset += val_length;
								break;
							case MaxDBType.Unicode: 
							case MaxDBType.StrUni: 
							case MaxDBType.VarCharUni:
								if (m_connection.DatabaseEncoding == Encoding.ASCII)
								{
									if (output_val)
									{
										val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(byte);
										param.Value = paramArr.ReadASCII(buffer_offset, val_length - 1);
									}
									else
										val_length = Math.Min(((string)param.Value).Length * sizeof(byte), 
											SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i));

									buffer_offset += val_length;
								}
								else
								{
									if (output_val)
									{
										val_length = SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i) * sizeof(char);
										param.Value = paramArr.ReadUnicode(buffer_offset, val_length - 1);
									}
									else
										val_length = Math.Min(((string)param.Value).Length, 
											SQLDBC.SQLDBC_ParameterMetaData_getParameterLength(meta, i)) * sizeof(char);

									buffer_offset += val_length;
								}
								break;
						}
						buffer_offset += val_length;
					}

				if(rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND) 
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_EXEC_FAILED) + ": " + 
						SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
			}
		}

		#endregion
#endif
	}
}
