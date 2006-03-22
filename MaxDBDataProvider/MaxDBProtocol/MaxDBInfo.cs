using System;
using System.Data;
using System.Text;
using System.IO;
using System.Collections;
using System.Runtime.CompilerServices;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE
	#region "Parse information class"

	internal class MaxDBParseInfo 
	{
		public MaxDBConnection m_connection;
		public string m_sqlCmd;
		private byte[] m_parseid;
		private byte[] m_massParseid;
		internal DBTechTranslator[] m_paramInfos;
		internal DBProcParameterInfo[] m_procParamInfos;
		internal short m_inputCount;

		private bool isMassCmd; // flag is set to true if command is a mass command
		internal bool m_isSelect; // flag is set to true if command is a select command 
		internal bool m_isDBProc; // flag is set to true if command is a call dbproc command 
		internal bool m_hasLongs; // flag is set to true if command handle long columns 
		internal bool m_hasStreams;
		bool m_cached; // flag is set to true if command is in parseinfo cache 
		internal int m_funcCode;
		int m_sessionID; // unique identifier for the connection
		internal string[] m_columnNames;

		Hashtable m_columnMap;

		internal DBTechTranslator[] m_columnInfos;

		internal bool m_varDataInput = false;

		// 11th Byte of Parseid coded application code
		private const int applicationCodeByte = 10;

		// tablename used for updateable resultsets
		internal string updTableName;

		public MaxDBParseInfo(MaxDBConnection connection, string sqlCmd, int functionCode)
		{
			m_connection = connection;
			m_sqlCmd = sqlCmd;
			m_massParseid = null;
			m_paramInfos = null;
			m_inputCount = 0;
			m_isSelect = false;
			m_isDBProc = false;
			m_hasLongs = false;
			m_hasStreams = false;
			isMassCmd = false;
			m_funcCode = functionCode;
			m_sessionID = -1;
			updTableName = null;
			m_cached = false;
			m_varDataInput = false;
			if ((m_funcCode == FunctionCode.Select) || (m_funcCode == FunctionCode.Show) 
				|| (m_funcCode == FunctionCode.DBProcWithResultSetExecute) || (m_funcCode == FunctionCode.Explain))
				m_isSelect = true;
        
			if ((m_funcCode == FunctionCode.DBProcWithResultSetExecute) || (m_funcCode == FunctionCode.DBProcExecute)) 
				m_isDBProc = true;
        
			m_columnNames = null;
			m_columnMap = null;
			if (m_funcCode == FunctionCode.DBProcExecute) 
				DescribeProcedureCall();
		}

		~MaxDBParseInfo() 
		{
			m_cached = false;
			DropParseIDs();
		}


		public int FuncCode
		{
			get
			{
				return m_funcCode;
			}
		}

		public bool IsCached
		{
			get
			{
				return m_cached;
			}
			set
			{
				m_cached = value;
			}
		}

		void DescribeProcedureCall()
		{
			// Syntax is one of
			// { CALL <procedure-name>(...) }
			// CALL <procedure-name>(...)
			// where procedure-name is something like IDENTIFIER, "IDENTIFIER",
			// "OWNER"."IDENTIFIER" etc.
			// we always simply give up if we find nothing that helps our needs
			//
			char[] cmdchars = m_sqlCmd.Trim().ToCharArray();
			int i = 0;
			int cmdchars_len = cmdchars.Length;
			// ODBC like dbfunction call.
			if (cmdchars[i] == '{') 
				i++;
        
			if (i == cmdchars_len) 
				return;
        
			while(char.IsWhiteSpace(cmdchars[i])) 
				if (++i == cmdchars_len) 
					return;
			// 'call'
			if (cmdchars[i] == 'C' || cmdchars[i] == 'c') 
			{
				if (++i == cmdchars_len) 
					return;
			} 
			else 
				return;
        
			if (cmdchars[i] == 'A' || cmdchars[i] == 'a') 
			{
				if (++i == cmdchars_len) 
					return;
			} 
			else 
				return;
        
			if (cmdchars[i] == 'L' || cmdchars[i] == 'l') 		
			{
				if (++i == cmdchars_len) 
					return;
			} 
			else 
				return;

			if (cmdchars[i] == 'L' || cmdchars[i] == 'l') 		
			{
				if (++i == cmdchars_len) 
					return;
			} 
			else 
				return;

			while (char.IsWhiteSpace(cmdchars[i])) 
				if (++i == cmdchars_len) 
					return;
 
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
					return;
			}

			for(;;) 
			{
				if (cmdchars[i] == '.' && !quoted) 
					break;
            
				if (cmdchars[i] == '(' && !quoted) 
					break;
            
				if (char.IsWhiteSpace(cmdchars[i]) && !quoted) 
					break;
            
				if (quoted && cmdchars[i] == '"') 
					break;
            
				++idend;
				if (++i == cmdchars_len) 
					return;
			} 

			string procedureName = new string(cmdchars, idstart, idend - idstart);
			string ownerName = null;
			if (!quoted) 
				procedureName = procedureName.ToUpper();
        
			if (cmdchars[i] == '"') 
				++i;
        
			while (i < cmdchars_len && char.IsWhiteSpace(cmdchars[i])) 
				if (++i == cmdchars_len) 
					break;

			if (i < cmdchars_len) 
			{
				if (cmdchars[i] == '.') 
				{
					if (++i == cmdchars_len) 
						return;
                
					while (char.IsWhiteSpace(cmdchars[i])) 
					{
						if (++i == cmdchars_len) 
							return;
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
							return;
					}

					for(;;) 
					{
						if (cmdchars[i] == '.' && !quoted) 
							break;
                    
						if (cmdchars[i] == '(' && !quoted) 
							break;
                    
						if (char.IsWhiteSpace(cmdchars[i]) && !quoted) 
							break;
                    
						if (quoted && cmdchars[i] == '"') 
							break;
                    
						++idend;
						if (++i == cmdchars_len) 
							return;
					} 

					procedureName = new string(cmdchars, idstart, idend - idstart);
					if (!quoted) 
						procedureName = procedureName.ToUpper();
				}
			}

			// Now we have procedure name and possibly the user name.
			MaxDBCommand cmd = null;
			string sql = "SELECT 1 FROM DUAL WHERE FALSE";
			if (ownerName == null) 
			{
				sql = "SELECT PARAM_NO, "
					+ "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
					+ "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER=USER AND "
					+ "DBPROCEDURE=? ORDER BY PARAM_NO, ASCII_OFFSET";
				cmd = new MaxDBCommand(sql, m_connection);
				cmd.Parameters.Add("DBPROCEDURE", procedureName);
			} 
			else 
			{
				sql = "SELECT PARAM_NO, "
					+ "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
					+ "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER=? AND "
					+ "DBPROCEDURE = ? ORDER BY PARAM_NO, ASCII_OFFSET";
				cmd = new MaxDBCommand(sql, m_connection);
				cmd.Parameters.Add("OWNER", ownerName);
				cmd.Parameters.Add("DBPROCEDURE", procedureName);
			}

			// We have a result set and can now create a parameter info.
			MaxDBDataReader rs = cmd.ExecuteReader();
			if (!rs.Read()) 
			{
				m_procParamInfos = new DBProcParameterInfo[0];
				rs.Close();
				return;
			}

			ArrayList parameterInfos = new ArrayList();
			DBProcParameterInfo currentInfo = null;
			int currentIndex = 0;
			do 
			{
				int index = rs.GetInt32(1);
				// Check if we have a structure element or a new parameter.
				if (index != currentIndex) 
				{
					string datatype = rs.GetString(2);
					if (datatype.ToUpper() == "ABAPTABLE" || datatype.ToUpper() == "STRUCTURE") 
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
					string datatype = rs.GetString(2);
					string code = rs.GetString(3);
					int len = rs.GetInt32(4);
					int dec = rs.GetInt32(5);
					int offset = rs.GetInt32(7);
					int asciiOffset = rs.GetInt32(8);
					int unicodeOffset = rs.GetInt32(9);
					currentInfo.addStructureElement(datatype, code, len, dec, offset, asciiOffset, unicodeOffset);
				}
			} 
			while (rs.Read());
			rs.Close();
			m_procParamInfos = (DBProcParameterInfo[]) parameterInfos.ToArray(typeof(DBProcParameterInfo));
		}

		public byte[] MassParseID
		{
			get
			{
				return m_massParseid;
			}
			set
			{
				m_massParseid = value;
				if (value == null) 
					return;

				for (int i = 0; i < FunctionCode.massCmdAppCodes.Length; i++) 
				{
					if (value[applicationCodeByte] == FunctionCode.massCmdAppCodes[i]) 
					{
						this.isMassCmd = true;
						return;
					}
				}
			}
		}

		/**
		 *  
		 */
		public bool IsMassCmd 
		{
			get
			{
				return this.isMassCmd;
			}
		}

		/**
		 * Checks the validity. A parse info is valid if the session is the same as
		 * of the current connection.
		 * 
		 * @return <code>true</code> if the session ids are equal
		 */
		public bool IsValid 
		{
			get
			{
				return m_sessionID == m_connection.m_sessionID;
			}
		}

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
			m_sessionID = sessionId;
			m_parseid = parseId;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void DropParseIDs() 
		{
			if (m_parseid != null && m_connection != null) 
			{
				m_connection.DropParseID(m_parseid);
				m_parseid = null;
			}
			if (m_massParseid != null && m_connection != null) 
			{
				m_connection.DropParseID(m_massParseid);
				m_massParseid = null;
			}
		}

		/**
		 * Gets the information about parameters in sql statement
		 * 
		 * @return a <code>DBTechTranslator []</code holding the parameter infos
		 */
		public DBTechTranslator[] ParamInfo 
		{
			get
			{
				return m_paramInfos;
			}
		}

		/**
		 * Gets the parse id.
		 */
		public byte[] ParseID 
		{
			get
			{
				return m_parseid;
			}
		}

		/**
		 * Retrieves whether the statement is already executed during parse. (by
		 * checking byte 11 of the parse if for <code>csp1_p_command_executed</code>.
		 */
		public bool IsAlreadyExecuted
		{
			get
			{
				return (m_parseid != null && m_parseid[MaxDBParseInfo.applicationCodeByte] == FunctionCode.command_executed);
			}
		}

		/**
		 * Sets the infos about parameters and result columns.
		 * 
		 * @param shortInfo
		 *            info about the parameters and result columns
		 * @param columnames
		 *            the names of the result columns
		 */
		public void SetShortInfosAndColumnNames(DBTechTranslator[] shortInfo, string[] columnNames)
		{
			// clear the internal dependent fields
			m_inputCount = 0;
			m_hasLongs = false;
			m_hasStreams = false;
			m_paramInfos = null;
			m_columnMap = null;
			m_columnInfos = null;
			m_columnNames = columnNames;

			if (shortInfo == null && columnNames == null) 
			{
				m_paramInfos = m_columnInfos = new DBTechTranslator[0];
				return;
			}

			// we have variants:
			// only a select is really good. All other variants
			// do not and never deliver information on being prepared.
			if (m_funcCode == FunctionCode.Select) 
			{
				if (columnNames == null || columnNames.Length == 0) 
				{
					m_paramInfos = shortInfo;
					for (int i = 0; i < m_paramInfos.Length; ++i) 
					{
						DBTechTranslator current = shortInfo[i];
						if (current.IsInput) 
						{
							current.ColumnIndex = i;
							m_inputCount++;
						}
						m_hasLongs |= current.IsLongKind;
						m_hasStreams |= current.IsStreamKind;
					}
				} 
				else 
				{
					int column_count = columnNames.Length;
					m_columnInfos = new DBTechTranslator[column_count];
					m_paramInfos = new DBTechTranslator[shortInfo.Length - column_count];

					int colInfoIdx = 0;
					int paramInfoIdx = 0;

					for (int i = 0; i < shortInfo.Length; ++i) 
					{
						DBTechTranslator current = shortInfo[i];
						if (current.IsInput) 
						{
							if (paramInfoIdx == m_paramInfos.Length) 
							{
								throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_UNEXPECTEDINPUT, paramInfoIdx));
							}
							current.ColumnIndex = paramInfoIdx;
							m_paramInfos[paramInfoIdx] = current;
							paramInfoIdx++;
							m_inputCount++;
						} 
						else 
						{
							if (colInfoIdx == m_columnInfos.Length) 
								throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_INTERNAL_UNEXPECTEDOUTPUT, colInfoIdx));
							m_columnInfos[colInfoIdx] = current;
							current.ColumnIndex = colInfoIdx;
							current.ColumnName = columnNames[colInfoIdx];
							colInfoIdx++;
						}
						m_hasLongs |= shortInfo[i].IsLongKind;
						m_hasStreams |= shortInfo[i].IsStreamKind;
					}
				}
			} 
			else 
			{ // no result set data, as we cannot to be sure
				m_paramInfos = shortInfo;
				if (columnNames != null) 
				{
					// fortunately at least column names
					// sometimes only output parameters are named
					if (columnNames.Length == m_paramInfos.Length) 
					{
						for (int i = 0; i < columnNames.Length; ++i) 
						{
							DBTechTranslator current = m_paramInfos[i];
							current.ColumnIndex = i;
							current.ColumnName = columnNames[i];
							if (m_procParamInfos != null && i < m_procParamInfos.Length) 
								current.SetProcParamInfo(m_procParamInfos[i]);
							m_inputCount += (short)(current.IsInput ? 1 : 0);
							m_hasLongs |= current.IsLongKind;
							m_hasStreams |= current.IsStreamKind;
						}
					} 
					else 
					{ // we will leave out the input parameters
						int colNameIdx = 0;
						for (int j = 0; j < m_paramInfos.Length; ++j) 
						{
							DBTechTranslator current = m_paramInfos[j];
							current.ColumnIndex = j;
							if (m_procParamInfos != null && j < m_procParamInfos.Length) 
								current.SetProcParamInfo(m_procParamInfos[j]);
							if (current.IsOutput) 
								current.ColumnName = columnNames[colNameIdx++];
							else 
								++m_inputCount;
							m_hasLongs |= current.IsLongKind;
							m_hasStreams |= current.IsStreamKind;
						}
					}
				} 
				else 
				{
					// No column names at all. OK.
					for (int i = 0; i < m_paramInfos.Length; ++i) 
					{
						DBTechTranslator current = m_paramInfos[i];
						current.ColumnIndex = i;
						if (m_procParamInfos != null && i < m_procParamInfos.Length) 
							current.SetProcParamInfo(m_procParamInfos[i]);
						m_inputCount += (short)(current.IsInput ? 1 : 0);
						m_hasLongs |= current.IsLongKind;
						m_hasStreams |= current.IsStreamKind;
					}
				}
			}
		}

		public Hashtable ColumnMap
		{
			get
			{
				if (m_columnMap != null)
					return m_columnMap;

				if (m_columnNames == null)
					throw new DataException(MaxDBMessages.Extract(MaxDBMessages.ERROR_NO_COLUMNNAMES));

				m_columnMap = new Hashtable(m_columnNames.Length);
				for (int i = 0; i < m_paramInfos.Length; ++i) 
				{
					DBTechTranslator current = m_paramInfos[i];
					string colname = current.ColumnName;
					if (colname != null) 
						m_columnMap[colname] = current;
				}
		
				return m_columnMap;
			}
		}

		public void SetMetaData(DBTechTranslator[] info, string[] colName)
		{
			int colCount = info.Length;
			DBTechTranslator currentInfo;
			string currentName;
			m_columnNames = colName;

			if (colCount == colName.Length) 
			{
				m_columnInfos = info;
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
				m_columnInfos = new DBTechTranslator[colName.Length];
				for (int i = 0; i < colCount; ++i) 
				{
					if (info[i].IsOutput) 
					{
						currentInfo = m_columnInfos[outputColCnt] = info[i];
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

	public class StructureElement 
	{
		public StructureElement(string typeName, string codeType, int length, int precision, int offset, int asciiOffset, int unicodeOffset) 
		{
			this.TypeName      = typeName.ToUpper().Trim();
			this.CodeType      = codeType.ToUpper().Trim();
			this.Length        = length;
			this.Precision     = precision;
			this.Offset        = offset;
			this.ASCIIOffset   = asciiOffset;
			this.UnicodeOffset = unicodeOffset;  		   	
		}
		 
		public string TypeName;
		public string CodeType;
		public int    Length;
		public int    Precision;
		public int    Offset;
		public int    ASCIIOffset;
		public int    UnicodeOffset;
		
		public string SQLTypeName
		{
			get
			{
				switch(TypeName.ToUpper().Trim())
				{
					case "CHAR":
						return TypeName + "(" + Length + ") " + CodeType;
					case "FIXED":
						return TypeName + "(" + Length + ", " + Precision +")";
					case "BOOLEAN":
						return TypeName;
					default:
						return TypeName + "(" + Length + ")";
				}
			}
		}
	}

	public class DBProcParameterInfo 
	{
		public const int ABAPTABLE  = 1;
		public const int STRUCTURE  = 2;

		private int       type;
		private string    sqlTypeName;
		private string    baseTypeName;
		private ArrayList typeElements;
	
		/*
		  Creates a new DB procedure parameter info.
		  @param datatype The data type as read from DBPROCPARAMINFO.
		  @param len The length information from DBPROCPARAMINFO.
		  @param dec The precision information from DBPROCPARAMINFO.
		*/
		public DBProcParameterInfo(string datatype) 
		{
			if(datatype.ToUpper().Trim() == "ABAPTABLE") 
			{
				type = ABAPTABLE;
				typeElements = new ArrayList();
			} 
			else if(datatype.ToUpper().Trim() == "STRUCTURE") 
			{
				type = STRUCTURE;
				typeElements = new ArrayList();
			} 
		}
	
		public void addStructureElement(string typeName, string codeType, int length, int precision, int offset, int asciiOffset, int unicodeOffset) 
		{
			if(typeElements == null) 
				return;		
			else 
				typeElements.Add(new StructureElement(typeName, codeType, length, precision, offset, asciiOffset, unicodeOffset));
		}

		public int MemberCount 
		{
			get
			{
				return typeElements.Count;
			}
		}
	
		public int ElementType 
		{
			get
			{
				return type;
			}
		}

		public StructureElement this[int index] 
		{
			get
			{
				return (StructureElement) typeElements[index];
			}
		}

		public string SQLTypeName
		{
			get
			{
				if (this.sqlTypeName == null) 
				{
					StringBuilder typeBuffer = new StringBuilder();
					StringBuilder baseType   = new StringBuilder();
					string close = ")";
					if (type == ABAPTABLE) 
					{
						if (typeElements.Count == 1) 
						{
							StructureElement el = (StructureElement)typeElements[0];
							if(el.TypeName.ToUpper().Trim() == "CHAR") 
							{
								if (el.CodeType.ToUpper().Trim() == "ASCII") 
									sqlTypeName = "CHARACTER STREAM";
								else if (el.CodeType.ToUpper().Trim() == "BYTE") 
									sqlTypeName = "BYTE STREAM";
							} 
							else if(el.TypeName.ToUpper().Trim() == "WYDE") 
								sqlTypeName = "CHARACTER STREAM";
						
							typeBuffer.Append("STREAM(");
						} 
						else 
						{
							typeBuffer.Append("STREAM(STRUCTURE(");
							close="))";
						}
					} 
					else 
						typeBuffer.Append("STRUCTURE(");

					for(int i = 0; i< typeElements.Count; ++i) 
					{
						if(i!=0) 
						{
							baseType.Append(", ");
							typeBuffer.Append(", ");
						}
						StructureElement el = (StructureElement)typeElements[i];
						typeBuffer.Append(el.SQLTypeName);
						baseType.Append(el.SQLTypeName);
					}
					typeBuffer.Append(close);
					sqlTypeName = typeBuffer.ToString();
					baseTypeName = baseType.ToString();
				}
		
				return sqlTypeName;
			}
		}

		public string BaseTypeName 
		{
			get
			{
				return baseTypeName;
			}
		}
	}

	#endregion

	#region "Database procedure parameters structure"

	public class DBProcStructure 
	{
		private object[] elements; 
		private string typeName;

		public DBProcStructure(object[] elements, string typeName) 
		{
			this.elements = elements;
			this.typeName = typeName;	
		}
    
		public object[] Attributes 
		{
			get
			{
				return elements;
			}
		}

		public string SQLTypeName 
		{
			get
			{
				return typeName;
			}
		}
	}
	#endregion

	#region "SQL parameter controller interface"

	public interface ISQLParamController 
	{
		MaxDBConnection Connection{get;}
		ByteArray ReplyData{get;}
	}
	
	#endregion

	#region "Fetch information class"

	internal class FetchInfo
	{
		private MaxDBConnection     m_connection;            // current connection
		private string              m_cursorName;            // cursor
		private DBTechTranslator[]  m_columnInfo;            // short info of all columns
		private int                 m_recordSize;            // physical row size
		private Hashtable           m_columnMapping = null;  // mapping from column names to short infos
		private string				m_fetchparamstring;		 // cache for fetch parameters

		public FetchInfo(MaxDBConnection connection, string cursorName, DBTechTranslator[] infos, string[] columnNames)
		{
			m_connection = connection;
			m_cursorName = cursorName;
			if(infos == null || columnNames==null) 
				m_columnInfo = null;
			else 
				SetMetaData(infos, columnNames);
		}

		private void SetMetaData(DBTechTranslator[] info, string[] colName)
		{
			int colCount = info.Length;
			DBTechTranslator currentInfo;
			int currentFieldEnd;

			m_recordSize = 0;

			if (colCount == colName.Length) 
			{
				m_columnInfo = info;
				for (int i = 0; i < colCount; ++i) 
				{
					currentInfo = info[i];
					currentInfo.ColumnName = colName[i];
					currentInfo.ColumnIndex = i;
					currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
					m_recordSize = Math.Max(m_recordSize, currentFieldEnd);
				}
			}
			else 
			{
				int outputColCnt = 0;
				m_columnInfo = new DBTechTranslator[colName.Length];
				for (int i = 0; i < colCount; ++i) 
				{
					if (info [i].IsOutput)
					{
						currentInfo = m_columnInfo[outputColCnt] = info [i];
						currentInfo.ColumnName = colName [outputColCnt];
						currentInfo.ColumnIndex = outputColCnt++;
						currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
						m_recordSize = Math.Max(m_recordSize, currentFieldEnd);
					}
				}
			}
		}

		private void SetColMapping ()
		{
			int colCnt = m_columnInfo.Length;
			m_columnMapping = new Hashtable(2 * colCnt);
			DBTechTranslator currentInfo;

			for (int i = 0; i < colCnt; i++) 
			{
				currentInfo = m_columnInfo[i];
				m_columnMapping[currentInfo.ColumnName] = currentInfo;
			}
		}

		private void Describe()
		{
			MaxDBConnection c = m_connection;
			DBTechTranslator[] infos = null;
			string[] columnNames = null;

			MaxDBRequestPacket request = c.GetRequestPacket();
			request.InitDbsCommand(false, "DESCRIBE \"" + m_cursorName + "\"");
			MaxDBReplyPacket reply = c.Exec(request, this, GCMode.GC_ALLOWED);
			reply.ClearPartOffset();
			for(int i = 0; i < reply.PartCount; i++) 
			{
				reply.NextPart();

				int partType = reply.PartType;

				if(partType == PartKind.ColumnNames) 
					columnNames=reply.ParseColumnNames();
				else if(partType == PartKind.ShortInfo) 
					infos = reply.ParseShortFields(m_connection.m_spaceOption, false, null, false);
				else if(partType == PartKind.Vardata_ShortInfo) 
					infos = reply.ParseShortFields(m_connection.m_spaceOption, false, null, true);
			}
			SetMetaData(infos, columnNames);
		}

		public MaxDBReplyPacket ExecFetchNext()
		{
			if (m_columnInfo == null)
				Describe();
				
			if(m_fetchparamstring == null) 
			{
				StringBuilder tmp = new StringBuilder("?");
				for(int i = 1; i < m_columnInfo.Length; i++) 
					tmp.Append(", ?");
				m_fetchparamstring = tmp.ToString();
			}

			string cmd="FETCH NEXT \"" + m_cursorName + "\" INTO " + m_fetchparamstring;
			
			MaxDBRequestPacket request = m_connection.GetRequestPacket();
			byte currentSQLMode = request.SwitchSqlMode(SqlMode.Internal);
			request.InitDbsCommand(m_connection.AutoCommit, cmd);

			request.SetMassCommand();
			request.AddResultCount(30000);

			try 
			{
				return m_connection.Exec(request, this, GCMode.GC_DELAYED);
			} 
			finally 
			{
				request.SwitchSqlMode(currentSQLMode);
			}
		}

		public DBTechTranslator GetColumnInfo(string name)
		{
			if (m_columnInfo == null)
				Describe();
			
			if (m_columnMapping == null)
				SetColMapping();
			
			object obj = m_columnMapping[name];
			if(obj == null) 
			{
				string uc = name.ToUpper();
				obj = m_columnMapping[uc];
				if(obj != null) 
					m_columnMapping[uc] = obj;
			}
			return (DBTechTranslator)obj;
		}

		public DBTechTranslator GetColumnInfo(int index)
		{
			if (m_columnInfo == null)
				Describe();
			return m_columnInfo[index];
		}

		public int NumberOfColumns
		{
			get
			{
				return m_columnInfo.Length;
			}
		}

		public int RecordSize
		{
			get
			{
				return m_recordSize;
			}
		}
	}

	#endregion
#endif

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

		public static string GetTypeName(int columnType)
		{
			switch (columnType) 
			{
				case DataType.CHA:
				case DataType.CHE:
				case DataType.DBYTEEBCDIC:
					return DataType.stringValues[DataType.CHA];
				case DataType.CHB:
				case DataType.ROWID:
					return DataType.stringValues[DataType.CHB];
				case DataType.UNICODE:
					return DataType.stringValues[DataType.UNICODE];
				case DataType.VARCHARA:
				case DataType.VARCHARE:
					return DataType.stringValues[DataType.VARCHARA];
				case DataType.VARCHARB:
					return DataType.stringValues[DataType.VARCHARB];
				case DataType.VARCHARUNI:
					return DataType.stringValues[DataType.VARCHARUNI];
				case DataType.STRA:
				case DataType.STRE:
				case DataType.LONGA:
				case DataType.LONGE:
				case DataType.LONGDB:
					return DataType.stringValues[DataType.LONGA];
				case DataType.STRB:
				case DataType.LONGB:
					return DataType.stringValues[DataType.LONGB];
				case DataType.STRUNI:
				case DataType.LONGUNI:
					return DataType.stringValues[DataType.LONGUNI];
				case DataType.DATE:
					return DataType.stringValues[DataType.DATE];
				case DataType.TIME:
					return DataType.stringValues[DataType.TIME];
				case DataType.TIMESTAMP:
					return DataType.stringValues[DataType.TIMESTAMP];
				case DataType.BOOLEAN:
					return DataType.stringValues[DataType.BOOLEAN];
				case DataType.FIXED:
				case DataType.NUMBER:
					return DataType.stringValues[DataType.FIXED];
				case DataType.FLOAT:
				case DataType.VFLOAT:
					return DataType.stringValues[DataType.FLOAT];
				case DataType.SMALLINT:
					return DataType.stringValues[DataType.SMALLINT];
				case DataType.INTEGER:
					return DataType.stringValues[DataType.INTEGER];
				default:
					return MaxDBMessages.Extract(MaxDBMessages.UNKNOWNTYPE);
			}
		}

		public static Type GetType(int columnType)
		{
			switch(columnType)
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
					return typeof(TextReader);
				case DataType.STRB:
				case DataType.LONGB:
				case DataType.LONGDB:
				case DataType.LONGUNI:
					return typeof(BinaryReader);
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
	}

	#endregion

}

