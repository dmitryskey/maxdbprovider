using System;
using System.Data;
using System.Text;
using System.Collections;
using System.Runtime.CompilerServices;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Parse information class"

	public class MaxDBParseInfo 
	{
		public MaxDBConnection connection;
		public string sqlCmd;
		private byte[] parseid;
		private byte[] massParseid;
		DBTechTranslator[] paramInfos;
		DBProcParameterInfo[] procParamInfos;
		int inputCount;

		private bool isMassCmd; // flag is set to true if command is a mass command
		bool isSelect; // flag is set to true if command is a select command 
		bool isDBProc; // flag is set to true if command is a call dbproc command 
		bool hasLongs; // flag is set to true if command handle long columns 
		bool hasStreams;
		bool m_cached; // flag is set to true if command is in parseinfo cache 
		int m_funcCode;
		int m_sessionID; // unique identifier for the connection
		string[] columnNames;

		Hashtable columnMap;

		DBTechTranslator[] columnInfos;

		bool isClosed = false;

		bool varDataInput = false;

		// 11th Byte of Parseid coded application code
		private const int applicationCodeByte = 10;

		// tablename used for updateable resultsets
		string updTableName;

		public MaxDBParseInfo(MaxDBConnection connection, string sqlCmd, int functionCode)
		{
			this.connection = connection;
			this.sqlCmd = sqlCmd;
			this.massParseid = null;
			this.paramInfos = null;
			this.inputCount = 0;
			this.isSelect = false;
			this.isDBProc = false;
			this.hasLongs = false;
			this.hasStreams = false;
			this.isMassCmd = false;
			this.m_funcCode = functionCode;
			this.m_sessionID = -1;
			this.updTableName = null;
			this.m_cached = false;
			varDataInput = false;
			if ((m_funcCode == FunctionCode.Select) || (m_funcCode == FunctionCode.Show) 
				|| (m_funcCode == FunctionCode.DBProcWithResultSetExecute) || (m_funcCode == FunctionCode.Explain))
				isSelect = true;
        
			if ((m_funcCode == FunctionCode.DBProcWithResultSetExecute) || (m_funcCode == FunctionCode.DBProcExecute)) 
				this.isDBProc = true;
        
			this.columnNames = null;
			this.columnMap = null;
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
			char[] cmdchars = sqlCmd.Trim().ToCharArray();
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
				cmd = new MaxDBCommand(sql, connection);
				cmd.Parameters.Add("DBPROCEDURE", procedureName);
			} 
			else 
			{
				sql = "SELECT PARAM_NO, "
					+ "DATATYPE, CODE, LEN, DEC, \"IN/OUT-TYPE\", OFFSET, ASCII_OFFSET, "
					+ "UNICODE_OFFSET FROM DBPROCPARAMINFO WHERE OWNER=? AND "
					+ "DBPROCEDURE = ? ORDER BY PARAM_NO, ASCII_OFFSET";
				cmd = new MaxDBCommand(sql, connection);
				cmd.Parameters.Add("OWNER", ownerName);
				cmd.Parameters.Add("DBPROCEDURE", procedureName);
			}

			// We have a result set and can now create a parameter info.
			MaxDBDataReader rs = cmd.ExecuteReader();
			if (!rs.Read()) 
			{
				procParamInfos = new DBProcParameterInfo[0];
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
						int len = rs.GetInt32(4);
						int dec = rs.GetInt32(5);
						currentInfo = new DBProcParameterInfo(datatype, len, dec);
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
			procParamInfos = (DBProcParameterInfo[]) parameterInfos.ToArray(typeof(DBProcParameterInfo));
		}

		public byte[] MassParseid
		{
			get
			{
				return massParseid;
			}
			set
			{
				this.massParseid = value;
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
				return m_sessionID == connection.m_sessionID;
			}
		}

		/**
		 * Gets the column infos, needed for result set meta data. If no result
		 * set/no result set meta data available then <code>null</code> is
		 * returned.
		 */
		public DBTechTranslator[] ColumnInfos
		{
			get
			{
				return columnInfos;
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
		public void SetParseIdAndSession(byte[] parseId, int sessionId) 
		{
			m_sessionID = sessionId;
			this.parseid = parseId;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void DropParseIDs() 
		{
			if (parseid != null && connection != null) 
			{
				connection.DropParseID(parseid);
				parseid = null;
			}
			if (this.massParseid != null && this.connection != null) 
			{
				connection.DropParseID(massParseid);
				massParseid = null;
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
				return this.paramInfos;
			}
		}

		/**
		 * Marks/Unmarks the statement as select.
		 * 
		 * @param select
		 *            the select mark
		 */
		public void setSelect(bool select) 
		{
			this.isSelect = select;
		}

		/**
		 * Gets the parse id.
		 */
		public byte[] ParseId 
		{
			get
			{
				return parseid;
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
				return (parseid != null && parseid[MaxDBParseInfo.applicationCodeByte] == FunctionCode.command_executed);
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
		public void setShortInfosAndColumnNames(DBTechTranslator[] shortInfo, string[] columnNames)
		{
			// clear the internal dependent fields
			inputCount = 0;
			hasLongs = false;
			hasStreams = false;
			this.columnNames = null;
			this.paramInfos = null;
			this.columnMap = null;
			this.columnInfos = null;
			this.columnNames = columnNames;

			if (shortInfo == null && columnNames == null) 
			{
				this.paramInfos = this.columnInfos = new DBTechTranslator[0];
				return;
			}

			// we have variants:
			// only a select is really good. All other variants
			// do not and never deliver information on being prepared.
			if (m_funcCode == FunctionCode.Select) 
			{
				if (columnNames == null || columnNames.Length == 0) 
				{
					// this.columnInfos=null;
					this.paramInfos = shortInfo;
					for (int i = 0; i < paramInfos.Length; ++i) 
					{
						DBTechTranslator current = shortInfo[i];
						if (current.IsInput) 
						{
							current.ColumnIndex = i;
							inputCount++;
						}
						hasLongs |= current.IsLongKind;
						hasStreams |= current.IsStreamKind;
					}
				} 
				else 
				{
					int column_count = columnNames.Length;
					this.columnInfos = new DBTechTranslator[column_count];
					this.paramInfos = new DBTechTranslator[shortInfo.Length - column_count];

					int colInfoIdx = 0;
					int paramInfoIdx = 0;

					for (int i = 0; i < shortInfo.Length; ++i) 
					{
						DBTechTranslator current = shortInfo[i];
						if (current.IsInput) 
						{
							if (paramInfoIdx == this.paramInfos.Length) 
							{
								throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_INTERNAL_UNEXPECTEDINPUT, paramInfoIdx));
							}
							current.ColumnIndex = paramInfoIdx;
							paramInfos[paramInfoIdx] = current;
							paramInfoIdx++;
							inputCount++;
						} 
						else 
						{
							if (colInfoIdx == columnInfos.Length) 
							{
								throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_INTERNAL_UNEXPECTEDOUTPUT, colInfoIdx));
							}
							columnInfos[colInfoIdx] = current;
							current.ColumnIndex = colInfoIdx;
							current.ColumnName = columnNames[colInfoIdx];
							colInfoIdx++;
						}
						hasLongs |= shortInfo[i].IsLongKind;
						hasStreams |= shortInfo[i].IsStreamKind;
					}
				}
			} 
			else 
			{ // no result set data, as we cannot to be sure
				this.paramInfos = shortInfo;
				if (columnNames != null) 
				{
					// fortunately at least column names
					// sometimes only output parameters are named
					if (columnNames.Length == paramInfos.Length) 
					{
						for (int i = 0; i < columnNames.Length; ++i) 
						{
							DBTechTranslator current = paramInfos[i];
							current.ColumnIndex = i;
							current.ColumnName = columnNames[i];
							if (this.procParamInfos != null	&& i < procParamInfos.Length) 
								current.SetProcParamInfo(procParamInfos[i]);
							inputCount += current.IsInput ? 1 : 0;
							hasLongs |= current.IsLongKind;
							hasStreams |= current.IsStreamKind;
						}
					} 
					else 
					{ // we will leave out the input parameters
						int colNameIdx = 0;
						for (int j = 0; j < paramInfos.Length; ++j) 
						{
							DBTechTranslator current = paramInfos[j];
							current.ColumnIndex = j;
							if (procParamInfos != null && j < procParamInfos.Length) 
								current.SetProcParamInfo(procParamInfos[j]);
							if (current.IsOutput) 
								current.ColumnName = columnNames[colNameIdx++];
							else 
								++inputCount;
							hasLongs |= current.IsLongKind;
							hasStreams |= current.IsStreamKind;
						}
					}
				} 
				else 
				{
					// No column names at all. OK.
					for (int i = 0; i < paramInfos.Length; ++i) 
					{
						DBTechTranslator current = paramInfos[i];
						current.ColumnIndex = i;
						if (procParamInfos != null && i < procParamInfos.Length) 
							current.SetProcParamInfo(procParamInfos[i]);
						inputCount += current.IsInput ? 1 : 0;
						hasLongs |= current.IsLongKind;
						hasStreams |= current.IsStreamKind;
					}
				}
			}
		}

		public Hashtable ColumnMap
		{
			get
			{
				if (columnMap != null)
					return columnMap;

				if (columnNames == null)
					throw new DataException(MessageTranslator.Translate(MessageKey.ERROR_NO_COLUMNNAMES));

				columnMap = new Hashtable(columnNames.Length);
				for (int i = 0; i < paramInfos.Length; ++i) 
				{
					DBTechTranslator current = paramInfos[i];
					string colname = current.ColumnName;
					if (colname != null) 
						columnMap[colname] = current;
				}
		
				return this.columnMap;
			}
		}

		void doDescribeParseId()
		{
			MaxDBRequestPacket requestPacket;
			MaxDBReplyPacket replyPacket;
			string[] columnNames = null;
			DBTechTranslator[] infos = null;

			requestPacket = connection.CreateRequestPacket();
			requestPacket.initDbsCommand(false, "Describe ");
			requestPacket.addParseidPart(this.parseid);
			replyPacket = connection.Exec(requestPacket, this, GCMode.GC_ALLOWED);

			replyPacket.ClearPartOffset();
			for(int i = 0; i < replyPacket.partCount; i++) 
			{
				replyPacket.nextPart();
				switch (replyPacket.PartType) 
				{
					case PartKind.ColumnNames:
						columnNames = replyPacket.parseColumnNames();
						break;
					case PartKind.ShortInfo:
						infos = replyPacket.ParseShortFields(connection.m_spaceOption, false, null, false);
						break;
					case PartKind.Vardata_ShortInfo:
						this.varDataInput = true;
						infos = replyPacket.ParseShortFields(connection.m_spaceOption, false, null, true);
						break;
					default:
						//this.addWarning (new SQLWarning ("part " +
						//        PartKind.names [enum.partKind ()] + " not handled"));
						break;
				}
			}
			SetMetaData(infos, columnNames);
		}

		public void SetMetaData(DBTechTranslator[] info, string[] colName)
		{
			int colCount = info.Length;
			DBTechTranslator currentInfo;
			string currentName;
			this.columnNames = colName;

			if (colCount == colName.Length) 
			{
				this.columnInfos = info;
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
				this.columnInfos = new DBTechTranslator[colName.Length];
				for (int i = 0; i < colCount; ++i) 
				{
					if (info[i].IsOutput) 
					{
						currentInfo = columnInfos[outputColCnt] = info[i];
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
		private string    typeName;
		private string    sqlTypeName;
		private string    baseTypeName;
		private int       length;
		private int       precision;
		private ArrayList typeElements;
	
		/*
		  Creates a new DB procedure parameter info.
		  @param datatype The data type as read from DBPROCPARAMINFO.
		  @param len The length information from DBPROCPARAMINFO.
		  @param dec The precision information from DBPROCPARAMINFO.
		*/
		public DBProcParameterInfo(string datatype, int len, int dec) 
		{
			typeName  = datatype;
			length    = len;
			precision = dec;	
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

	public interface SQLParamController 
	{
		MaxDBConnection Connection{get;}
		ByteArray ReplyData{get;}
	}
	
	#endregion

	#region "Fetch information class"

	public class FetchInfo
	{
		private MaxDBConnection     connection;            // current connection
		private String              cursorName;            // cursor
		private DBTechTranslator[]  columnInfo;            // short info of all columns
		private int                 recordSize;            // physical row size
		private Hashtable           columnMapping = null;  // mapping from column names to short infos
		private string _fetchparamstring;     // cache for fetch parameters

		public FetchInfo(MaxDBConnection connection, string cursorName, DBTechTranslator[] infos, string[] columnNames)
		{
			this.connection = connection;
			this.cursorName = cursorName;
			if(infos==null || columnNames==null) 
				this.columnInfo = null;
			else 
				SetMetaData(infos, columnNames);
		}

		private void SetMetaData(DBTechTranslator[] info, string[] colName)
		{
			int colCount = info.Length;
			DBTechTranslator currentInfo;
			int currentFieldEnd;

			recordSize = 0;

			if (colCount == colName.Length) 
			{
				columnInfo = info;
				for (int i = 0; i < colCount; ++i) 
				{
					currentInfo = info[i];
					currentInfo.ColumnName = colName[i];
					currentInfo.ColumnIndex = i;
					currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
					recordSize = Math.Max(recordSize, currentFieldEnd);
				}
			}
			else 
			{
				int outputColCnt = 0;
				columnInfo = new DBTechTranslator[colName.Length];
				for (int i = 0; i < colCount; ++i) 
				{
					if (info [i].IsOutput)
					{
						currentInfo = columnInfo[outputColCnt] = info [i];
						currentInfo.ColumnName = colName [outputColCnt];
						currentInfo.ColumnIndex = outputColCnt++;
						currentFieldEnd = currentInfo.PhysicalLength + currentInfo.BufPos - 1;
						recordSize = Math.Max(recordSize, currentFieldEnd);
					}
				}
			}
		}

		private void SetColMapping ()
		{
			int colCnt = columnInfo.Length;
			columnMapping = new Hashtable(2 * colCnt);
			DBTechTranslator currentInfo;

			for (int i = 0; i < colCnt; i++) 
			{
				currentInfo = columnInfo[i];
				columnMapping[currentInfo.ColumnName] = currentInfo;
			}
		}

		private void Describe()
		{
			MaxDBConnection c = connection;
			DBTechTranslator[] infos = null;
			string[] columnNames = null;

			MaxDBRequestPacket request = c.CreateRequestPacket();
			request.initDbsCommand(false, "DESCRIBE \"" + cursorName + "\"");
			MaxDBReplyPacket reply = c.Exec(request, this, GCMode.GC_ALLOWED);
			reply.ClearPartOffset();
			for(int i = 0; i < reply.partCount; i++) 
			{
				reply.nextPart();

				int partType = reply.PartType;

				if(partType == PartKind.ColumnNames) 
					columnNames=reply.parseColumnNames();
				else if(partType == PartKind.ShortInfo) 
					infos = reply.ParseShortFields(connection.m_spaceOption, false, null, false);
				else if(partType == PartKind.Vardata_ShortInfo) 
					infos = reply.ParseShortFields(this.connection.m_spaceOption, false, null, true);
			}
			SetMetaData(infos, columnNames);
		}

		public MaxDBReplyPacket ExecFetchNext(int fetchSize)
		{
			if (columnInfo == null)
				Describe();
				
			if(_fetchparamstring == null) 
			{
				StringBuilder tmp = new StringBuilder("?");
				for(int i = 1; i < columnInfo.Length; i++) 
					tmp.Append(", ?");
				_fetchparamstring = tmp.ToString();
			}

			string cmd="FETCH NEXT \"" + cursorName + "\" INTO " + _fetchparamstring;
			
			MaxDBRequestPacket request = connection.CreateRequestPacket();
			byte currentSQLMode = request.switchSqlMode(SqlMode.Internal);
			request.initDbsCommand(connection.AutoCommit, cmd);
			if(fetchSize > 1) 
				request.setMassCommand();
			else 
				fetchSize = 1;

			request.AddResultCount(fetchSize);
			try 
			{
				return connection.Exec(request, this, GCMode.GC_DELAYED);
			} 
			finally 
			{
				request.switchSqlMode(currentSQLMode);
			}
		}

		public DBTechTranslator GetColumnInfo(string name)
		{
			if (columnInfo == null)
				Describe();
			
			if (columnMapping == null)
				SetColMapping();
			
			object obj = columnMapping[name];
			if(obj == null) 
			{
				string uc = name.ToUpper();
				obj = columnMapping[uc];
				if(obj != null) 
					columnMapping[uc] = obj;
			}
			return (DBTechTranslator)obj;
		}

		public int NumberOfColumns
		{
			get
			{
				return columnInfo.Length;
			}
		}

		public int RecordSize
		{
			get
			{
				return recordSize;
			}
		}
	}

	#endregion
}

