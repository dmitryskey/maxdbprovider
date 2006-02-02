using System;
using System.Text;
using System.Collections;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Parse information class"

	public class MaxDBParseInfo
	{
		public string sqlCmd;
		public bool cached; // flag is set to true if command is in parseinfo cache 
		public int functionCode;

		public MaxDBParseInfo()
		{
			//
			// TODO: Add constructor logic here
			//
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
}

