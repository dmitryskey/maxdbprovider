//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using MaxDB.Data.MaxDBProtocol;

namespace MaxDB.Data
{
	/// <summary>
	/// Specifies MaxDB specific data type of a field, property, for use in a <see cref="MaxDBParameter"/>.
	/// </summary>
	public enum MaxDBType
	{
		/// <summary>Data type FIXED.</summary>
		Fixed = DataType.FIXED,
		/// <summary>Data type FLOAT.</summary>
		Float = DataType.FLOAT,
		/// <summary>Data type CHAR ASCII.</summary>    
		CharA = DataType.CHA,
		/// <summary>Data type CHAR EBCDIC (deprecated).</summary>     
		CharE = DataType.CHE,
		/// <summary>Data type CHAR BYTE.</summary>     
		CharB = DataType.CHB,
		/// <summary>Internally used (deprecated).</summary>     
		RowId = DataType.ROWID,
		/// <summary>Data type LONG ASCII.</summary>    
		StrA = DataType.STRA,
		/// <summary>Data type LONG EBCDIC (deprecated).</summary>     
		StrE = DataType.STRE,
		/// <summary>Data type LONG BYTE.</summary>    
		StrB = DataType.STRB,
		/// <summary>Internally used (deprecated).</summary>     
		StrDB = DataType.STRDB,
		/// <summary>Data type DATE (SQL mode INTERNAL).</summary>    
		Date = DataType.DATE,
		/// <summary>Data type TIME (SQL mode INTERNAL).</summary>     
		Time = DataType.TIME,
		/// <summary>Data type FLOAT (output of arithmetic expressions).</summary>
		VFloat = DataType.VFLOAT,
		/// <summary>Data type TIMESTAMP (SQL mode INTERNAL), or DATE (SQL mode Oracle).</summary>    
		Timestamp = DataType.TIMESTAMP,
		/// <summary>Internally used (deprecated).</summary>     
		Unknown = DataType.UNKNOWN,
		/// <summary>Internally used (deprecated).</summary>     
		Number = DataType.NUMBER,
		/// <summary>Internally used (deprecated).</summary>     
		NoNumber = DataType.NONUMBER,
		/// <summary>Internally used (deprecated).</summary>     
		Duration = DataType.DURATION,
		/// <summary>Internally used (deprecated).</summary>     
		DByteEbcdic = DataType.DBYTEEBCDIC,
		/// <summary>Data type LONG ASCII (deprecated).</summary> 
		LongA = DataType.LONGA,
		/// <summary>Data type LONG EBCDIC (deprecated).</summary>   
		LongE = DataType.LONGE,
		/// <summary>Data type LONG BYTE (deprecated).</summary>   
		LongB = DataType.LONGB,
		/// <summary>Internally used (deprecated).</summary>  
		LongDB = DataType.LONGDB,
		/// <summary>Data type BOOLEAN.</summary>    
		Boolean = DataType.BOOLEAN,
		/// <summary>Data type CHAR UNICODE.</summary>    
		Unicode = DataType.UNICODE,
		/// <summary>Internally used (deprecated).</summary>    
		DTFiller1 = DataType.DTFILLER1,
		/// <summary>Internally used (deprecated).</summary>    
		DTFiller2 = DataType.DTFILLER2,
		/// <summary>Internally used (deprecated).</summary>    
		DTFiller3 = DataType.DTFILLER3,
		/// <summary>Internally used (deprecated).</summary>    
		DTFiller4 = DataType.DTFILLER4,
		/// <summary>Data type SMALLINT.</summary>    
		SmallInt = DataType.SMALLINT,
		/// <summary>Data type INTEGER.</summary>   
		Integer = DataType.INTEGER,
		/// <summary>Data type Data type VARCHAR ASCII.</summary>   
		VarCharA = DataType.VARCHARA,
		/// <summary>Data type Data type VARCHAR EBCDIC (deprecated).</summary>   
		VarCharE = DataType.VARCHARE,
		/// <summary>Data type Data type VARCHAR BYTE.</summary>    
		VarCharB = DataType.VARCHARB,
		/// <summary>Data type LONG UNICODE.</summary>   
		StrUni = DataType.STRUNI,
		/// <summary>Data type LONG UNICODE (deprecated).</summary>   
		LongUni = DataType.LONGUNI,
		/// <summary>Data type VARCHAR UNICODE.</summary>   
		VarCharUni = DataType.VARCHARUNI,
		/// <summary>Data type used for C++ Stored Procedures.</summary>  
		Udt = DataType.UDT,
		/// <summary>Data type used for C++ Stored Procedures.</summary>   
		AbapStream = DataType.ABAPTABHANDLE,
		/// <summary>Data type used for C++ Stored Procedures.</summary>
		Dwyde = DataType.DWYDE,
	}
}
