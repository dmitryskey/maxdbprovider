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
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for MaxDBType.
	/// </summary>

	public enum MaxDBType
	{
		Fixed		= DataType.FIXED, 
		Float		= DataType.FLOAT,            
		CharA		= DataType.CHA,            
		CharE		= DataType.CHE,            
		CharB 		= DataType.CHB,            
		RowID 		= DataType.ROWID,            
		StrA  		= DataType.STRA,            
		StrE  		= DataType.STRE,            
		StrB  		= DataType.STRB,            
		StrDB 		= DataType.STRDB,            
		Date  		= DataType.DATE,           
		Time  		= DataType.TIME,           
		VFloat		= DataType.VFLOAT,           
		TimeStamp	= DataType.TIMESTAMP,           
		Unknown		= DataType.UNKNOWN,           
		Number		= DataType.NUMBER,           
		NoNumber	= DataType.NONUMBER,           
		Duration	= DataType.DURATION,           
		DByteEBCDIC = DataType.DBYTEEBCDIC,         
		LongA		= DataType.LONGA,           
		LongE		= DataType.LONGE,           
		LongB		= DataType.LONGB,          
		LongDB		= DataType.LONGDB,           
		Boolean		= DataType.BOOLEAN,           
		Unicode		= DataType.UNICODE,           
		DTFiller1	= DataType.DTFILLER1,           
		DTFiller2	= DataType.DTFILLER2,           
		DTFiller3	= DataType.DTFILLER3,           
		DTFiller4	= DataType.DTFILLER4,           
		SmallInt	= DataType.SMALLINT,           
		Integer		= DataType.INTEGER,           
		VarCharA	= DataType.VARCHARA,           
		VarCharE	= DataType.VARCHARE,           
		VarCharB	= DataType.VARCHARB,           
		StrUni		= DataType.STRUNI,           
		LongUni		= DataType.LONGUNI,           
		VarCharUni	= DataType.VARCHARUNI,          
		UDT			= DataType.UDT,           
		ABAPStream	= DataType.ABAPTABHANDLE,       
		DWyde		= DataType.DWYDE,
	}
}
