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
