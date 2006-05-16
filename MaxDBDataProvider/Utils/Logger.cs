using System;
using System.Diagnostics;
using System.IO;
using System.Configuration;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider.Utils
{
	public enum MaxDBTraceLevel
	{
		None = 0,
		SQLOnly = 1,
		Full = 2
	}
	/// <summary>
	/// Summary description for Logger.
	/// </summary>
	internal class MaxDBTraceSwitch : Switch
	{
		public MaxDBTraceSwitch(string displayName, string description): base(displayName, description)
		{
		}

		public MaxDBTraceLevel Level
		{
			get
			{
				switch(SwitchSetting)
				{
					case (int)MaxDBTraceLevel.None:
						return MaxDBTraceLevel.None;
					case (int)MaxDBTraceLevel.SQLOnly:
						return MaxDBTraceLevel.SQLOnly;
					case (int)MaxDBTraceLevel.Full:
						return MaxDBTraceLevel.Full;
					default:
						return MaxDBTraceLevel.None;
				}
			}
		}

		public bool TraceSQL
		{
			get
			{
				return (Level != MaxDBTraceLevel.None);
			}
		}

		public bool TraceFull
		{
			get
			{
				return (Level == MaxDBTraceLevel.Full);
			}
		}
	}

	internal class MaxDBLogger : IDisposable
	{
		public const int 
			NumSize = 4,
			TypeSize = 16,
			LenSize = 10,
			InputSize = 10,
			DataSize = 256;

		public const string Null = "NULL";

		private MaxDBConnection m_conn;
		private MaxDBTraceSwitch m_traceSwitch = new MaxDBTraceSwitch("TraceLevel", "Trace Level");
#if !SAFE
		private IntPtr m_prop = IntPtr.Zero;
		private string m_logname;
#endif

		public MaxDBLogger(MaxDBConnection conn)
		{
			m_conn = conn;
#if !SAFE
			if (m_traceSwitch.TraceSQL)
			{
				m_prop = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
				SQLDBC.SQLDBC_ConnectProperties_setProperty(m_prop, "SQL", "1");
				SQLDBC.SQLDBC_ConnectProperties_setProperty(m_prop, "TIMESTAMP", "1");

				if (ConfigurationSettings.AppSettings["SDBPath"] != null)
				{
					m_logname = ConfigurationSettings.AppSettings["SDBPath"] + "\\data\\wrk" + "\\adonetlog.html";
					SQLDBC.SQLDBC_ConnectProperties_setProperty(m_prop, "FILENAME", "adonet.html");
				}
				else
				{
					m_logname = Path.GetTempPath() + "adonetlog.html";
					SQLDBC.SQLDBC_ConnectProperties_setProperty(m_prop, "FILENAME", m_logname);
				}
				if (m_traceSwitch.TraceFull)
					SQLDBC.SQLDBC_ConnectProperties_setProperty(m_prop, "PACKET", "1");
				SQLDBC.SQLDBC_Environment_setTraceOptions(m_conn.m_envHandler, m_prop);
			}
#endif
		}

		public MaxDBTraceLevel Level
		{
			get
			{
				return m_traceSwitch.Level;	
			}
		}

		public bool TraceSQL
		{
			get
			{
				return m_traceSwitch.TraceSQL;
			}
		}

		public bool TraceFull
		{
			get
			{
				return m_traceSwitch.TraceFull;
			}
		}

		public void SqlTrace(DateTime dt, string msg)
		{
#if SAFE
			if (m_traceSwitch.TraceSQL)
				Trace.WriteLine(dt.ToString(Consts.TimeStampFormat) + " " + msg);
#endif
		}

		public void SqlTraceParseInfo(DateTime dt, object objInfo)
		{
#if SAFE
			MaxDBParseInfo parseInfo = (MaxDBParseInfo)objInfo;
			if (m_traceSwitch.TraceSQL)
			{
				if (parseInfo.ParamInfo != null && parseInfo.ParamInfo.Length > 0)
				{
					SqlTrace(dt, "PARAMETERS:");
					SqlTrace(dt, "I   T              L    P   IO    N");
					foreach (DBTechTranslator info in parseInfo.ParamInfo)
					{
						Trace.Write(dt.ToString(Consts.TimeStampFormat) + " ");

						SqlTraceTransl(info);

						if (FunctionCode.IsQuery(parseInfo.m_funcCode))
						{
							if(!info.IsOutput) 
							{
								if(info.IsInput) 
								{
									if(info.IsOutput) 
										Trace.Write(" INOUT ");// ... two in one. We must reduce the overall number !!!
									else 
										Trace.Write(" IN    ");
								} 
								else 
									Trace.Write(" OUT   ");
							}
						}
						else
						{
							if(info.IsInput) 
							{
								if(info.IsOutput) 
									Trace.Write(" INOUT ");// ... two in one. We must reduce the overall number !!!
								else 
									Trace.Write(" IN    ");
							} 
							else 
								Trace.Write(" OUT   ");
						}

						Trace.WriteLine(info.ColumnName);
					}
				}

				if (parseInfo.ColumnInfo != null && parseInfo.ColumnInfo.Length > 0)
				{
					SqlTrace(dt, "COLUMNS:");
					SqlTrace(dt, "I   T              L    P   N");
					foreach(DBTechTranslator info in parseInfo.ColumnInfo)
					{
						Trace.Write(dt.ToString(Consts.TimeStampFormat) + " ");
						SqlTraceTransl(info);
						Trace.WriteLine(info.ColumnName);
					}
				}
			}
#endif
		}

		public void SqlTraceDataHeader(DateTime dt)
		{
#if SAFE
			SqlTrace(dt, "I".PadRight(NumSize) + "T".PadRight(TypeSize) + "L".PadRight(LenSize) + "I".PadRight(InputSize) + "DATA");
#endif
		}

#if SAFE
		private void SqlTraceTransl(DBTechTranslator info)
		{
			Trace.Write(info.ColumnIndex.ToString().PadRight(4));
			Trace.Write(info.ColumnTypeName.PadRight(15));
			Trace.Write(info.PhysicalLength.ToString().PadRight(5));
			Trace.Write(info.Precision.ToString().PadRight(4));
		}
#endif

		public void Flush()
		{
#if SAFE
			if (m_traceSwitch.TraceSQL)
				Trace.Flush();
#else
			if (!File.Exists(m_logname))
				return;
			StreamReader sr = new StreamReader(m_logname);
			string header = sr.ReadLine();
			if (header != null)
			{
				string line;
				do
				{
					line = sr.ReadLine();
					if (line != null)
						Trace.WriteLine(line);
				}
				while(line != null);
			}
			sr.Close();

			Trace.Flush();
#endif
		}

		#region IDisposable Members

		public void Dispose()
		{
#if !SAFE
			if (m_conn != null && m_traceSwitch.TraceSQL && m_prop != IntPtr.Zero)
				SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(m_prop);
#endif
		}

		#endregion
	}
}
