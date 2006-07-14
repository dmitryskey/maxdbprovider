using System;
using System.Diagnostics;
using System.IO;
using System.Configuration;
using MaxDB.Data.MaxDBProtocol;
using System.Globalization;

namespace MaxDB.Data.Utilities
{
	public enum MaxDBTraceLevel
	{
		None = 0,
		SqlOnly = 1,
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
					case (int)MaxDBTraceLevel.SqlOnly:
						return MaxDBTraceLevel.SqlOnly;
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

		private MaxDBTraceSwitch mSwitcher = new MaxDBTraceSwitch("TraceLevel", "Trace Level");
#if !SAFE
		private IntPtr mProperties = IntPtr.Zero;
		private string strLogName;
		private MaxDBConnection dbConnection;
#endif

		public MaxDBLogger()
		{
		}

#if !SAFE
		public MaxDBLogger(MaxDBConnection conn)
		{
            if (mSwitcher.TraceSQL)
			{
				dbConnection = conn;

				mProperties = UnsafeNativeMethods.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mProperties, "SQL", "1");
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mProperties, "TIMESTAMP", "1");

				strLogName = Path.GetTempPath() + "adonetlog.html";
 				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mProperties, "FILENAME", "\"" + strLogName + "\"");
                if (mSwitcher.TraceFull)
					UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mProperties, "PACKET", "1");
				UnsafeNativeMethods.SQLDBC_Environment_setTraceOptions(dbConnection.mEnviromentHandler, mProperties);
			}
		}
#endif

		public bool TraceSQL
		{
			get
			{
                return mSwitcher.TraceSQL;
			}
		}

#if SAFE
		public bool TraceFull
		{
			get
			{
                return mSwitcher.TraceFull;
			}
		}
#endif // SAFE

		public void SqlTrace(DateTime dt, string msg)
		{
#if SAFE
			if (mSwitcher.TraceSQL)
                Trace.WriteLine(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " " + msg);
#endif // SAFE
        }

#if SAFE
		public void SqlTraceParseInfo(DateTime dt, object objInfo)
		{
			MaxDBParseInfo parseInfo = (MaxDBParseInfo)objInfo;
			if (mSwitcher.TraceSQL)
			{
				if (parseInfo.ParamInfo != null && parseInfo.ParamInfo.Length > 0)
				{
					SqlTrace(dt, "PARAMETERS:");
					SqlTrace(dt, "I   T              L    P   IO    N");
					foreach (DBTechTranslator info in parseInfo.ParamInfo)
					{
                        Trace.Write(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " ");

						SqlTraceTransl(info);

						if (FunctionCode.IsQuery(parseInfo.FuncCode))
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
					SqlTrace(dt, "I   T              L           P           N");
					foreach(DBTechTranslator info in parseInfo.ColumnInfo)
					{
                        Trace.Write(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " ");
						SqlTraceTransl(info);
						Trace.WriteLine(info.ColumnName);
					}
				}
            }
        }

		public void SqlTraceDataHeader(DateTime dt)
		{
			SqlTrace(dt, "I".PadRight(NumSize) + "T".PadRight(TypeSize) + "L".PadRight(LenSize) + "I".PadRight(InputSize) + "DATA");
        }
#endif // SAFE

#if SAFE
		private static void SqlTraceTransl(DBTechTranslator info)
		{
            Trace.Write(info.ColumnIndex.ToString(CultureInfo.InvariantCulture).PadRight(4));
			Trace.Write(info.ColumnTypeName.PadRight(15));
            Trace.Write((info.PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(12));
            Trace.Write(info.Precision.ToString(CultureInfo.InvariantCulture).PadRight(12));
        }
#endif // SAFE

        public void Flush()
		{
            if (mSwitcher.TraceSQL)
			{
#if SAFE
				Trace.Flush();
#else

                if (!File.Exists(strLogName))
                    return;
                string tmpFile = Path.GetTempFileName();
                File.Copy(strLogName, tmpFile, true);
                StreamReader sr = new StreamReader(tmpFile);
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
                    while (line != null);
                }
                sr.Close();
                File.Delete(tmpFile);

                Trace.Flush();
#endif // SAFE
            }
		}

		#region IDisposable Members

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing)
		{
			if (disposing)
			{
#if !SAFE
				Flush();
				if (dbConnection != null && mSwitcher.TraceSQL && mProperties != IntPtr.Zero)
					UnsafeNativeMethods.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(mProperties);
#endif // !SAFE
			}
		}

		#endregion
	}
}
