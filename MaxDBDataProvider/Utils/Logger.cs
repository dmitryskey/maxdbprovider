// Copyright © 2005-2018 Dmitry S. Kataev
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.Utilities
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using MaxDB.Data.MaxDBProtocol;

    internal enum MaxDBTraceLevel
    {
        None = 0,
        SqlOnly = 1,
        Full = 2
    }

    internal class MaxDBTraceSwitch : Switch
    {
        public MaxDBTraceSwitch(string displayName, string description)
            : base(displayName, description)
        {
        }

        public MaxDBTraceLevel Level
        {
            get
            {
                switch (this.SwitchSetting)
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

        public bool TraceSQL => this.Level != MaxDBTraceLevel.None;

        public bool TraceFull => this.Level == MaxDBTraceLevel.Full;
    }

    internal class MaxDBLogger
    {
        public const int
NumSize = 4;
        public const int
TypeSize = 16;
        public const int
LenSize = 10;
        public const int
InputSize = 10;
        public const int
DataSize = 256;

        public const string Null = "NULL";

        private readonly MaxDBTraceSwitch mSwitcher = new MaxDBTraceSwitch("TraceLevel", "Trace Level");

        public MaxDBLogger()
        {
        }

        public bool TraceSQL => this.mSwitcher.TraceSQL;

        public bool TraceFull => this.mSwitcher.TraceFull;

        public void SqlTrace(DateTime dt, string msg)
        {
            if (this.mSwitcher.TraceSQL)
            {
                Trace.WriteLine(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " " + msg);
            }
        }

        public void SqlTraceParseInfo(DateTime dt, object objInfo)
        {
            var parseInfo = (MaxDBParseInfo)objInfo;
            if (this.mSwitcher.TraceSQL)
            {
                if (parseInfo.ParamInfo != null && parseInfo.ParamInfo.Length > 0)
                {
                    this.SqlTrace(dt, "PARAMETERS:");
                    this.SqlTrace(dt, "I   T              L    P   IO    N");
                    foreach (var info in parseInfo.ParamInfo)
                    {
                        Trace.Write(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " ");

                        SqlTraceTransl(info);

                        if (FunctionCode.IsQuery(parseInfo.FuncCode))
                        {
                            if (!info.IsOutput)
                            {
                                if (info.IsInput)
                                {
                                    if (info.IsOutput)
                                    {
                                        Trace.Write(" INOUT "); // ... two in one. We must reduce the overall number !!!
                                    }
                                    else
                                    {
                                        Trace.Write(" IN    ");
                                    }
                                }
                                else
                                {
                                    Trace.Write(" OUT   ");
                                }
                            }
                        }
                        else
                        {
                            if (info.IsInput)
                            {
                                if (info.IsOutput)
                                {
                                    Trace.Write(" INOUT "); // ... two in one. We must reduce the overall number !!!
                                }
                                else
                                {
                                    Trace.Write(" IN    ");
                                }
                            }
                            else
                            {
                                Trace.Write(" OUT   ");
                            }
                        }

                        Trace.WriteLine(info.ColumnName);
                    }
                }

                if (parseInfo.ColumnInfo != null && parseInfo.ColumnInfo.Length > 0)
                {
                    this.SqlTrace(dt, "COLUMNS:");
                    this.SqlTrace(dt, "I   T              L           P           N");
                    foreach (var info in parseInfo.ColumnInfo)
                    {
                        Trace.Write(dt.ToString(Consts.TimeStampFormat, CultureInfo.InvariantCulture) + " ");
                        SqlTraceTransl(info);
                        Trace.WriteLine(info.ColumnName);
                    }
                }
            }
        }

        public void SqlTraceDataHeader(DateTime dt) => this.SqlTrace(dt, "I".PadRight(NumSize) + "T".PadRight(TypeSize) + "L".PadRight(LenSize) + "I".PadRight(InputSize) + "DATA");

        private static void SqlTraceTransl(MaxDBTranslators.DBTechTranslator info)
        {
            Trace.Write(info.ColumnIndex.ToString(CultureInfo.InvariantCulture).PadRight(4));
            Trace.Write(info.ColumnTypeName.PadRight(15));
            Trace.Write((info.PhysicalLength - 1).ToString(CultureInfo.InvariantCulture).PadRight(12));
            Trace.Write(info.Precision.ToString(CultureInfo.InvariantCulture).PadRight(12));
        }

        public void Flush()
        {
            if (this.mSwitcher.TraceSQL)
            {
                Trace.Flush();
            }
        }
    }
}
