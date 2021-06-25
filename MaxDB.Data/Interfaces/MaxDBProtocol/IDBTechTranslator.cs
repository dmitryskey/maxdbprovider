//-----------------------------------------------------------------------------------------------
// <copyright file="IDBTechTranslator.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General License for more details.
//
// You should have received a copy of the GNU General License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.Interfaces.MaxDBProtocol
{
    using System;
    using System.IO;
    using MaxDB.Data.Interfaces.Utils;
    using MaxDB.Data.Utils;

    /// <summary>
    /// DB Tech translator interface.
    /// </summary>
    internal interface IDBTechTranslator
    {
        /// <summary>
        /// Gets column buffer position.
        /// </summary>
        int BufPos { get; }

        /// <summary>
        /// Gets column precision.
        /// </summary>
        int Precision { get; }

        /// <summary>
        /// Gets column buffer scale.
        /// </summary>
        int Scale { get; }

        /// <summary>
        /// Gets column physical lenght.
        /// </summary>
        int PhysicalLength { get; }

        /// <summary>
        /// Gets or sets column index.
        /// </summary>
        int ColumnIndex { get; set; }

        string ColumnTypeName { get; }

        Type ColumnDataType { get; }

        MaxDBType ColumnProviderType { get; }

        int ColumnDisplaySize { get; }

        string ColumnName { get; set; }

        Stream GetASCIIStream(ISqlParameterController controller, IByteArray mem, IByteArray longData);

        Stream GetUnicodeStream(ISqlParameterController controller, IByteArray mem, IByteArray longData);

        Stream GetBinaryStream(ISqlParameterController controller, IByteArray mem, IByteArray longData);

        bool GetBoolean(IByteArray mem);

        byte GetByte(ISqlParameterController controller, IByteArray mem);

        byte[] GetBytes(ISqlParameterController controller, IByteArray mem);

        long GetBytes(ISqlParameterController controller, IByteArray mem, long fldOffset, byte[] buffer, int bufferoffset, int length);

        DateTime GetDateTime(IByteArray mem);

        double GetDouble(IByteArray mem);

        float GetFloat(IByteArray mem);

        BigDecimal GetBigDecimal(IByteArray mem);

        decimal GetDecimal(IByteArray mem);

        short GetInt16(IByteArray mem);

        int GetInt32(IByteArray mem);

        long GetInt64(IByteArray mem);

        object GetValue(ISqlParameterController controller, IByteArray mem);

        object[] GetValues(IByteArray mem);

        string GetString(ISqlParameterController controller, IByteArray mem);

        long GetChars(ISqlParameterController controller, IByteArray mem, long fldOffset, char[] buffer, int bufferoffset, int length);

        bool IsCaseSensitive { get; }

        bool IsCurrency { get; }

        bool IsDefinitelyWritable { get; }

        bool IsInput { get; }

        bool IsOutput { get; }

        bool IsLongKind { get; }

        bool IsTextualKind { get; }

        bool IsDBNull(IByteArray mem);

        int CheckDefineByte(IByteArray mem);

        bool IsNullable { get; }

        void Put(IMaxDBDataPart dataPart, object data);

        object TransObjectForInput(object value);

        object TransCharacterStreamForInput(Stream stream, int length);

        object TransBinaryStreamForInput(Stream stream, int length);

        object TransBigDecimalForInput(BigDecimal bigDecimal);

        object TransBooleanForInput(bool val);

        object TransByteForInput(byte val);

        object TransBytesForInput(byte[] val);

        object TransDateTimeForInput(DateTime val);

        object TransTimeSpanForInput(TimeSpan val);

        object TransDoubleForInput(double val);

        object TransFloatForInput(float val);

        object TransInt16ForInput(short val);

        object TransInt32ForInput(int val);

        object TransInt64ForInput(long val);

        object TransStringForInput(string val);

        double BigDecimal2Double(BigDecimal bd);

        float BigDecimal2Float(BigDecimal bd);

        decimal BigDecimal2Decimal(BigDecimal bd);

        short BigDecimal2Int16(BigDecimal bd);

        int BigDecimal2Int32(BigDecimal bd);

        long BigDecimal2Int64(BigDecimal bd);

        void SetProcParamInfo(IDBProcParameterInfo info);
    }
}