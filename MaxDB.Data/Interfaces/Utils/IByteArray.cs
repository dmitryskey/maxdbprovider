//-----------------------------------------------------------------------------------------------
// <copyright file="IByteArray.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data.Interfaces.Utils
{
    using System.Text;

    internal interface IByteArray
    {
        /// <summary>
        /// Gets a byte array length.
        /// </summary>
        int Length
        {
            get;
        }

        /// <summary>
        /// Gets a value indicating whether the data array is a little-endian or big-endian.
        /// </summary>
        bool Swapped
        {
            get;
        }

        /// <summary>
        /// Gets or sets data buffer offset.
        /// </summary>
        int Offset
        {
            get; set;
        }

        /// <summary>
        /// Creates a shallow copy of the <see cref="ByteArray"/>.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <param name="swapMode">Swap mode.</param>
        /// <returns>A shallow copy of the <see cref="ByteArray"/>.</returns>
        IByteArray Clone(int offset = 0, bool? swapMode = null);

        /// <summary>
        /// Gets byte array data.
        /// </summary>
        /// <returns>A <see cref="byte[]"/> array.</returns>
        byte[] GetArrayData();

        /// <summary>
        /// Reads the specified number of bytes from the current byte array.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <param name="len">Number of bytes to read.</param>
        /// <returns>A byte array containing data read from the underlying byte array.</returns>
        byte[] ReadBytes(int offset, int len);

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        void WriteBytes(byte[] values, int offset);

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Number of bytes to write.</param>
        void WriteBytes(byte[] values, int offset, int len);

        /// <summary>
        /// Write bytes to the array.
        /// </summary>
        /// <param name="values">Byte array.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Number of bytes to write.</param>
        /// <param name="filler">Bytes to fill out array.</param>
        void WriteBytes(byte[] values, int offset, int len, byte[] filler);

        /// <summary>
        /// Read a byte from the array.
        /// </summary>
        /// <param name="offset">Byte array offset.</param>
        /// <returns>The byte value.</returns>
        byte ReadByte(int offset);

        /// <summary>
        /// Write a byte to the byte array.
        /// </summary>
        /// <param name="value">Byte value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteByte(byte value, int offset);

        /// <summary>
        /// Read <see cref="ushort"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned short.</returns>
        ushort ReadUInt16(int offset);

        /// <summary>
        /// Write <see cref="ushort"/>.
        /// </summary>
        /// <param name="value">Unsigned short value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteUInt16(ushort value, int offset);

        /// <summary>
        /// Read <see cref="short"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed short.</returns>
        short ReadInt16(int offset);

        /// <summary>
        /// Write <see cref="short"/>.
        /// </summary>
        /// <param name="value">Signed short value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteInt16(short value, int offset);

        /// <summary>
        /// Read <see cref="uint"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned int.</returns>
        uint ReadUInt32(int offset);

        /// <summary>
        /// Write <see cref="uint"/>.
        /// </summary>
        /// <param name="value">Unsigned int value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteUInt32(uint value, int offset);

        /// <summary>
        /// Read <see cref="int"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed int.</returns>
        int ReadInt32(int offset);

        /// <summary>
        /// Write <see cref="int"/>.
        /// </summary>
        /// <param name="value">Signed int value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteInt32(int value, int offset);

        /// <summary>
        /// Read <see cref="ulong"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Unsigned long.</returns>
        ulong ReadUInt64(int offset);

        /// <summary>
        /// Read <see cref="long"/>.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <returns>Signed long.</returns>
        long ReadInt64(int offset);

        /// <summary>
        /// Write <see cref="long"/>.
        /// </summary>
        /// <param name="value">Signed long value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteInt64(long value, int offset);

        /// <summary>
        /// Read ASCII string.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String lenght.</param>
        /// <returns>ASCII string.</returns>
        string ReadAscii(int offset, int len);

        /// <summary>
        /// Read string.
        /// </summary>
        /// <param name="encoding">String encoding.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String length.</param>
        /// <returns>String in the given encoding.</returns>
        string ReadEncoding(Encoding encoding, int offset, int len);

        /// <summary>
        /// Write ASCII string.
        /// </summary>
        /// <param name="value">ASCII string.</param>
        /// <param name="offset">Array offset.</param>
        void WriteAscii(string value, int offset);

        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="encoding">String encoding.</param>
        /// <param name="value">Strign value.</param>
        /// <param name="offset">Array offset.</param>
        void WriteEncoding(Encoding encoding, string value, int offset);

        /// <summary>
        /// Read unicode string.
        /// </summary>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">String length to read.</param>
        /// <returns>Unicode stirng.</returns>
        string ReadUnicode(int offset, int len);

        /// <summary>
        /// Write unicode string.
        /// </summary>
        /// <param name="value">Unicode string.</param>
        /// <param name="offset">Array offset.</param>
        /// <param name="len">Stirng length.</param>
        void WriteUnicode(string value, int offset, int len = -1);
    }
}