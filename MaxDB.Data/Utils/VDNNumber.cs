//-----------------------------------------------------------------------------------------------
// <copyright file="VDNNumber.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
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

namespace MaxDB.Data.Utils
{
    using System;
    using System.Globalization;
    using System.Numerics;
    using System.Text;
    using MaxDB.Data.MaxDBProtocol;

    /// <summary>
    /// VDN Number class.
    /// </summary>
    internal abstract class VDNNumber
    {
        private const int ZeroExpValue = 128;
        private const int TensComplement = 9;
        private const int NumberDigits = 38;
        private const string ZeroString = "0000000000000000000000000000000000000000000000000000000000000000";

        /// <summary>
        /// Convert <see cref="BigDecimal"/> to string.
        /// </summary>
        /// <param name="val">Big Decimal number.</param>
        /// <returns>String presentation.</returns>
        public static string BigDecimal2PlainString(BigDecimal val)
        {
            string res;
            int scale = val.Scale;
            if (scale < 0)
            {
                val = val.SetScale(0);
                scale = 0;
            }

            if (scale == 0)
            {
                res = ((BigInteger)val).ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                string unsignedIntVal = BigInteger.Abs(val.UnscaledValue).ToString(CultureInfo.InvariantCulture);
                string prefix = val < 0 ? "-0." : "0.";
                int pointPos = unsignedIntVal.Length - scale;
                if (pointPos == 0)
                {
                    res = prefix + unsignedIntVal;
                }
                else if (pointPos > 0)
                {
                    var buf = new StringBuilder(unsignedIntVal);
                    buf.Insert(pointPos, '.');
                    if (val < 0)
                    {
                        buf.Insert(0, '-');
                    }

                    res = buf.ToString();
                }
                else
                {
                    var buf = new StringBuilder(3 - pointPos + unsignedIntVal.Length);
                    buf.Append(prefix);
                    for (int i = 0; i < -pointPos; i++)
                    {
                        buf.Append('0');
                    }

                    buf.Append(unsignedIntVal);
                    res = buf.ToString();
                }
            }

            return res;
        }

        /// <summary>
        /// Convert <see cref="BigDecimal"/> to byte array.
        /// </summary>
        /// <param name="dec">Big decimal value.</param>
        /// <param name="validDigits">Number of digits.</param>
        /// <returns>Byte array.</returns>
        public static byte[] BigDecimal2Number(BigDecimal dec, int validDigits = NumberDigits)
        {
            byte[] number;
            string plain = BigDecimal2PlainString(dec);
            int scale = (dec.Scale < 0) ? 0 : dec.Scale;
            char[] chars = plain.ToCharArray();
            bool isNegative;
            int firstDigit;
            int exponent;
            int digitCount = chars.Length;

            if (chars[0] == '-')
            {
                isNegative = true;
                firstDigit = 1;
            }
            else
            {
                isNegative = false;
                firstDigit = 0;
            }

            while ((chars[firstDigit] == '0' || chars[firstDigit] == '.') && firstDigit < digitCount - 1)
            {
                firstDigit++;
            }

            exponent = chars.Length - firstDigit - scale;
            digitCount = chars.Length - firstDigit;
            if (digitCount == 1 && chars[firstDigit] == '0')
            {
                return new byte[] { (byte)ZeroExpValue };
            }

            if (exponent > 0 && scale > 0)
            {
                // adjust place taken by decimal point
                exponent--;
                digitCount--;
                Array.Copy(chars, chars.Length - scale, chars, chars.Length - scale - 1, scale);
            }

            if (digitCount > validDigits)
            {
                if (chars[firstDigit + validDigits] >= '5')
                {
                    chars[firstDigit + validDigits]++;
                }

                digitCount = validDigits;
            }

            for (int i = firstDigit; i < digitCount + firstDigit; ++i)
            {
                chars[i] -= '0';
            }

            number = new byte[digitCount + 1];
            PackDigits(chars, firstDigit, digitCount, isNegative, number);

            exponent = isNegative ? 64 - exponent : exponent + 192;

            number[0] = (byte)exponent;
            return number;
        }

        /// <summary>
        /// Convert <see cref="long"/> to byte array.
        /// </summary>
        /// <param name="val">Long value.</param>
        /// <returns>Byte array.</returns>
        public static byte[] Long2Number(long val)
        {
            bool isNegative = false;
            int negativeVal = 1;
            byte[] number;
            char[] scratch = new char[NumberDigits + 1];
            char digit;
            int scratchPos = NumberDigits - 1;
            int exponent;

            if (val == 0)
            {
                return new byte[] { ZeroExpValue };
            }

            if (val < 0)
            {
                negativeVal = -1;
                isNegative = true;
            }

            // calculate digits
            while (val != 0)
            {
                digit = (char)(negativeVal * (val % 10));
                scratch[scratchPos] = digit;
                val /= 10;
                scratchPos--;
            }

            exponent = NumberDigits - scratchPos - 1;
            scratchPos++;
            number = new byte[NumberDigits - scratchPos + 1];
            PackDigits(scratch, scratchPos, NumberDigits - scratchPos, isNegative, number);

            exponent = isNegative ? 64 - exponent : exponent + 192;

            number[0] = (byte)exponent;
            return number;
        }

        /// <summary>
        /// Convert VDN Number to <see cref="BigDecimal"/>.
        /// </summary>
        /// <param name="rawNumber">VDN Number byte array.</param>
        /// <returns>Big Decimal.</returns>
        public static BigDecimal Number2BigDecimal(byte[] rawNumber)
        {
            int characteristic;
            int digitCount = (rawNumber.Length - 1) * 2;
            int exponent;
            byte[] digits;
            int lastSignificant = 2;
            string numberString;
            try
            {
                characteristic = rawNumber[0] & 0xff;
                if (characteristic == ZeroExpValue)
                {
                    return new BigDecimal(0);
                }

                digits = new byte[digitCount + 2];
                if (characteristic < ZeroExpValue)
                {
                    exponent = -(characteristic - 64);
                    digits[0] = (byte)'-';
                    digits[1] = (byte)'.';

                    for (int i = 1; i < rawNumber.Length; i++)
                    {
                        int val;
                        val = (((char)rawNumber[i]) & 0xff) >> 4;
                        if (val != 0)
                        {
                            lastSignificant = i * 2;
                        }

                        digits[i * 2] = (byte)(TensComplement - val + '0');
                        val = ((char)rawNumber[i]) & 0x0f;
                        if (val != 0)
                        {
                            lastSignificant = (i * 2) + 1;
                        }

                        digits[(i * 2) + 1] = (byte)(TensComplement - val + '0');
                    }

                    digits[lastSignificant]++;
                }
                else
                {
                    exponent = characteristic - 192;
                    digits[0] = (byte)'0';
                    digits[1] = (byte)'.';
                    for (int i = 1; i < rawNumber.Length; ++i)
                    {
                        int val;
                        val = (((char)rawNumber[i]) & 0xff) >> 4;
                        if (val != 0)
                        {
                            lastSignificant = i * 2;
                        }

                        digits[i * 2] = (byte)(val + '0');
                        val = ((char)rawNumber[i]) & 0x0f;
                        if (val != 0)
                        {
                            lastSignificant = (i * 2) + 1;
                        }

                        digits[(i * 2) + 1] = (byte)(val + '0');
                    }
                }

                numberString = Encoding.ASCII.GetString(digits, 0, lastSignificant + 1);
                BigDecimal result = new BigDecimal(numberString);
                return result.MovePointRight(exponent);
            }
            catch (Exception)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONVDNnumber, Consts.ToHexString(rawNumber)));
            }
        }

        /// <summary>
        /// Convert VDN Number to <see cref="long"/>.
        /// </summary>
        /// <param name="rawNumber">VDN Number byte array.</param>
        /// <returns>64-bit Long integer.</returns>
        public static long Number2Long(byte[] rawNumber)
        {
            long result = 0;
            int characteristic;
            int exponent;
            bool isNegative;
            int numberDigits = (rawNumber.Length * 2) - 2;

            characteristic = rawNumber[0] & 0xFF;
            if (characteristic == ZeroExpValue)
            {
                return 0;
            }

            if (characteristic < ZeroExpValue)
            {
                exponent = -(characteristic - 64);
                if (exponent < 0 || exponent > numberDigits)
                {
                    return (long)Number2BigDecimal(rawNumber);
                }

                isNegative = true;
                for (int i = 0; i < exponent; i++)
                {
                    int val = rawNumber[(i / 2) + 1];
                    if ((i % 2) == 0)
                    {
                        val &= 0xF0;
                        val >>= 4;
                    }
                    else
                    {
                        val &= 0x0F;
                    }

                    result *= 10;
                    result += TensComplement - val;
                }

                result++;
            }
            else
            {
                exponent = characteristic - 192;
                if (exponent < 0 || exponent > numberDigits)
                {
                    return (long)Number2BigDecimal(rawNumber);
                }

                isNegative = false;
                for (int i = 0; i < exponent; i++)
                {
                    int val = rawNumber[(i / 2) + 1];
                    if ((i % 2) == 0)
                    {
                        val &= 0xF0;
                        val >>= 4;
                    }
                    else
                    {
                        val &= 0x0F;
                    }

                    result *= 10;
                    result += val;
                }
            }

            result *= isNegative ? -1 : 1;

            return result;
        }

        /// <summary>
        /// Convert VDN Number to <see cref="int"/>.
        /// </summary>
        /// <param name="rawBytes">VDN Number byte array.</param>
        /// <returns>32-bit integer.</returns>
        public static int Number2Int(byte[] rawBytes) => (int)Number2Long(rawBytes);

        /// <summary>
        /// Convert VDN Number to <see cref="string"/>.
        /// </summary>
        /// <param name="number">VDN Number byte array.</param>
        /// <param name="fixedtype">Flag indicating whether the number is the fixed decimal point one.</param>
        /// <param name="logicalLength">Logical length.</param>
        /// <param name="frac">Faction part length.</param>
        /// <returns>String presentation.</returns>
        public static string Number2String(byte[] number, bool fixedtype, int logicalLength, int frac)
        {
            int characteristic;

            try
            {
                characteristic = number[0] & 0xFF;
                if (characteristic == ZeroExpValue)
                {
                    return "0";
                }

                char[] digits = new char[logicalLength];
                int exponent;
                int lastsignificant = 0;
                bool isnegative = false;
                if (characteristic < ZeroExpValue)
                {
                    isnegative = true;
                    exponent = -(characteristic - 64);
                    for (int i = 0; i < logicalLength; i++)
                    {
                        int v1 = i % 2 == 0 ? (number[1 + (i / 2)] & 0xFF) >> 4 : number[1 + (i / 2)] & 0xF;
                        if (v1 != 0)
                        {
                            lastsignificant = i;
                        }

                        digits[i] = (char)(9 - v1 + '0');
                    }

                    digits[lastsignificant]++;
                }
                else
                {
                    exponent = characteristic - 192;
                    for (int i = 0; i < logicalLength; i++)
                    {
                        int v1 = i % 2 == 0 ? (number[1 + (i / 2)] & 0xFF) >> 4 : number[1 + (i / 2)] & 0xF;
                        if (v1 != 0)
                        {
                            lastsignificant = i;
                        }

                        digits[i] = (char)(v1 + '0');
                    }
                }

                string sign = isnegative ? "-" : string.Empty;
                string numberstr = new string(digits, 0, lastsignificant + 1);
                if (fixedtype)
                {
                    if (exponent > 0)
                    {
                        if (numberstr.Length < logicalLength)
                        {
                            numberstr += ZeroString.Substring(0, logicalLength - numberstr.Length);
                        }

                        return sign + numberstr.Substring(0, exponent) + (frac != 0 ? "." + numberstr.Substring(exponent, exponent + frac) : string.Empty);
                    }
                    else
                    {
                        int zeroend = frac - (-exponent) - numberstr.Length;
                        if (zeroend < 0)
                        {
                            zeroend = 0;
                        }

                        return sign + "0." + ZeroString.Substring(0, -exponent) + numberstr + ZeroString.Substring(0, zeroend);
                    }
                }
                else
                {
                    if (exponent < -3 || exponent > 7)
                    {
                        return sign + "0." + numberstr + "E" + exponent;
                    }
                    else
                    {
                        switch (exponent)
                        {
                            case -3:
                                return sign + "0.000" + numberstr;
                            case -2:
                                return sign + "0.00" + numberstr;
                            case -1:
                                return sign + "0.0" + numberstr;
                            case 0:
                                return sign + "0." + numberstr;
                        }

                        return numberstr.Length <= exponent
                            ? sign + numberstr + ZeroString.Substring(0, exponent - numberstr.Length)
                            : sign + numberstr.Substring(0, exponent) + "." + numberstr.Substring(exponent);
                    }
                }
            }
            catch (Exception)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONVDNnumber, Consts.ToHexString(number)));
            }
        }

        private static void PackDigits(char[] digits, int start, int count, bool isNegative, byte[] number)
        {
            int lastDigit = start + count - 1;
            byte highNibble;
            byte lowNibble;

            if (isNegative)
            {
                // 10s complement
                for (int i = start; i < lastDigit; ++i)
                {
                    digits[i] = (char)(9 - digits[i]);
                }

                digits[lastDigit] = (char)(10 - digits[lastDigit]);

                // handle overflow
                int digitPos = lastDigit;
                while (digits[digitPos] == 10)
                {
                    digits[digitPos] = '\0';
                    digits[digitPos - 1]++;
                    digitPos--;
                }
            }

            // pack digits into bytes
            for (int i = 1; start <= lastDigit; ++i, start += 2)
            {
                highNibble = (byte)digits[start];
                if ((start + 1) <= lastDigit)
                {
                    lowNibble = (byte)digits[start + 1];
                }
                else
                {
                    lowNibble = 0;
                }

                number[i] = (byte)(highNibble << 4 | lowNibble);
            }
        }
    }
}
