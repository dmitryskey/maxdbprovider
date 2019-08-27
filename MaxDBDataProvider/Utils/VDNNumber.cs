// Copyright © 2005-2006 Dmitry S. Kataev
// Copyright © 2002-2003 SAP AG
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
    using System.Numerics;
    using System.Text;
    using MaxDB.Data.MaxDBProtocol;

    internal abstract class VDNNumber
    {
        private const int iZeroExpValue = 128;
        private const int iTensComplement = 9;
        private const int iNumberDigits = 38;
        private const string strZeroString = "0000000000000000000000000000000000000000000000000000000000000000";

        public static byte[] BigDecimal2Number(BigDecimal dec) => BigDecimal2Number(dec, iNumberDigits);

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
                res = ((BigInteger)val).ToString();
            }
            else
            {
                string unsignedIntVal = BigInteger.Abs(val.UnscaledValue).ToString();
                string prefix = (val < 0 ? "-0." : "0.");
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

        public static byte[] BigDecimal2Number(BigDecimal dec, int validDigits)
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
                return new byte[] { (byte)iZeroExpValue };
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

        public static byte[] Long2Number(long val)
        {
            bool isNegative = false;
            int negativeVal = 1;
            byte[] number;
            char[] scratch = new char[iNumberDigits + 1];
            char digit;
            int scratchPos = iNumberDigits - 1;
            int exponent;

            if (val == 0)
            {
                return new byte[] { iZeroExpValue };
            }

            if (val < 0)
            {
                negativeVal = -1;
                isNegative = true;
            }

            /*
             * calculate digits
             */
            while (val != 0)
            {
                digit = (char)(negativeVal * (val % 10));
                scratch[scratchPos] = digit;
                val /= 10;
                scratchPos--;
            }

            exponent = iNumberDigits - scratchPos - 1;
            scratchPos++;
            number = new byte[iNumberDigits - scratchPos + 1];
            PackDigits(scratch, scratchPos, iNumberDigits - scratchPos, isNegative, number);

            exponent = isNegative ? 64 - exponent : exponent + 192;

            number[0] = (byte)exponent;
            return number;
        }

        public static BigDecimal Number2BigDecimal(byte[] rawNumber)
        {
            BigDecimal result = null;
            int characteristic;
            int digitCount = (rawNumber.Length - 1) * 2;
            int exponent;
            byte[] digits;
            int lastSignificant = 2;
            string numberString;
            try
            {
                characteristic = rawNumber[0] & 0xff;
                if (characteristic == iZeroExpValue)
                {
                    return new BigDecimal(0);
                }

                digits = new byte[digitCount + 2];
                if (characteristic < iZeroExpValue)
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

                        digits[i * 2] = (byte)(iTensComplement - val + '0');
                        val = (((char)rawNumber[i]) & 0x0f);
                        if (val != 0)
                        {
                            lastSignificant = i * 2 + 1;
                        }

                        digits[i * 2 + 1] = (byte)(iTensComplement - val + '0');
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
                        val = (((char)rawNumber[i]) & 0x0f);
                        if (val != 0)
                        {
                            lastSignificant = i * 2 + 1;
                        }

                        digits[i * 2 + 1] = (byte)(val + '0');
                    }
                }

                numberString = Encoding.ASCII.GetString(digits, 0, lastSignificant + 1);
                result = new BigDecimal(numberString);
                return result.MovePointRight(exponent);
            }
            catch (Exception)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONVDNnumber, Consts.ToHexString(rawNumber)));
            }
        }

        public static long Number2Long(byte[] rawNumber)
        {
            long result = 0;
            int characteristic;
            int exponent;
            bool isNegative;
            int numberDigits = rawNumber.Length * 2 - 2;

            characteristic = rawNumber[0] & 0xFF;
            if (characteristic == iZeroExpValue)
            {
                return 0;
            }

            if (characteristic < iZeroExpValue)
            {
                exponent = -(characteristic - 64);
                if (exponent < 0 || exponent > numberDigits)
                {
                    return (long)(Number2BigDecimal(rawNumber));
                }

                isNegative = true;
                for (int i = 0; i < exponent; i++)
                {
                    int val = rawNumber[i / 2 + 1];
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
                    result += iTensComplement - val;
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
                    int val = rawNumber[i / 2 + 1];
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

        public static int Number2Int(byte[] rawBytes) => (int)Number2Long(rawBytes);

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

            /*
             * pack digits into bytes
             */
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

        public static string Number2String(byte[] number, bool fixedtype, int logicalLength, int frac)
        {
            int characteristic;

            try
            {
                characteristic = number[0] & 0xFF;
                if (characteristic == iZeroExpValue)
                {
                    return "0";
                }

                char[] digits = new char[logicalLength];
                int exponent;
                int lastsignificant = 0;
                bool isnegative = false;
                if (characteristic < iZeroExpValue)
                {
                    isnegative = true;
                    exponent = -(characteristic - 64);
                    for (int i = 0; i < logicalLength; i++)
                    {
                        int v1 = i % 2 == 0 ? (number[1 + i / 2] & 0xFF) >> 4 : number[1 + i / 2] & 0xF;
                        if (v1 != 0)
                        {
                            lastsignificant = i;
                        }

                        digits[i] = (char)((9 - v1) + '0');
                    }

                    digits[lastsignificant]++;
                }
                else
                {
                    exponent = characteristic - 192;
                    for (int i = 0; i < logicalLength; i++)
                    {
                        int v1 = i % 2 == 0 ? (number[1 + i / 2] & 0xFF) >> 4 : number[1 + i / 2] & 0xF;
                        if (v1 != 0)
                        {
                            lastsignificant = i;
                        }

                        digits[i] = (char)((v1) + '0');
                    }
                }

                string sign = (isnegative ? "-" : string.Empty);
                string numberstr = new string(digits, 0, lastsignificant + 1);
                if (fixedtype)
                {
                    if (exponent > 0)
                    {
                        if (numberstr.Length < logicalLength)
                        {
                            numberstr = numberstr + strZeroString.Substring(0, logicalLength - numberstr.Length);
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

                        return sign + "0." + strZeroString.Substring(0, -exponent) + numberstr + strZeroString.Substring(0, zeroend);
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
                            ? sign + numberstr + strZeroString.Substring(0, exponent - numberstr.Length)
                            : sign + numberstr.Substring(0, exponent) + "." + numberstr.Substring(exponent);
                    }
                }
            }
            catch (Exception)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONVERSIONVDNnumber, Consts.ToHexString(number)));
            }
        }
    }
}
