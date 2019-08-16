//	Copyright © 2005-2018 Dmitry S. Kataev
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
using System.Globalization;
using System.Numerics;

namespace MaxDB.Data.Utilities
{
    internal class BigDecimal
    {
        private string strSeparator = CultureInfo.InvariantCulture.NumberFormat.CurrencyDecimalSeparator;

        public BigDecimal() => UnscaledValue = new BigInteger(0);

        public BigDecimal(long num) => UnscaledValue = new BigInteger(num);

        public BigDecimal(ulong num) => UnscaledValue = new BigInteger(num);

        public BigDecimal(double num)
            : this(num.ToString(CultureInfo.InvariantCulture))
        {
        }

        public BigDecimal(BigInteger num)
            : this(num, 0)
        {
        }

        public BigDecimal(BigInteger num, int scale)
        {
            UnscaledValue = num;
            Scale = scale;
        }

        public BigDecimal(string num)
        {
            string int_part;
            string float_part;
            int s = 0;
            int cur_off = 0;

            // initially iScale = 0 and biNumber is null

            // empty string = 0
            if (num.Trim().Length == 0)
            {
                UnscaledValue = new BigInteger(0);
                return;
            }

            // find the decimal separator and exponent position
            num = num.Replace(CultureInfo.InvariantCulture.NumberFormat.CurrencyGroupSeparator, string.Empty);
            int dot_index = num.IndexOf(strSeparator, cur_off);
            int exp_index = num.IndexOf('e', cur_off);
            if (exp_index < 0)
            {
                exp_index = num.IndexOf('E', cur_off);
            }

            // check number format
            if (num.IndexOf(strSeparator, dot_index + 1) >= 0)
            {
                throw new FormatException();
            }

            int last_exp_index = num.IndexOf('e', exp_index + 1);
            if (last_exp_index < 0)
            {
                last_exp_index = num.IndexOf('E', exp_index + 1);
            }

            if (last_exp_index >= 0)
            {
                throw new FormatException();
            }

            // if this number is integer
            if (dot_index < 0)
            {
                // with exponent
                if (exp_index >= 0)
                {
                    // extract integer part
                    int_part = num.Substring(0, exp_index - cur_off).TrimStart('0');
                    cur_off = exp_index + 1;

                    // extract exponent value
                    if (cur_off < num.Length)
                    {
                        s = int.Parse(num.Substring(cur_off), CultureInfo.InvariantCulture);
                    }

                    if (int_part.Trim().Length == 0)
                    {
                        UnscaledValue = new BigInteger(0);
                    }
                    else
                    {
                        UnscaledValue = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
                        if (UnscaledValue != 0)
                        {
                            Scale = -s;
                        }
                    }
                }
                else
                {
                    // just get integer part
                    int_part = num.TrimStart('0');
                    if (int_part.Trim().Length == 0)
                    {
                        UnscaledValue = new BigInteger(0);
                    }
                    else
                    {
                        UnscaledValue = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
                    }
                }

                return;
            }

            // extract integer part
            int_part = num.Substring(0, dot_index - cur_off).TrimStart('0');
            cur_off = dot_index + 1;

            string float_num = int_part;

            // if exponent doesn't exist just extract float part 
            if (exp_index < 0)
            {
                float_part = num.Substring(cur_off).TrimEnd('0');
                float_num += float_part;
                if (float_num.Trim().Length == 0)
                {
                    UnscaledValue = new BigInteger(0);
                }
                else
                {
                    UnscaledValue = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
                    if (UnscaledValue != 0)
                    {
                        Scale = float_part.Length;
                    }
                }

                return;
            }

            // extract float part
            float_part = num.Substring(cur_off, exp_index - cur_off).TrimEnd('0');
            cur_off = exp_index + 1;

            // get exponent value
            if (cur_off < num.Length)
            {
                s = int.Parse(num.Substring(cur_off), CultureInfo.InvariantCulture);
            }

            float_num += float_part;
            if (float_num.Trim().Length == 0)
            {
                UnscaledValue = new BigInteger(0);
            }
            else
            {
                UnscaledValue = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
                if (UnscaledValue != 0)
                {
                    Scale = float_part.Length - s;
                }
            }
        }

        public int Scale { get; }

        public BigDecimal SetScale(int val)
        {
            var ten = new BigInteger(10);
            var num = UnscaledValue;
            if (val > Scale)
            {
                for (int i = 0; i < val - Scale; i++)
                {
                    num *= ten;
                }
            }
            else
            {
                for (int i = 0; i < Scale - val; i++)
                {
                    num /= ten;
                }
            }

            return new BigDecimal(num, val);
        }

        public BigInteger UnscaledValue { get; }

        public static BigDecimal operator +(BigDecimal bd1, BigDecimal bd2)
        {
            int scale = bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale;
            return new BigDecimal(bd1.SetScale(scale).UnscaledValue + bd2.SetScale(scale).UnscaledValue, scale);
        }

        public static BigDecimal operator -(BigDecimal bd) => new BigDecimal(-bd.UnscaledValue, bd.Scale);

        public static BigDecimal operator -(BigDecimal bd1, BigDecimal bd2)
        {
            int scale = bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale;
            return new BigDecimal(bd1.SetScale(scale).UnscaledValue - bd2.SetScale(scale).UnscaledValue, scale);
        }

        public static BigDecimal operator *(BigDecimal bd1, BigDecimal bd2) => new BigDecimal(bd1.UnscaledValue * bd2.UnscaledValue, bd1.Scale + bd2.Scale);

        public static bool operator <(BigDecimal bd1, BigDecimal bd2)
        {
            int scale = bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale;
            return bd1.SetScale(scale).UnscaledValue < bd2.SetScale(scale).UnscaledValue;
        }

        public static bool operator >(BigDecimal bd1, BigDecimal bd2)
        {
            int scale = bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale;
            return bd1.SetScale(scale).UnscaledValue > bd2.SetScale(scale).UnscaledValue;
        }

        public static bool operator <=(BigDecimal bd1, BigDecimal bd2) => !(bd1 > bd2);

        public static bool operator >=(BigDecimal bd1, BigDecimal bd2) => !(bd1 < bd2);

        public static bool operator ==(BigDecimal bd1, BigDecimal bd2)
        {
            if (Equals(bd1, null) && !Equals(bd2, null))
            {
                return false;
            }

            if (!Equals(bd1, null) && Equals(bd2, null))
            {
                return false;
            }

            if (Equals(bd1, null) && Equals(bd2, null))
            {
                return true;
            }

            int scale = bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale;
            return bd1.SetScale(scale).UnscaledValue == bd2.SetScale(scale).UnscaledValue;
        }

        public static bool operator !=(BigDecimal bd1, BigDecimal bd2) => !(bd1 == bd2);

        public static explicit operator BigInteger(BigDecimal val) => val.SetScale(0).UnscaledValue;

        public static explicit operator long(BigDecimal val) => (long)val.SetScale(0).UnscaledValue;

        public static explicit operator float(BigDecimal val) => float.Parse(val.ToString(), CultureInfo.InvariantCulture);

        public static explicit operator double(BigDecimal val) => double.Parse(val.ToString(), CultureInfo.InvariantCulture);

        public static explicit operator decimal(BigDecimal val) => decimal.Parse(val.ToString(), CultureInfo.InvariantCulture);

        public static implicit operator BigDecimal(long val) => new BigDecimal(val);

        public static implicit operator BigDecimal(ulong val) => new BigDecimal(val);

        public static implicit operator BigDecimal(int val) => new BigDecimal(val);

        public static implicit operator BigDecimal(uint val) => new BigDecimal(val);

        public static implicit operator BigDecimal(double val) => new BigDecimal(val);

        public BigDecimal MovePointLeft(int n) => n >= 0 ? new BigDecimal(UnscaledValue, Scale + n) : MovePointRight(-n);

        public BigDecimal MovePointRight(int n)
        {
            if (n >= 0)
            {
                if (Scale >= n)
                {
                    return new BigDecimal(UnscaledValue, Scale - n);
                }
                else
                {
                    var ten = new BigInteger(10);
                    var num = UnscaledValue;
                    for (int i = 0; i < n - Scale; i++)
                    {
                        num *= ten;
                    }

                    return new BigDecimal(num);
                }
            }
            else
            {
                return MovePointLeft(-n);
            }
        }

        public override string ToString()
        {
            string s_num = UnscaledValue.ToString();
            int s_scale = Scale;
            if (UnscaledValue < 0)
            {
                s_num = s_num.Remove(0, 1);
            }

            if (s_scale < 0)
            {
                s_num = s_num.PadRight(s_num.Length - s_scale, '0');
                s_scale = 0;
            }

            if (s_scale >= s_num.Length)
            {
                s_num = s_num.PadLeft(s_scale + 1, '0');
            }

            s_num = (UnscaledValue >= 0 ? string.Empty : "-") + s_num.Insert(s_num.Length - s_scale, strSeparator);

            if (s_num.EndsWith(strSeparator))
            {
                s_num = s_num.Remove(s_num.Length - strSeparator.Length, strSeparator.Length);
            }

            return s_num;
        }

        public override bool Equals(object o) => this == (BigDecimal)o;

        public override int GetHashCode() => ToString().GetHashCode();
    }
}
