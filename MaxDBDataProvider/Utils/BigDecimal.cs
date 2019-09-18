//-----------------------------------------------------------------------------------------------
// <copyright file="BigDecimal.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data.Utilities
{
    using System;
    using System.Globalization;
    using System.Numerics;

    /// <summary>
    /// Big decimal number.
    /// </summary>
    internal class BigDecimal
    {
        private readonly string strSeparator = CultureInfo.InvariantCulture.NumberFormat.CurrencyDecimalSeparator;

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        public BigDecimal() => this.UnscaledValue = new BigInteger(0);

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">Long number.</param>
        public BigDecimal(long num) => this.UnscaledValue = new BigInteger(num);

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">Unsigned long number.</param>
        public BigDecimal(ulong num) => this.UnscaledValue = new BigInteger(num);

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">Double number.</param>
        public BigDecimal(double num)
            : this(num.ToString(CultureInfo.InvariantCulture))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">Big integer.</param>
        public BigDecimal(BigInteger num)
            : this(num, 0)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">Big integer.</param>
        /// <param name="scale">Decimal scale.</param>
        public BigDecimal(BigInteger num, int scale)
        {
            this.UnscaledValue = num;
            this.Scale = scale;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BigDecimal"/> class.
        /// </summary>
        /// <param name="num">String presentation.</param>
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
                this.UnscaledValue = new BigInteger(0);
                return;
            }

            // find the decimal separator and exponent position
            num = num.Replace(CultureInfo.InvariantCulture.NumberFormat.CurrencyGroupSeparator, string.Empty);
            int dot_index = num.IndexOf(this.strSeparator, cur_off, StringComparison.InvariantCulture);
            int exp_index = num.IndexOf('e', cur_off);
            if (exp_index < 0)
            {
                exp_index = num.IndexOf('E', cur_off);
            }

            // check number format
            if (num.IndexOf(this.strSeparator, dot_index + 1, StringComparison.InvariantCulture) >= 0)
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
                        this.UnscaledValue = new BigInteger(0);
                    }
                    else
                    {
                        this.UnscaledValue = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
                        if (this.UnscaledValue != 0)
                        {
                            this.Scale = -s;
                        }
                    }
                }
                else
                {
                    // just get integer part
                    int_part = num.TrimStart('0');
                    if (int_part.Trim().Length == 0)
                    {
                        this.UnscaledValue = new BigInteger(0);
                    }
                    else
                    {
                        this.UnscaledValue = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
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
                    this.UnscaledValue = new BigInteger(0);
                }
                else
                {
                    this.UnscaledValue = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
                    if (this.UnscaledValue != 0)
                    {
                        this.Scale = float_part.Length;
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
                this.UnscaledValue = new BigInteger(0);
            }
            else
            {
                this.UnscaledValue = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
                if (this.UnscaledValue != 0)
                {
                    this.Scale = float_part.Length - s;
                }
            }
        }

        /// <summary>
        /// Gets a big decimal scale.
        /// </summary>
        public int Scale { get; }

        /// <summary>
        /// Gets unscaled value.
        /// </summary>
        public BigInteger UnscaledValue { get; }

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

        /// <summary>
        /// Set big decimal scale.
        /// </summary>
        /// <param name="val">Scale value.</param>
        /// <returns>Big decimal with updated scale.</returns>
        public BigDecimal SetScale(int val)
        {
            var ten = new BigInteger(10);
            var num = this.UnscaledValue;
            if (val > this.Scale)
            {
                for (int i = 0; i < val - this.Scale; i++)
                {
                    num *= ten;
                }
            }
            else
            {
                for (int i = 0; i < this.Scale - val; i++)
                {
                    num /= ten;
                }
            }

            return new BigDecimal(num, val);
        }

        /// <summary>
        /// Move decimal point left.
        /// </summary>
        /// <param name="n">Number of positions to move.</param>
        /// <returns>Updated big decimal.</returns>
        public BigDecimal MovePointLeft(int n) => n >= 0 ? new BigDecimal(this.UnscaledValue, this.Scale + n) : this.MovePointRight(-n);

        /// <summary>
        /// Move decimal point right.
        /// </summary>
        /// <param name="n">Number of positions to move.</param>
        /// <returns>Updated big decimal.</returns>
        public BigDecimal MovePointRight(int n)
        {
            if (n >= 0)
            {
                if (this.Scale >= n)
                {
                    return new BigDecimal(this.UnscaledValue, this.Scale - n);
                }
                else
                {
                    var ten = new BigInteger(10);
                    var num = this.UnscaledValue;
                    for (int i = 0; i < n - this.Scale; i++)
                    {
                        num *= ten;
                    }

                    return new BigDecimal(num);
                }
            }
            else
            {
                return this.MovePointLeft(-n);
            }
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>A string that represents the current object.</returns>
        public override string ToString()
        {
            string s_num = this.UnscaledValue.ToString(CultureInfo.InvariantCulture);
            int s_scale = this.Scale;
            if (this.UnscaledValue < 0)
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

            s_num = (this.UnscaledValue >= 0 ? string.Empty : "-") + s_num.Insert(s_num.Length - s_scale, this.strSeparator);

            if (s_num.EndsWith(this.strSeparator, StringComparison.InvariantCulture))
            {
                s_num = s_num.Remove(s_num.Length - this.strSeparator.Length, this.strSeparator.Length);
            }

            return s_num;
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="o">The object to compare with the current object.</param>
        /// <returns><c>true</c> if the specified object is equal to the current object; otherwise, <c>false</c>.</returns>
        public override bool Equals(object o) => this == (BigDecimal)o;

        /// <summary>
        /// Serves as the big decimal hash function.
        /// </summary>
        /// <returns>A hash code for the current object.</returns>
        public override int GetHashCode() => this.ToString().GetHashCode();
    }
}
