//************************************************************************************
// BigInteger Class Version 1.03
//
// Copyright (C) 2005-2006 Dmitry S. Kataev
// Copyright (c) 2002 Chew Keong TAN
// All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, and/or sell copies of the Software, and to permit persons
// to whom the Software is furnished to do so, provided that the above
// copyright notice(s) and this permission notice appear in all copies of
// the Software and that both the above copyright notice(s) and this
// permission notice appear in supporting documentation.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
// OF THIRD PARTY RIGHTS. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// HOLDERS INCLUDED IN THIS NOTICE BE LIABLE FOR ANY CLAIM, OR ANY SPECIAL
// INDIRECT OR CONSEQUENTIAL DAMAGES, OR ANY DAMAGES WHATSOEVER RESULTING
// FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
// NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
// WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
//
//
// Disclaimer
// ----------
// Although reasonable care has been taken to ensure the correctness of this
// implementation, this code should never be used in any application without
// proper verification and testing.  I disclaim all liability and responsibility
// to any person or entity with respect to any loss or damage caused, or alleged
// to be caused, directly or indirectly, by the use of this BigInteger class.
//
// Comments, bugs and suggestions to
// (http://www.codeproject.com/csharp/biginteger.asp)
//
//
// Overloaded Operators +, -, *, /, %, >>, <<, ==, !=, >, <, >=, <=, &, |, ^, ++, --, ~
//
// Features
// --------
// 1) Arithmetic operations involving large signed integers (2's complement).
// 2) Primality test using Fermat little theorm, Rabin Miller's method,
//    Solovay Strassen's method and Lucas strong pseudoprime.
// 3) Modulo exponential with Barrett's reduction.
// 4) Inverse modulo.
// 5) Pseudo prime generation.
// 6) Co-prime generation.
//
//
// Known Problem
// -------------
// This pseudoprime passes my implementation of
// primality test but failed in JDK's isProbablePrime test.
//
//       byte[] pseudoPrime1 = { (byte)0x00,
//             (byte)0x85, (byte)0x84, (byte)0x64, (byte)0xFD, (byte)0x70, (byte)0x6A,
//             (byte)0x9F, (byte)0xF0, (byte)0x94, (byte)0x0C, (byte)0x3E, (byte)0x2C,
//             (byte)0x74, (byte)0x34, (byte)0x05, (byte)0xC9, (byte)0x55, (byte)0xB3,
//             (byte)0x85, (byte)0x32, (byte)0x98, (byte)0x71, (byte)0xF9, (byte)0x41,
//             (byte)0x21, (byte)0x5F, (byte)0x02, (byte)0x9E, (byte)0xEA, (byte)0x56,
//             (byte)0x8D, (byte)0x8C, (byte)0x44, (byte)0xCC, (byte)0xEE, (byte)0xEE,
//             (byte)0x3D, (byte)0x2C, (byte)0x9D, (byte)0x2C, (byte)0x12, (byte)0x41,
//             (byte)0x1E, (byte)0xF1, (byte)0xC5, (byte)0x32, (byte)0xC3, (byte)0xAA,
//             (byte)0x31, (byte)0x4A, (byte)0x52, (byte)0xD8, (byte)0xE8, (byte)0xAF,
//             (byte)0x42, (byte)0xF4, (byte)0x72, (byte)0xA1, (byte)0x2A, (byte)0x0D,
//             (byte)0x97, (byte)0xB1, (byte)0x31, (byte)0xB3,
//       };
//
//
// Change Log
// ----------
// 1) September 23, 2002 (Version 1.03)
//    - Fixed operator- to give correct data length.
//    - Added Lucas sequence generation.
//    - Added Strong Lucas Primality test.
//    - Added integer square root method.
//    - Added setBit/unsetBit methods.
//    - New isProbablePrime() method which do not require the
//      confident parameter.
//
// 2) August 29, 2002 (Version 1.02)
//    - Fixed bug in the exponentiation of negative numbers.
//    - Faster modular exponentiation using Barrett reduction.
//    - Added getBytes() method.
//    - Fixed bug in ToHexString method.
//    - Added overloading of ^ operator.
//    - Faster computation of Jacobi symbol.
//
// 3) August 19, 2002 (Version 1.01)
//    - Big integer is stored and manipulated as unsigned integers (4 bytes) instead of
//      individual bytes this gives significant performance improvement.
//    - Updated Fermat's Little Theorem test to use a^(p-1) mod p = 1
//    - Added isProbablePrime method.
//    - Updated documentation.
//
// 4) August 9, 2002 (Version 1.0)
//    - Initial Release.
//
//
// References
// [1] D. E. Knuth, "Seminumerical Algorithms", The Art of Computer Programming Vol. 2,
//     3rd Edition, Addison-Wesley, 1998.
//
// [2] K. H. Rosen, "Elementary Number Theory and Its Applications", 3rd Ed,
//     Addison-Wesley, 1993.
//
// [3] B. Schneier, "Applied Cryptography", 2nd Ed, John Wiley & Sons, 1996.
//
// [4] A. Menezes, P. van Oorschot, and S. Vanstone, "Handbook of Applied Cryptography",
//     CRC Press, 1996, www.cacr.math.uwaterloo.ca/hac
//
// [5] A. Bosselaers, R. Govaerts, and J. Vandewalle, "Comparison of Three Modular
//     Reduction Functions," Proc. CRYPTO'93, pp.175-186.
//
// [6] R. Baillie and S. S. Wagstaff Jr, "Lucas Pseudoprimes", Mathematics of Computation,
//     Vol. 35, No. 152, Oct 1980, pp. 1391-1417.
//
// [7] H. C. Williams, "Édouard Lucas and Primality Testing", Canadian Mathematical
//     Society Series of Monographs and Advance Texts, vol. 22, John Wiley & Sons, New York,
//     NY, 1998.
//
// [8] P. Ribenboim, "The new book of prime number records", 3rd edition, Springer-Verlag,
//     New York, NY, 1995.
//
// [9] M. Joye and J.-J. Quisquater, "Efficient computation of full Lucas sequences",
//     Electronics Letters, 32(6), 1996, pp 537-538.
//
//************************************************************************************

using System;
using System.Globalization;
using System.Text;

namespace MaxDB.Data.Utilities
{
#if SAFE
	internal class BigInteger
	{
		// maximum length of the BigInteger in uint (4 bytes)
		// change this to suit the required level of precision.

		private const int iMaxLength = 70;

		private uint[] uiData;             // stores bytes from the Big Integer
		public int iDataLength;                 // number of actual chars used


		//***********************************************************************
		// Constructor (Default value for BigInteger is 0
		//***********************************************************************

		public BigInteger()
		{
			uiData = new uint[iMaxLength];
			iDataLength = 1;
		}

		//***********************************************************************
		// Constructor (Default value provided by long)
		//***********************************************************************

		public BigInteger(long value)
		{
			uiData = new uint[iMaxLength];
			long tempVal = value;

			// copy bytes from long to BigInteger without any assumption of
			// the length of the long datatype

			iDataLength = 0;
			while (value != 0 && iDataLength < iMaxLength)
			{
				uiData[iDataLength] = (uint)(value & 0xFFFFFFFF);
				value >>= 32;
				iDataLength++;
			}

			if (tempVal > 0)         // overflow check for +ve value
			{
				if (value != 0 || (uiData[iMaxLength - 1] & 0x80000000) != 0)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "constructor"));
			}
			else if (tempVal < 0)    // underflow check for -ve value
			{
				if (value != -1 || (uiData[iDataLength - 1] & 0x80000000) == 0)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_UNDERFLOW, "constructor"));
			}

			if (iDataLength == 0)
				iDataLength = 1;
		}


		//***********************************************************************
		// Constructor (Default value provided by ulong)
		//***********************************************************************

		public BigInteger(ulong value)
		{
			uiData = new uint[iMaxLength];

			// copy bytes from ulong to BigInteger without any assumption of
			// the length of the ulong datatype

			iDataLength = 0;
			while (value != 0 && iDataLength < iMaxLength)
			{
				uiData[iDataLength] = (uint)(value & 0xFFFFFFFF);
				value >>= 32;
				iDataLength++;
			}

			if (value != 0 || (uiData[iMaxLength - 1] & 0x80000000) != 0)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "constructor"));

			if (iDataLength == 0)
				iDataLength = 1;
		}

		//***********************************************************************
		// Constructor (Default value provided by BigInteger)
		//***********************************************************************

		public BigInteger(BigInteger bi)
		{
			uiData = new uint[iMaxLength];

			iDataLength = bi.iDataLength;

			for (int i = 0; i < iDataLength; i++)
				uiData[i] = bi.uiData[i];
		}

		//***********************************************************************
		// Constructor (Default value provided by an array of unsigned integers)
		//*********************************************************************

		public BigInteger(uint[] inData)
		{
			iDataLength = inData.Length;

			if (iDataLength > iMaxLength)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "constructor"));

			uiData = new uint[iMaxLength];

			for (int i = iDataLength - 1, j = 0; i >= 0; i--, j++)
				uiData[j] = inData[i];

			while (iDataLength > 1 && uiData[iDataLength - 1] == 0)
				iDataLength--;
		}


		//***********************************************************************
		// Overloading of the typecast operator.
		// For BigInteger bi = 10;
		//***********************************************************************

		public static implicit operator BigInteger(long val)
		{
			return (new BigInteger(val));
		}

		public static implicit operator BigInteger(ulong val)
		{
			return (new BigInteger(val));
		}

		public static implicit operator BigInteger(int val)
		{
			return (new BigInteger(val));
		}

		public static implicit operator BigInteger(uint val)
		{
			return (new BigInteger(val));
		}

		//***********************************************************************
		// Overloading of addition operator
		//***********************************************************************

		public static BigInteger operator +(BigInteger bi1, BigInteger bi2)
		{
			BigInteger result = new BigInteger();

			result.iDataLength = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;

			long carry = 0;
			for (int i = 0; i < result.iDataLength; i++)
			{
				long sum = (long)bi1.uiData[i] + (long)bi2.uiData[i] + carry;
				carry = sum >> 32;
				result.uiData[i] = (uint)(sum & 0xFFFFFFFF);
			}

			if (carry != 0 && result.iDataLength < iMaxLength)
			{
				result.uiData[result.iDataLength] = (uint)(carry);
				result.iDataLength++;
			}

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;


			// overflow check
			int lastPos = iMaxLength - 1;
			if ((bi1.uiData[lastPos] & 0x80000000) == (bi2.uiData[lastPos] & 0x80000000) &&
				(result.uiData[lastPos] & 0x80000000) != (bi1.uiData[lastPos] & 0x80000000))
			{
				throw (new ArithmeticException());
			}

			return result;
		}

		//***********************************************************************
		// Overloading of the unary ++ operator
		//***********************************************************************

		public static BigInteger operator ++(BigInteger bi1)
		{
			BigInteger result = new BigInteger(bi1);

			long val, carry = 1;
			int index = 0;

			while (carry != 0 && index < iMaxLength)
			{
				val = (long)(result.uiData[index]);
				val++;

				result.uiData[index] = (uint)(val & 0xFFFFFFFF);
				carry = val >> 32;

				index++;
			}

			if (index > result.iDataLength)
				result.iDataLength = index;
			else
			{
				while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
					result.iDataLength--;
			}

			// overflow check
			int lastPos = iMaxLength - 1;

			// overflow if initial value was +ve but ++ caused a sign
			// change to negative.

			if ((bi1.uiData[lastPos] & 0x80000000) == 0 && (result.uiData[lastPos] & 0x80000000) != (bi1.uiData[lastPos] & 0x80000000))
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "increment operator"));

			return result;
		}


		//***********************************************************************
		// Overloading of subtraction operator
		//***********************************************************************

		public static BigInteger operator -(BigInteger bi1, BigInteger bi2)
		{
			BigInteger result = new BigInteger();

			result.iDataLength = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;

			long carryIn = 0;
			for (int i = 0; i < result.iDataLength; i++)
			{
				long diff;

				diff = (long)bi1.uiData[i] - (long)bi2.uiData[i] - carryIn;
				result.uiData[i] = (uint)(diff & 0xFFFFFFFF);

				if (diff < 0)
					carryIn = 1;
				else
					carryIn = 0;
			}

			// roll over to negative
			if (carryIn != 0)
			{
				for (int i = result.iDataLength; i < iMaxLength; i++)
					result.uiData[i] = 0xFFFFFFFF;
				result.iDataLength = iMaxLength;
			}

			// fixed in v1.03 to give correct datalength for a - (-b)
			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			// overflow check

			int lastPos = iMaxLength - 1;
			if ((bi1.uiData[lastPos] & 0x80000000) != (bi2.uiData[lastPos] & 0x80000000) &&
				(result.uiData[lastPos] & 0x80000000) != (bi1.uiData[lastPos] & 0x80000000))
			{
				throw (new ArithmeticException());
			}

			return result;
		}


		//***********************************************************************
		// Overloading of the unary -- operator
		//***********************************************************************
		public static BigInteger operator --(BigInteger bi1)
		{
			BigInteger result = new BigInteger(bi1);

			long val;
			bool carryIn = true;
			int index = 0;

			while (carryIn && index < iMaxLength)
			{
				val = (long)(result.uiData[index]);
				val--;

				result.uiData[index] = (uint)(val & 0xFFFFFFFF);

				if (val >= 0)
					carryIn = false;

				index++;
			}

			if (index > result.iDataLength)
				result.iDataLength = index;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			// overflow check
			int lastPos = iMaxLength - 1;

			// overflow if initial value was -ve but -- caused a sign
			// change to positive.

			if ((bi1.uiData[lastPos] & 0x80000000) != 0 && (result.uiData[lastPos] & 0x80000000) != (bi1.uiData[lastPos] & 0x80000000))
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_UNDERFLOW, "decrement operator"));

			return result;
		}


		//***********************************************************************
		// Overloading of multiplication operator
		//***********************************************************************

		public static BigInteger operator *(BigInteger bi1, BigInteger bi2)
		{
			int lastPos = iMaxLength - 1;
			bool bi1Neg = false, bi2Neg = false;

			// take the absolute value of the inputs
			if ((bi1.uiData[lastPos] & 0x80000000) != 0)     // bi1 negative
			{
				bi1Neg = true;
				bi1 = -bi1;
			}
			if ((bi2.uiData[lastPos] & 0x80000000) != 0)     // bi2 negative
			{
				bi2Neg = true;
				bi2 = -bi2;
			}

			BigInteger result = new BigInteger();

			// multiply the absolute values
			try
			{
				for (int i = 0; i < bi1.iDataLength; i++)
				{
					if (bi1.uiData[i] == 0) continue;

					ulong mcarry = 0;
					for (int j = 0, k = i; j < bi2.iDataLength; j++, k++)
					{
						// k = i + j
						ulong val = ((ulong)bi1.uiData[i] * (ulong)bi2.uiData[j]) +
							(ulong)result.uiData[k] + mcarry;

						result.uiData[k] = (uint)(val & 0xFFFFFFFF);
						mcarry = (val >> 32);
					}

					if (mcarry != 0)
						result.uiData[i + bi2.iDataLength] = (uint)mcarry;
				}
			}
			catch
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "multiplication operator"));
			}


			result.iDataLength = bi1.iDataLength + bi2.iDataLength;
			if (result.iDataLength > iMaxLength)
				result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			// overflow check (result is -ve)
			if ((result.uiData[lastPos] & 0x80000000) != 0)
			{
				if (bi1Neg != bi2Neg && result.uiData[lastPos] == 0x80000000)    // different sign
				{
					// handle the special case where multiplication produces
					// a max negative number in 2's complement.

					if (result.iDataLength == 1)
						return result;
					else
					{
						bool isMaxNeg = true;
						for (int i = 0; i < result.iDataLength - 1 && isMaxNeg; i++)
						{
							if (result.uiData[i] != 0)
								isMaxNeg = false;
						}

						if (isMaxNeg)
							return result;
					}
				}

				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_OVERFLOW, "multiplication operator"));
			}

			// if input has different signs, then result is -ve
			if (bi1Neg != bi2Neg)
				return -result;

			return result;
		}



		//***********************************************************************
		// Overloading of unary << operators
		//***********************************************************************

		public static BigInteger operator <<(BigInteger bi1, int shiftVal)
		{
			BigInteger result = new BigInteger(bi1);
			result.iDataLength = shiftLeft(result.uiData, shiftVal);

			return result;
		}


		// least significant bits at lower part of buffer

		private static int shiftLeft(uint[] buffer, int shiftVal)
		{
			int shiftAmount = 32;
			int bufLen = buffer.Length;

			while (bufLen > 1 && buffer[bufLen - 1] == 0)
				bufLen--;

			for (int count = shiftVal; count > 0; )
			{
				if (count < shiftAmount)
					shiftAmount = count;

				ulong carry = 0;
				for (int i = 0; i < bufLen; i++)
				{
					ulong val = ((ulong)buffer[i]) << shiftAmount;
					val |= carry;

					buffer[i] = (uint)(val & 0xFFFFFFFF);
					carry = val >> 32;
				}

				if (carry != 0)
				{
					if (bufLen + 1 <= buffer.Length)
					{
						buffer[bufLen] = (uint)carry;
						bufLen++;
					}
				}
				count -= shiftAmount;
			}
			return bufLen;
		}


		//***********************************************************************
		// Overloading of unary >> operators
		//***********************************************************************

		public static BigInteger operator >>(BigInteger bi1, int shiftVal)
		{
			BigInteger result = new BigInteger(bi1);
			result.iDataLength = shiftRight(result.uiData, shiftVal);


			if ((bi1.uiData[iMaxLength - 1] & 0x80000000) != 0) // negative
			{
				for (int i = iMaxLength - 1; i >= result.iDataLength; i--)
					result.uiData[i] = 0xFFFFFFFF;

				uint mask = 0x80000000;
				for (int i = 0; i < 32; i++)
				{
					if ((result.uiData[result.iDataLength - 1] & mask) != 0)
						break;

					result.uiData[result.iDataLength - 1] |= mask;
					mask >>= 1;
				}
				result.iDataLength = iMaxLength;
			}

			return result;
		}

		private static int shiftRight(uint[] buffer, int shiftVal)
		{
			int shiftAmount = 32;
			int invShift = 0;
			int bufLen = buffer.Length;

			while (bufLen > 1 && buffer[bufLen - 1] == 0)
				bufLen--;

			for (int count = shiftVal; count > 0; )
			{
				if (count < shiftAmount)
				{
					shiftAmount = count;
					invShift = 32 - shiftAmount;
				}

				ulong carry = 0;
				for (int i = bufLen - 1; i >= 0; i--)
				{
					ulong val = ((ulong)buffer[i]) >> shiftAmount;
					val |= carry;

					carry = ((ulong)buffer[i]) << invShift;
					buffer[i] = (uint)(val);
				}

				count -= shiftAmount;
			}

			while (bufLen > 1 && buffer[bufLen - 1] == 0)
				bufLen--;

			return bufLen;
		}

		//***********************************************************************
		// Overloading of the NOT operator (1's complement)
		//***********************************************************************

		public static BigInteger operator ~(BigInteger bi1)
		{
			BigInteger result = new BigInteger(bi1);

			for (int i = 0; i < iMaxLength; i++)
				result.uiData[i] = (uint)(~(bi1.uiData[i]));

			result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			return result;
		}

		//***********************************************************************
		// Overloading of the NEGATE operator (2's complement)
		//***********************************************************************

		public static BigInteger operator -(BigInteger bi1)
		{
			// handle neg of zero separately since it'll cause an overflow
			// if we proceed.

			if (bi1.iDataLength == 1 && bi1.uiData[0] == 0)
				return (new BigInteger());

			BigInteger result = new BigInteger(bi1);

			// 1's complement
			for (int i = 0; i < iMaxLength; i++)
				result.uiData[i] = (uint)(~(bi1.uiData[i]));

			// add one to result of 1's complement
			long val, carry = 1;
			int index = 0;

			while (carry != 0 && index < iMaxLength)
			{
				val = (long)(result.uiData[index]);
				val++;

				result.uiData[index] = (uint)(val & 0xFFFFFFFF);
				carry = val >> 32;

				index++;
			}

			if ((bi1.uiData[iMaxLength - 1] & 0x80000000) == (result.uiData[iMaxLength - 1] & 0x80000000))
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_UNDERFLOW, "negation operator"));

			result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;
			return result;
		}

		//***********************************************************************
		// Overloading of equality operator
		//***********************************************************************

		public static bool operator ==(BigInteger bi1, BigInteger bi2)
		{
			return bi1.Equals(bi2);
		}

		public static bool operator !=(BigInteger bi1, BigInteger bi2)
		{
			return !(bi1.Equals(bi2));
		}

		public override bool Equals(object o)
		{
			BigInteger bi = (BigInteger)o;

			if (iDataLength != bi.iDataLength)
				return false;

			for (int i = 0; i < iDataLength; i++)
			{
				if (uiData[i] != bi.uiData[i])
					return false;
			}
			return true;
		}

		public override int GetHashCode()
		{
			return ToString().GetHashCode();
		}

		//***********************************************************************
		// Overloading of inequality operator
		//***********************************************************************

		public static bool operator >(BigInteger bi1, BigInteger bi2)
		{
			int pos = iMaxLength - 1;

			// bi1 is negative, bi2 is positive
			if ((bi1.uiData[pos] & 0x80000000) != 0 && (bi2.uiData[pos] & 0x80000000) == 0)
				return false;

				// bi1 is positive, bi2 is negative
			else if ((bi1.uiData[pos] & 0x80000000) == 0 && (bi2.uiData[pos] & 0x80000000) != 0)
				return true;

			// same sign
			int len = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;
			for (pos = len - 1; pos >= 0 && bi1.uiData[pos] == bi2.uiData[pos]; pos--) ;

			if (pos >= 0)
			{
				if (bi1.uiData[pos] > bi2.uiData[pos])
					return true;
				return false;
			}
			return false;
		}

		public static bool operator <(BigInteger bi1, BigInteger bi2)
		{
			int pos = iMaxLength - 1;

			// bi1 is negative, bi2 is positive
			if ((bi1.uiData[pos] & 0x80000000) != 0 && (bi2.uiData[pos] & 0x80000000) == 0)
				return true;

				// bi1 is positive, bi2 is negative
			else if ((bi1.uiData[pos] & 0x80000000) == 0 && (bi2.uiData[pos] & 0x80000000) != 0)
				return false;

			// same sign
			int len = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;
			for (pos = len - 1; pos >= 0 && bi1.uiData[pos] == bi2.uiData[pos]; pos--) ;

			if (pos >= 0)
			{
				if (bi1.uiData[pos] < bi2.uiData[pos])
					return true;
				return false;
			}
			return false;
		}

		public static bool operator >=(BigInteger bi1, BigInteger bi2)
		{
			return (bi1 == bi2 || bi1 > bi2);
		}

		public static bool operator <=(BigInteger bi1, BigInteger bi2)
		{
			return (bi1 == bi2 || bi1 < bi2);
		}

		//***********************************************************************
		// Private function that supports the division of two numbers with
		// a divisor that has more than 1 digit.
		//
		// Algorithm taken from [1]
		//***********************************************************************

		private static void multiByteDivide(BigInteger bi1, BigInteger bi2,
			BigInteger outQuotient, BigInteger outRemainder)
		{
			uint[] result = new uint[iMaxLength];

			int remainderLen = bi1.iDataLength + 1;
			uint[] remainder = new uint[remainderLen];

			uint mask = 0x80000000;
			uint val = bi2.uiData[bi2.iDataLength - 1];
			int shift = 0, resultPos = 0;

			while (mask != 0 && (val & mask) == 0)
			{
				shift++; mask >>= 1;
			}

			for (int i = 0; i < bi1.iDataLength; i++)
				remainder[i] = bi1.uiData[i];
			shiftLeft(remainder, shift);
			bi2 = bi2 << shift;

			int j = remainderLen - bi2.iDataLength;
			int pos = remainderLen - 1;

			ulong firstDivisorByte = bi2.uiData[bi2.iDataLength - 1];
			ulong secondDivisorByte = bi2.uiData[bi2.iDataLength - 2];

			int divisorLen = bi2.iDataLength + 1;
			uint[] dividendPart = new uint[divisorLen];

			while (j > 0)
			{
				ulong dividend = ((ulong)remainder[pos] << 32) + (ulong)remainder[pos - 1];

				ulong q_hat = dividend / firstDivisorByte;
				ulong r_hat = dividend % firstDivisorByte;

				bool done = false;
				while (!done)
				{
					done = true;

					if (q_hat == 0x100000000 ||
						(q_hat * secondDivisorByte) > ((r_hat << 32) + remainder[pos - 2]))
					{
						q_hat--;
						r_hat += firstDivisorByte;

						if (r_hat < 0x100000000)
							done = false;
					}
				}

				for (int h = 0; h < divisorLen; h++)
					dividendPart[h] = remainder[pos - h];

				BigInteger kk = new BigInteger(dividendPart);
				BigInteger ss = bi2 * (long)q_hat;

				while (ss > kk)
				{
					q_hat--;
					ss -= bi2;
				}
				BigInteger yy = kk - ss;

				for (int h = 0; h < divisorLen; h++)
					remainder[pos - h] = yy.uiData[bi2.iDataLength - h];

				result[resultPos++] = (uint)q_hat;

				pos--;
				j--;
			}

			outQuotient.iDataLength = resultPos;
			int y = 0;
			for (int x = outQuotient.iDataLength - 1; x >= 0; x--, y++)
				outQuotient.uiData[y] = result[x];
			for (; y < iMaxLength; y++)
				outQuotient.uiData[y] = 0;

			while (outQuotient.iDataLength > 1 && outQuotient.uiData[outQuotient.iDataLength - 1] == 0)
				outQuotient.iDataLength--;

			if (outQuotient.iDataLength == 0)
				outQuotient.iDataLength = 1;

			outRemainder.iDataLength = shiftRight(remainder, shift);

			for (y = 0; y < outRemainder.iDataLength; y++)
				outRemainder.uiData[y] = remainder[y];
			for (; y < iMaxLength; y++)
				outRemainder.uiData[y] = 0;
		}


		//***********************************************************************
		// Private function that supports the division of two numbers with
		// a divisor that has only 1 digit.
		//***********************************************************************

		private static void singleByteDivide(BigInteger bi1, BigInteger bi2,
			BigInteger outQuotient, BigInteger outRemainder)
		{
			uint[] result = new uint[iMaxLength];
			int resultPos = 0;

			// copy dividend to reminder
			for (int i = 0; i < iMaxLength; i++)
				outRemainder.uiData[i] = bi1.uiData[i];
			outRemainder.iDataLength = bi1.iDataLength;

			while (outRemainder.iDataLength > 1 && outRemainder.uiData[outRemainder.iDataLength - 1] == 0)
				outRemainder.iDataLength--;

			ulong divisor = (ulong)bi2.uiData[0];
			int pos = outRemainder.iDataLength - 1;
			ulong dividend = (ulong)outRemainder.uiData[pos];

			if (dividend >= divisor)
			{
				ulong quotient = dividend / divisor;
				result[resultPos++] = (uint)quotient;

				outRemainder.uiData[pos] = (uint)(dividend % divisor);
			}
			pos--;

			while (pos >= 0)
			{
				dividend = ((ulong)outRemainder.uiData[pos + 1] << 32) + (ulong)outRemainder.uiData[pos];
				ulong quotient = dividend / divisor;
				result[resultPos++] = (uint)quotient;

				outRemainder.uiData[pos + 1] = 0;
				outRemainder.uiData[pos--] = (uint)(dividend % divisor);
			}

			outQuotient.iDataLength = resultPos;
			int j = 0;
			for (int i = outQuotient.iDataLength - 1; i >= 0; i--, j++)
				outQuotient.uiData[j] = result[i];
			for (; j < iMaxLength; j++)
				outQuotient.uiData[j] = 0;

			while (outQuotient.iDataLength > 1 && outQuotient.uiData[outQuotient.iDataLength - 1] == 0)
				outQuotient.iDataLength--;

			if (outQuotient.iDataLength == 0)
				outQuotient.iDataLength = 1;

			while (outRemainder.iDataLength > 1 && outRemainder.uiData[outRemainder.iDataLength - 1] == 0)
				outRemainder.iDataLength--;
		}


		//***********************************************************************
		// Overloading of division operator
		//***********************************************************************

		public static BigInteger operator /(BigInteger bi1, BigInteger bi2)
		{
			BigInteger quotient = new BigInteger();
			BigInteger remainder = new BigInteger();

			int lastPos = iMaxLength - 1;
			bool divisorNeg = false, dividendNeg = false;

			if ((bi1.uiData[lastPos] & 0x80000000) != 0)     // bi1 negative
			{
				bi1 = -bi1;
				dividendNeg = true;
			}
			if ((bi2.uiData[lastPos] & 0x80000000) != 0)     // bi2 negative
			{
				bi2 = -bi2;
				divisorNeg = true;
			}

			if (bi1 < bi2)
			{
				return quotient;
			}

			else
			{
				if (bi2.iDataLength == 1)
					singleByteDivide(bi1, bi2, quotient, remainder);
				else
					multiByteDivide(bi1, bi2, quotient, remainder);

				if (dividendNeg != divisorNeg)
					return -quotient;

				return quotient;
			}
		}


		//***********************************************************************
		// Overloading of modulus operator
		//***********************************************************************

		public static BigInteger operator %(BigInteger bi1, BigInteger bi2)
		{
			BigInteger quotient = new BigInteger();
			BigInteger remainder = new BigInteger(bi1);

			int lastPos = iMaxLength - 1;
			bool dividendNeg = false;

			if ((bi1.uiData[lastPos] & 0x80000000) != 0)     // bi1 negative
			{
				bi1 = -bi1;
				dividendNeg = true;
			}
			if ((bi2.uiData[lastPos] & 0x80000000) != 0)     // bi2 negative
				bi2 = -bi2;

			if (bi1 < bi2)
			{
				return remainder;
			}

			else
			{
				if (bi2.iDataLength == 1)
					singleByteDivide(bi1, bi2, quotient, remainder);
				else
					multiByteDivide(bi1, bi2, quotient, remainder);

				if (dividendNeg)
					return -remainder;

				return remainder;
			}
		}


		//***********************************************************************
		// Overloading of bitwise AND operator
		//***********************************************************************

		public static BigInteger operator &(BigInteger bi1, BigInteger bi2)
		{
			BigInteger result = new BigInteger();

			int len = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;

			for (int i = 0; i < len; i++)
			{
				uint sum = (uint)(bi1.uiData[i] & bi2.uiData[i]);
				result.uiData[i] = sum;
			}

			result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			return result;
		}


		//***********************************************************************
		// Overloading of bitwise OR operator
		//***********************************************************************

		public static BigInteger operator |(BigInteger bi1, BigInteger bi2)
		{
			BigInteger result = new BigInteger();

			int len = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;

			for (int i = 0; i < len; i++)
			{
				uint sum = (uint)(bi1.uiData[i] | bi2.uiData[i]);
				result.uiData[i] = sum;
			}

			result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			return result;
		}


		//***********************************************************************
		// Overloading of bitwise XOR operator
		//***********************************************************************

		public static BigInteger operator ^(BigInteger bi1, BigInteger bi2)
		{
			BigInteger result = new BigInteger();

			int len = (bi1.iDataLength > bi2.iDataLength) ? bi1.iDataLength : bi2.iDataLength;

			for (int i = 0; i < len; i++)
			{
				uint sum = (uint)(bi1.uiData[i] ^ bi2.uiData[i]);
				result.uiData[i] = sum;
			}

			result.iDataLength = iMaxLength;

			while (result.iDataLength > 1 && result.uiData[result.iDataLength - 1] == 0)
				result.iDataLength--;

			return result;
		}

		//***********************************************************************
		// Returns the absolute value
		//***********************************************************************

		public BigInteger Abs
		{
			get
			{
				if ((uiData[iMaxLength - 1] & 0x80000000) != 0)
					return (-this);
				else
					return (new BigInteger(this));
			}
		}

		//***********************************************************************
		// Returns a string representing the BigInteger in base 10.
		//***********************************************************************

		public override string ToString()
		{
			return ToString(10);
		}

		//***********************************************************************
		// Returns a string representing the BigInteger in sign-and-magnitude
		// format in the specified radix.
		//
		// Example
		// -------
		// If the value of BigInteger is -255 in base 10, then
		// ToString(16) returns "-FF"
		//
		//***********************************************************************

		public string ToString(int radix)
		{
			if (radix < 2 || radix > 36)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BIGINT_RADIX_OVERFLOW));

			string charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			StringBuilder result = new StringBuilder();

			BigInteger a = this;

			bool negative = false;
			if ((a.uiData[iMaxLength - 1] & 0x80000000) != 0)
			{
				negative = true;
				a = -a;
			}

			BigInteger quotient = new BigInteger();
			BigInteger remainder = new BigInteger();
			BigInteger biRadix = new BigInteger(radix);

			if (a.iDataLength == 1 && a.uiData[0] == 0)
				result.Append("0");
			else
			{
				while (a.iDataLength > 1 || (a.iDataLength == 1 && a.uiData[0] != 0))
				{
					singleByteDivide(a, biRadix, quotient, remainder);

					if (remainder.uiData[0] < 10)
						result.Insert(0, remainder.uiData[0]);
					else
						result.Insert(0, charSet[(int)remainder.uiData[0] - 10]);

					a = quotient;
				}
				if (negative)
					result.Insert(0, "-");
			}

			return result.ToString();
		}

		//***********************************************************************
		// Returns the lowest 4 bytes of the BigInteger as an int.
		//***********************************************************************

		public static explicit operator int(BigInteger bi)
		{
			return (int)bi.uiData[0];
		}

		//***********************************************************************
		// Returns the lowest 8 bytes of the BigInteger as a long.
		//***********************************************************************
		public static explicit operator long(BigInteger bi)
		{
			return (long)bi.uiData[0] | ((long)bi.uiData[1] << 32);
		}
	}

	internal class BigDecimal
	{
		private BigInteger biNumber;
		private int iScale;
		private string strSeparator = CultureInfo.InvariantCulture.NumberFormat.CurrencyDecimalSeparator;

		public BigDecimal()
		{
			biNumber = new BigInteger(0);
		}

		public BigDecimal(long num)
		{
			biNumber = new BigInteger(num);
		}

		public BigDecimal(ulong num)
		{
			biNumber = new BigInteger(num);
		}

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
			biNumber = num;
			iScale = scale;
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
				biNumber = new BigInteger(0);
				return;
			}

			// find the decimal separator and exponent position
			num = num.Replace(CultureInfo.InvariantCulture.NumberFormat.CurrencyGroupSeparator, string.Empty);
			int dot_index = num.IndexOf(strSeparator, cur_off);
			int exp_index = num.IndexOf('e', cur_off);
			if (exp_index < 0) 
				exp_index = num.IndexOf('E', cur_off);

			// check number format
			if (num.IndexOf(strSeparator, dot_index + 1) >= 0)
				throw new FormatException();
			int last_exp_index = num.IndexOf('e', exp_index + 1);
			if (last_exp_index < 0) 
				last_exp_index = num.IndexOf('E', exp_index + 1);
			if (last_exp_index >= 0)
				throw new FormatException();

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
						s = int.Parse(num.Substring(cur_off), CultureInfo.InvariantCulture);

					if (int_part.Trim().Length == 0)
						biNumber = new BigInteger(0);
					else
					{
						biNumber = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
						if (biNumber != 0)
							iScale = -s;
					}
				}
				else
				{
					// just get integer part
					int_part = num.TrimStart('0');
					if (int_part.Trim().Length == 0)
						biNumber = new BigInteger(0);
					else
						biNumber = new BigInteger(long.Parse(int_part, CultureInfo.InvariantCulture));
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
					biNumber = new BigInteger(0);
				else
				{
					biNumber = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
					if (biNumber != 0)
						iScale = float_part.Length;
				}
				return;
			}

			// extract float part
			float_part = num.Substring(cur_off, exp_index - cur_off).TrimEnd('0');
			cur_off = exp_index + 1;

			// get exponent value
			if (cur_off < num.Length)
				s = int.Parse(num.Substring(cur_off), CultureInfo.InvariantCulture);

			float_num += float_part;
			if (float_num.Trim().Length == 0)
				biNumber = new BigInteger(0);
			else
			{
				biNumber = new BigInteger(long.Parse(float_num, CultureInfo.InvariantCulture));
				if (biNumber != 0)
					iScale = float_part.Length - s;
			}
		}

		public int Scale
		{
			get
			{
				return iScale;
			}
		}

		public BigDecimal setScale(int val)
		{
			BigInteger ten = new BigInteger(10);
			BigInteger num = biNumber;
			if (val > iScale)
				for (int i = 0; i < val - iScale; i++)
					num *= ten;
			else
				for (int i = 0; i < iScale - val; i++)
					num /= ten;

			return new BigDecimal(num, val);
		}

		public BigInteger unscaledValue
		{
			get
			{
				return biNumber;
			}
		}

		public static BigDecimal operator +(BigDecimal bd1, BigDecimal bd2)
		{
			int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
			return new BigDecimal(bd1.setScale(scale).unscaledValue + bd2.setScale(scale).unscaledValue, scale);
		}

		public static BigDecimal operator -(BigDecimal bd)
		{
			return new BigDecimal(-bd.unscaledValue, bd.Scale);
		}

		public static BigDecimal operator -(BigDecimal bd1, BigDecimal bd2)
		{
			int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
			return new BigDecimal(bd1.setScale(scale).unscaledValue - bd2.setScale(scale).unscaledValue, scale);
		}

		public static BigDecimal operator *(BigDecimal bd1, BigDecimal bd2)
		{
			return new BigDecimal(bd1.unscaledValue * bd2.unscaledValue, bd1.Scale + bd2.Scale);
		}

		public static bool operator <(BigDecimal bd1, BigDecimal bd2)
		{
			int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
			return bd1.setScale(scale).unscaledValue < bd2.setScale(scale).unscaledValue;
		}

		public static bool operator >(BigDecimal bd1, BigDecimal bd2)
		{
			int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
			return bd1.setScale(scale).unscaledValue > bd2.setScale(scale).unscaledValue;
		}

		public static bool operator <=(BigDecimal bd1, BigDecimal bd2)
		{
			return !(bd1 > bd2);
		}

		public static bool operator >=(BigDecimal bd1, BigDecimal bd2)
		{
			return !(bd1 < bd2);
		}

		public static bool operator ==(BigDecimal bd1, BigDecimal bd2)
		{
			if (object.Equals(bd1, null) && !object.Equals(bd2, null)) return false;
			if (!object.Equals(bd1, null) && object.Equals(bd2, null)) return false;
			if (object.Equals(bd1, null) && object.Equals(bd2, null)) return true;
			int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
			return bd1.setScale(scale).unscaledValue == bd2.setScale(scale).unscaledValue;
		}

		public static bool operator !=(BigDecimal bd1, BigDecimal bd2)
		{
			return !(bd1 == bd2);
		}

		public static explicit operator BigInteger(BigDecimal val)
		{
			return val.setScale(0).unscaledValue;
		}

		public static explicit operator long(BigDecimal val)
		{
			return (long)val.setScale(0).unscaledValue;
		}

		public static explicit operator float(BigDecimal val)
		{
			return float.Parse(val.ToString(), CultureInfo.InvariantCulture);
		}

		public static explicit operator double(BigDecimal val)
		{
			return double.Parse(val.ToString(), CultureInfo.InvariantCulture);
		}

		public static explicit operator decimal(BigDecimal val)
		{
			return decimal.Parse(val.ToString(), CultureInfo.InvariantCulture);
		}

		public static implicit operator BigDecimal(long val)
		{
			return (new BigDecimal(val));
		}

		public static implicit operator BigDecimal(ulong val)
		{
			return (new BigDecimal(val));
		}

		public static implicit operator BigDecimal(int val)
		{
			return (new BigDecimal(val));
		}

		public static implicit operator BigDecimal(uint val)
		{
			return (new BigDecimal(val));
		}

		public static implicit operator BigDecimal(double val)
		{
			return (new BigDecimal(val));
		}

		public BigDecimal MovePointLeft(int n)
		{
			if (n >= 0)
				return new BigDecimal(biNumber, iScale + n);
			else
				return MovePointRight(-n);
		}

		public BigDecimal MovePointRight(int n)
		{
			if (n >= 0)
			{
				if (iScale >= n)
					return new BigDecimal(biNumber, iScale - n);
				else
				{
					BigInteger ten = new BigInteger(10);
					BigInteger num = biNumber;
					for (int i = 0; i < n - iScale; i++)
						num *= ten;

					return new BigDecimal(num);
				}
			}
			else
				return MovePointLeft(-n);
		}

		public override string ToString()
		{
			string s_num = biNumber.ToString();
			int s_scale = iScale;
			if (biNumber < 0)
				s_num = s_num.Remove(0, 1);
			if (s_scale < 0)
			{
				s_num = s_num.PadRight(s_num.Length - s_scale, '0');
				s_scale = 0;
			}
			if (s_scale >= s_num.Length)
				s_num = s_num.PadLeft(s_scale + 1, '0');

			s_num = (biNumber >= 0 ? string.Empty : "-") + s_num.Insert(s_num.Length - s_scale, strSeparator);

			if (s_num.EndsWith(strSeparator))
				s_num = s_num.Remove(s_num.Length - strSeparator.Length, strSeparator.Length);

			return s_num;
		}

		public override bool Equals(object o)
		{
			return this == (BigDecimal)o;
		}

		public override int GetHashCode()
		{
			return ToString().GetHashCode();
		}
	}
#endif // SAFE
}
