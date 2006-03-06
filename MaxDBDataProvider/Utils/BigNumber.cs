//************************************************************************************
// BigInteger Class Version 1.03
//
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

public class BigInteger
{
	// maximum length of the BigInteger in uint (4 bytes)
	// change this to suit the required level of precision.

	private const int maxLength = 70;

	private uint[] data = null;             // stores bytes from the Big Integer
	public int dataLength;                 // number of actual chars used


	//***********************************************************************
	// Constructor (Default value for BigInteger is 0
	//***********************************************************************

	public BigInteger()
	{
		data = new uint[maxLength];
		dataLength = 1;
	}

	//***********************************************************************
	// Constructor (Default value provided by long)
	//***********************************************************************

	public BigInteger(long value)
	{
		data = new uint[maxLength];
		long tempVal = value;

		// copy bytes from long to BigInteger without any assumption of
		// the length of the long datatype

		dataLength = 0;
		while(value != 0 && dataLength < maxLength)
		{
			data[dataLength] = (uint)(value & 0xFFFFFFFF);
			value >>= 32;
			dataLength++;
		}

		if(tempVal > 0)         // overflow check for +ve value
		{
			if(value != 0 || (data[maxLength-1] & 0x80000000) != 0)
				throw(new ArithmeticException("Positive overflow in constructor."));
		}
		else if(tempVal < 0)    // underflow check for -ve value
		{
			if(value != -1 || (data[dataLength-1] & 0x80000000) == 0)
				throw(new ArithmeticException("Negative underflow in constructor."));
		}

		if(dataLength == 0)
			dataLength = 1;
	}


	//***********************************************************************
	// Constructor (Default value provided by ulong)
	//***********************************************************************

	public BigInteger(ulong value)
	{
		data = new uint[maxLength];

		// copy bytes from ulong to BigInteger without any assumption of
		// the length of the ulong datatype

		dataLength = 0;
		while(value != 0 && dataLength < maxLength)
		{
			data[dataLength] = (uint)(value & 0xFFFFFFFF);
			value >>= 32;
			dataLength++;
		}

		if(value != 0 || (data[maxLength-1] & 0x80000000) != 0)
			throw(new ArithmeticException("Positive overflow in constructor."));

		if(dataLength == 0)
			dataLength = 1;
	}

	//***********************************************************************
	// Constructor (Default value provided by BigInteger)
	//***********************************************************************

	public BigInteger(BigInteger bi)
	{
		data = new uint[maxLength];

		dataLength = bi.dataLength;

		for(int i = 0; i < dataLength; i++)
			data[i] = bi.data[i];
	}

	//***********************************************************************
	// Constructor (Default value provided by a string of digits of the
	//              specified base)
	//
	// Example (base 10)
	// -----------------
	// To initialize "a" with the default value of 1234 in base 10
	//      BigInteger a = new BigInteger("1234", 10)
	//
	// To initialize "a" with the default value of -1234
	//      BigInteger a = new BigInteger("-1234", 10)
	//
	// Example (base 16)
	// -----------------
	// To initialize "a" with the default value of 0x1D4F in base 16
	//      BigInteger a = new BigInteger("1D4F", 16)
	//
	// To initialize "a" with the default value of -0x1D4F
	//      BigInteger a = new BigInteger("-1D4F", 16)
	//
	// Note that string values are specified in the <sign><magnitude>
	// format.
	//
	//***********************************************************************

	public BigInteger(string value, int radix)
	{
		BigInteger multiplier = new BigInteger(1);
		BigInteger result = new BigInteger();
		value = (value.ToUpper()).Trim();
		int limit = 0;

		if(value[0] == '-')
			limit = 1;

		for(int i = value.Length - 1; i >= limit ; i--)
		{
			int posVal = (int)value[i];

			if(posVal >= '0' && posVal <= '9')
				posVal -= '0';
			else if(posVal >= 'A' && posVal <= 'Z')
				posVal = (posVal - 'A') + 10;
			else
				posVal = 9999999;       // arbitrary large


			if(posVal >= radix)
				throw(new ArithmeticException("Invalid string in constructor."));
			else
			{
				if(value[0] == '-')
					posVal = -posVal;

				result = result + (multiplier * posVal);

				if((i - 1) >= limit)
					multiplier = multiplier * radix;
			}
		}

		if(value[0] == '-')     // negative values
		{
			if((result.data[maxLength-1] & 0x80000000) == 0)
				throw(new ArithmeticException("Negative underflow in constructor."));
		}
		else    // positive values
		{
			if((result.data[maxLength-1] & 0x80000000) != 0)
				throw(new ArithmeticException("Positive overflow in constructor."));
		}

		data = new uint[maxLength];
		for(int i = 0; i < result.dataLength; i++)
			data[i] = result.data[i];

		dataLength = result.dataLength;
	}


	//***********************************************************************
	// Constructor (Default value provided by an array of bytes)
	//
	// The lowest index of the input byte array (i.e [0]) should contain the
	// most significant byte of the number, and the highest index should
	// contain the least significant byte.
	//
	// E.g.
	// To initialize "a" with the default value of 0x1D4F in base 16
	//      byte[] temp = { 0x1D, 0x4F };
	//      BigInteger a = new BigInteger(temp)
	//
	// Note that this method of initialization does not allow the
	// sign to be specified.
	//
	//***********************************************************************

	public BigInteger(byte[] inData)
	{
		dataLength = inData.Length >> 2;

		int leftOver = inData.Length & 0x3;
		if(leftOver != 0)         // length not multiples of 4
			dataLength++;


		if(dataLength > maxLength)
			throw(new ArithmeticException("Byte overflow in constructor."));

		data = new uint[maxLength];

		for(int i = inData.Length - 1, j = 0; i >= 3; i -= 4, j++)
		{
			data[j] = (uint)((inData[i-3] << 24) + (inData[i-2] << 16) +
				(inData[i-1] <<  8) + inData[i]);
		}

		if(leftOver == 1)
			data[dataLength-1] = (uint)inData[0];
		else if(leftOver == 2)
			data[dataLength-1] = (uint)((inData[0] << 8) + inData[1]);
		else if(leftOver == 3)
			data[dataLength-1] = (uint)((inData[0] << 16) + (inData[1] << 8) + inData[2]);


		while(dataLength > 1 && data[dataLength-1] == 0)
			dataLength--;
	}


	//***********************************************************************
	// Constructor (Default value provided by an array of bytes of the
	// specified length.)
	//***********************************************************************

	public BigInteger(byte[] inData, int inLen)
	{
		dataLength = inLen >> 2;

		int leftOver = inLen & 0x3;
		if(leftOver != 0)         // length not multiples of 4
			dataLength++;

		if(dataLength > maxLength || inLen > inData.Length)
			throw(new ArithmeticException("Byte overflow in constructor."));


		data = new uint[maxLength];

		for(int i = inLen - 1, j = 0; i >= 3; i -= 4, j++)
		{
			data[j] = (uint)((inData[i-3] << 24) + (inData[i-2] << 16) +
				(inData[i-1] <<  8) + inData[i]);
		}

		if(leftOver == 1)
			data[dataLength-1] = (uint)inData[0];
		else if(leftOver == 2)
			data[dataLength-1] = (uint)((inData[0] << 8) + inData[1]);
		else if(leftOver == 3)
			data[dataLength-1] = (uint)((inData[0] << 16) + (inData[1] << 8) + inData[2]);


		if(dataLength == 0)
			dataLength = 1;

		while(dataLength > 1 && data[dataLength-1] == 0)
			dataLength--;
	}


	//***********************************************************************
	// Constructor (Default value provided by an array of unsigned integers)
	//*********************************************************************

	public BigInteger(uint[] inData)
	{
		dataLength = inData.Length;

		if(dataLength > maxLength)
			throw(new ArithmeticException("Byte overflow in constructor."));

		data = new uint[maxLength];

		for(int i = dataLength - 1, j = 0; i >= 0; i--, j++)
			data[j] = inData[i];

		while(dataLength > 1 && data[dataLength-1] == 0)
			dataLength--;
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

		result.dataLength = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;

		long carry = 0;
		for(int i = 0; i < result.dataLength; i++)
		{
			long sum = (long)bi1.data[i] + (long)bi2.data[i] + carry;
			carry  = sum >> 32;
			result.data[i] = (uint)(sum & 0xFFFFFFFF);
		}

		if(carry != 0 && result.dataLength < maxLength)
		{
			result.data[result.dataLength] = (uint)(carry);
			result.dataLength++;
		}

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;


		// overflow check
		int lastPos = maxLength - 1;
		if((bi1.data[lastPos] & 0x80000000) == (bi2.data[lastPos] & 0x80000000) &&
			(result.data[lastPos] & 0x80000000) != (bi1.data[lastPos] & 0x80000000))
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

		while(carry != 0 && index < maxLength)
		{
			val = (long)(result.data[index]);
			val++;

			result.data[index] = (uint)(val & 0xFFFFFFFF);
			carry = val >> 32;

			index++;
		}

		if(index > result.dataLength)
			result.dataLength = index;
		else
		{
			while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
				result.dataLength--;
		}

		// overflow check
		int lastPos = maxLength - 1;

		// overflow if initial value was +ve but ++ caused a sign
		// change to negative.

		if((bi1.data[lastPos] & 0x80000000) == 0 &&
			(result.data[lastPos] & 0x80000000) != (bi1.data[lastPos] & 0x80000000))
		{
			throw (new ArithmeticException("Overflow in ++."));
		}
		return result;
	}


	//***********************************************************************
	// Overloading of subtraction operator
	//***********************************************************************

	public static BigInteger operator -(BigInteger bi1, BigInteger bi2)
	{
		BigInteger result = new BigInteger();

		result.dataLength = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;

		long carryIn = 0;
		for(int i = 0; i < result.dataLength; i++)
		{
			long diff;

			diff = (long)bi1.data[i] - (long)bi2.data[i] - carryIn;
			result.data[i] = (uint)(diff & 0xFFFFFFFF);

			if(diff < 0)
				carryIn = 1;
			else
				carryIn = 0;
		}

		// roll over to negative
		if(carryIn != 0)
		{
			for(int i = result.dataLength; i < maxLength; i++)
				result.data[i] = 0xFFFFFFFF;
			result.dataLength = maxLength;
		}

		// fixed in v1.03 to give correct datalength for a - (-b)
		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		// overflow check

		int lastPos = maxLength - 1;
		if((bi1.data[lastPos] & 0x80000000) != (bi2.data[lastPos] & 0x80000000) &&
			(result.data[lastPos] & 0x80000000) != (bi1.data[lastPos] & 0x80000000))
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

		while(carryIn && index < maxLength)
		{
			val = (long)(result.data[index]);
			val--;

			result.data[index] = (uint)(val & 0xFFFFFFFF);

			if(val >= 0)
				carryIn = false;

			index++;
		}

		if(index > result.dataLength)
			result.dataLength = index;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		// overflow check
		int lastPos = maxLength - 1;

		// overflow if initial value was -ve but -- caused a sign
		// change to positive.

		if((bi1.data[lastPos] & 0x80000000) != 0 &&
			(result.data[lastPos] & 0x80000000) != (bi1.data[lastPos] & 0x80000000))
		{
			throw (new ArithmeticException("Underflow in --."));
		}

		return result;
	}


	//***********************************************************************
	// Overloading of multiplication operator
	//***********************************************************************

	public static BigInteger operator *(BigInteger bi1, BigInteger bi2)
	{
		int lastPos = maxLength-1;
		bool bi1Neg = false, bi2Neg = false;

		// take the absolute value of the inputs
		try
		{
			if((bi1.data[lastPos] & 0x80000000) != 0)     // bi1 negative
			{
				bi1Neg = true; bi1 = -bi1;
			}
			if((bi2.data[lastPos] & 0x80000000) != 0)     // bi2 negative
			{
				bi2Neg = true; bi2 = -bi2;
			}
		}
		catch(Exception) {}

		BigInteger result = new BigInteger();

		// multiply the absolute values
		try
		{
			for(int i = 0; i < bi1.dataLength; i++)
			{
				if(bi1.data[i] == 0)    continue;

				ulong mcarry = 0;
				for(int j = 0, k = i; j < bi2.dataLength; j++, k++)
				{
					// k = i + j
					ulong val = ((ulong)bi1.data[i] * (ulong)bi2.data[j]) +
						(ulong)result.data[k] + mcarry;

					result.data[k] = (uint)(val & 0xFFFFFFFF);
					mcarry = (val >> 32);
				}

				if(mcarry != 0)
					result.data[i+bi2.dataLength] = (uint)mcarry;
			}
		}
		catch(Exception)
		{
			throw(new ArithmeticException("Multiplication overflow."));
		}


		result.dataLength = bi1.dataLength + bi2.dataLength;
		if(result.dataLength > maxLength)
			result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		// overflow check (result is -ve)
		if((result.data[lastPos] & 0x80000000) != 0)
		{
			if(bi1Neg != bi2Neg && result.data[lastPos] == 0x80000000)    // different sign
			{
				// handle the special case where multiplication produces
				// a max negative number in 2's complement.

				if(result.dataLength == 1)
					return result;
				else
				{
					bool isMaxNeg = true;
					for(int i = 0; i < result.dataLength - 1 && isMaxNeg; i++)
					{
						if(result.data[i] != 0)
							isMaxNeg = false;
					}

					if(isMaxNeg)
						return result;
				}
			}

			throw(new ArithmeticException("Multiplication overflow."));
		}

		// if input has different signs, then result is -ve
		if(bi1Neg != bi2Neg)
			return -result;

		return result;
	}



	//***********************************************************************
	// Overloading of unary << operators
	//***********************************************************************

	public static BigInteger operator <<(BigInteger bi1, int shiftVal)
	{
		BigInteger result = new BigInteger(bi1);
		result.dataLength = shiftLeft(result.data, shiftVal);

		return result;
	}


	// least significant bits at lower part of buffer

	private static int shiftLeft(uint[] buffer, int shiftVal)
	{
		int shiftAmount = 32;
		int bufLen = buffer.Length;

		while(bufLen > 1 && buffer[bufLen-1] == 0)
			bufLen--;

		for(int count = shiftVal; count > 0;)
		{
			if(count < shiftAmount)
				shiftAmount = count;

			ulong carry = 0;
			for(int i = 0; i < bufLen; i++)
			{
				ulong val = ((ulong)buffer[i]) << shiftAmount;
				val |= carry;

				buffer[i] = (uint)(val & 0xFFFFFFFF);
				carry = val >> 32;
			}

			if(carry != 0)
			{
				if(bufLen + 1 <= buffer.Length)
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
		result.dataLength = shiftRight(result.data, shiftVal);


		if((bi1.data[maxLength-1] & 0x80000000) != 0) // negative
		{
			for(int i = maxLength - 1; i >= result.dataLength; i--)
				result.data[i] = 0xFFFFFFFF;

			uint mask = 0x80000000;
			for(int i = 0; i < 32; i++)
			{
				if((result.data[result.dataLength-1] & mask) != 0)
					break;

				result.data[result.dataLength-1] |= mask;
				mask >>= 1;
			}
			result.dataLength = maxLength;
		}

		return result;
	}

	private static int shiftRight(uint[] buffer, int shiftVal)
	{
		int shiftAmount = 32;
		int invShift = 0;
		int bufLen = buffer.Length;

		while(bufLen > 1 && buffer[bufLen-1] == 0)
			bufLen--;

		for(int count = shiftVal; count > 0;)
		{
			if(count < shiftAmount)
			{
				shiftAmount = count;
				invShift = 32 - shiftAmount;
			}

			ulong carry = 0;
			for(int i = bufLen - 1; i >= 0; i--)
			{
				ulong val = ((ulong)buffer[i]) >> shiftAmount;
				val |= carry;

				carry = ((ulong)buffer[i]) << invShift;
				buffer[i] = (uint)(val);
			}

			count -= shiftAmount;
		}

		while(bufLen > 1 && buffer[bufLen-1] == 0)
			bufLen--;

		return bufLen;
	}

	//***********************************************************************
	// Overloading of the NOT operator (1's complement)
	//***********************************************************************

	public static BigInteger operator ~(BigInteger bi1)
	{
		BigInteger result = new BigInteger(bi1);

		for(int i = 0; i < maxLength; i++)
			result.data[i] = (uint)(~(bi1.data[i]));

		result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		return result;
	}

	//***********************************************************************
	// Overloading of the NEGATE operator (2's complement)
	//***********************************************************************

	public static BigInteger operator -(BigInteger bi1)
	{
		// handle neg of zero separately since it'll cause an overflow
		// if we proceed.

		if(bi1.dataLength == 1 && bi1.data[0] == 0)
			return (new BigInteger());

		BigInteger result = new BigInteger(bi1);

		// 1's complement
		for(int i = 0; i < maxLength; i++)
			result.data[i] = (uint)(~(bi1.data[i]));

		// add one to result of 1's complement
		long val, carry = 1;
		int index = 0;

		while(carry != 0 && index < maxLength)
		{
			val = (long)(result.data[index]);
			val++;

			result.data[index] = (uint)(val & 0xFFFFFFFF);
			carry = val >> 32;

			index++;
		}

		if((bi1.data[maxLength-1] & 0x80000000) == (result.data[maxLength-1] & 0x80000000))
			throw (new ArithmeticException("Overflow in negation.\n"));

		result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;
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

		if(dataLength != bi.dataLength)
			return false;

		for(int i = 0; i < dataLength; i++)
		{
			if(data[i] != bi.data[i])
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
		int pos = maxLength - 1;

		// bi1 is negative, bi2 is positive
		if((bi1.data[pos] & 0x80000000) != 0 && (bi2.data[pos] & 0x80000000) == 0)
			return false;

			// bi1 is positive, bi2 is negative
		else if((bi1.data[pos] & 0x80000000) == 0 && (bi2.data[pos] & 0x80000000) != 0)
			return true;

		// same sign
		int len = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;
		for(pos = len - 1; pos >= 0 && bi1.data[pos] == bi2.data[pos]; pos--);

		if(pos >= 0)
		{
			if(bi1.data[pos] > bi2.data[pos])
				return true;
			return false;
		}
		return false;
	}

	public static bool operator <(BigInteger bi1, BigInteger bi2)
	{
		int pos = maxLength - 1;

		// bi1 is negative, bi2 is positive
		if((bi1.data[pos] & 0x80000000) != 0 && (bi2.data[pos] & 0x80000000) == 0)
			return true;

			// bi1 is positive, bi2 is negative
		else if((bi1.data[pos] & 0x80000000) == 0 && (bi2.data[pos] & 0x80000000) != 0)
			return false;

		// same sign
		int len = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;
		for(pos = len - 1; pos >= 0 && bi1.data[pos] == bi2.data[pos]; pos--);

		if(pos >= 0)
		{
			if(bi1.data[pos] < bi2.data[pos])
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
		uint[] result = new uint[maxLength];

		int remainderLen = bi1.dataLength + 1;
		uint[] remainder = new uint[remainderLen];

		uint mask = 0x80000000;
		uint val = bi2.data[bi2.dataLength - 1];
		int shift = 0, resultPos = 0;

		while(mask != 0 && (val & mask) == 0)
		{
			shift++; mask >>= 1;
		}

		for(int i = 0; i < bi1.dataLength; i++)
			remainder[i] = bi1.data[i];
		shiftLeft(remainder, shift);
		bi2 = bi2 << shift;

		int j = remainderLen - bi2.dataLength;
		int pos = remainderLen - 1;

		ulong firstDivisorByte = bi2.data[bi2.dataLength-1];
		ulong secondDivisorByte = bi2.data[bi2.dataLength-2];

		int divisorLen = bi2.dataLength + 1;
		uint[] dividendPart = new uint[divisorLen];

		while(j > 0)
		{
			ulong dividend = ((ulong)remainder[pos] << 32) + (ulong)remainder[pos-1];

			ulong q_hat = dividend / firstDivisorByte;
			ulong r_hat = dividend % firstDivisorByte;

			bool done = false;
			while(!done)
			{
				done = true;

				if(q_hat == 0x100000000 ||
					(q_hat * secondDivisorByte) > ((r_hat << 32) + remainder[pos-2]))
				{
					q_hat--;
					r_hat += firstDivisorByte;

					if(r_hat < 0x100000000)
						done = false;
				}
			}

			for(int h = 0; h < divisorLen; h++)
				dividendPart[h] = remainder[pos-h];

			BigInteger kk = new BigInteger(dividendPart);
			BigInteger ss = bi2 * (long)q_hat;

			while(ss > kk)
			{
				q_hat--;
				ss -= bi2;
			}
			BigInteger yy = kk - ss;

			for(int h = 0; h < divisorLen; h++)
				remainder[pos-h] = yy.data[bi2.dataLength-h];

			result[resultPos++] = (uint)q_hat;

			pos--;
			j--;
		}

		outQuotient.dataLength = resultPos;
		int y = 0;
		for(int x = outQuotient.dataLength - 1; x >= 0; x--, y++)
			outQuotient.data[y] = result[x];
		for(; y < maxLength; y++)
			outQuotient.data[y] = 0;

		while(outQuotient.dataLength > 1 && outQuotient.data[outQuotient.dataLength-1] == 0)
			outQuotient.dataLength--;

		if(outQuotient.dataLength == 0)
			outQuotient.dataLength = 1;

		outRemainder.dataLength = shiftRight(remainder, shift);

		for(y = 0; y < outRemainder.dataLength; y++)
			outRemainder.data[y] = remainder[y];
		for(; y < maxLength; y++)
			outRemainder.data[y] = 0;
	}


	//***********************************************************************
	// Private function that supports the division of two numbers with
	// a divisor that has only 1 digit.
	//***********************************************************************

	private static void singleByteDivide(BigInteger bi1, BigInteger bi2,
		BigInteger outQuotient, BigInteger outRemainder)
	{
		uint[] result = new uint[maxLength];
		int resultPos = 0;

		// copy dividend to reminder
		for(int i = 0; i < maxLength; i++)
			outRemainder.data[i] = bi1.data[i];
		outRemainder.dataLength = bi1.dataLength;

		while(outRemainder.dataLength > 1 && outRemainder.data[outRemainder.dataLength-1] == 0)
			outRemainder.dataLength--;

		ulong divisor = (ulong)bi2.data[0];
		int pos = outRemainder.dataLength - 1;
		ulong dividend = (ulong)outRemainder.data[pos];

		if(dividend >= divisor)
		{
			ulong quotient = dividend / divisor;
			result[resultPos++] = (uint)quotient;

			outRemainder.data[pos] = (uint)(dividend % divisor);
		}
		pos--;

		while(pos >= 0)
		{
			dividend = ((ulong)outRemainder.data[pos+1] << 32) + (ulong)outRemainder.data[pos];
			ulong quotient = dividend / divisor;
			result[resultPos++] = (uint)quotient;

			outRemainder.data[pos+1] = 0;
			outRemainder.data[pos--] = (uint)(dividend % divisor);
		}

		outQuotient.dataLength = resultPos;
		int j = 0;
		for(int i = outQuotient.dataLength - 1; i >= 0; i--, j++)
			outQuotient.data[j] = result[i];
		for(; j < maxLength; j++)
			outQuotient.data[j] = 0;

		while(outQuotient.dataLength > 1 && outQuotient.data[outQuotient.dataLength-1] == 0)
			outQuotient.dataLength--;

		if(outQuotient.dataLength == 0)
			outQuotient.dataLength = 1;

		while(outRemainder.dataLength > 1 && outRemainder.data[outRemainder.dataLength-1] == 0)
			outRemainder.dataLength--;
	}


	//***********************************************************************
	// Overloading of division operator
	//***********************************************************************

	public static BigInteger operator /(BigInteger bi1, BigInteger bi2)
	{
		BigInteger quotient = new BigInteger();
		BigInteger remainder = new BigInteger();

		int lastPos = maxLength-1;
		bool divisorNeg = false, dividendNeg = false;

		if((bi1.data[lastPos] & 0x80000000) != 0)     // bi1 negative
		{
			bi1 = -bi1;
			dividendNeg = true;
		}
		if((bi2.data[lastPos] & 0x80000000) != 0)     // bi2 negative
		{
			bi2 = -bi2;
			divisorNeg = true;
		}

		if(bi1 < bi2)
		{
			return quotient;
		}

		else
		{
			if(bi2.dataLength == 1)
				singleByteDivide(bi1, bi2, quotient, remainder);
			else
				multiByteDivide(bi1, bi2, quotient, remainder);

			if(dividendNeg != divisorNeg)
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

		int lastPos = maxLength-1;
		bool dividendNeg = false;

		if((bi1.data[lastPos] & 0x80000000) != 0)     // bi1 negative
		{
			bi1 = -bi1;
			dividendNeg = true;
		}
		if((bi2.data[lastPos] & 0x80000000) != 0)     // bi2 negative
			bi2 = -bi2;

		if(bi1 < bi2)
		{
			return remainder;
		}

		else
		{
			if(bi2.dataLength == 1)
				singleByteDivide(bi1, bi2, quotient, remainder);
			else
				multiByteDivide(bi1, bi2, quotient, remainder);

			if(dividendNeg)
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

		int len = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;

		for(int i = 0; i < len; i++)
		{
			uint sum = (uint)(bi1.data[i] & bi2.data[i]);
			result.data[i] = sum;
		}

		result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		return result;
	}


	//***********************************************************************
	// Overloading of bitwise OR operator
	//***********************************************************************

	public static BigInteger operator |(BigInteger bi1, BigInteger bi2)
	{
		BigInteger result = new BigInteger();

		int len = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;

		for(int i = 0; i < len; i++)
		{
			uint sum = (uint)(bi1.data[i] | bi2.data[i]);
			result.data[i] = sum;
		}

		result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		return result;
	}


	//***********************************************************************
	// Overloading of bitwise XOR operator
	//***********************************************************************

	public static BigInteger operator ^(BigInteger bi1, BigInteger bi2)
	{
		BigInteger result = new BigInteger();

		int len = (bi1.dataLength > bi2.dataLength) ? bi1.dataLength : bi2.dataLength;

		for(int i = 0; i < len; i++)
		{
			uint sum = (uint)(bi1.data[i] ^ bi2.data[i]);
			result.data[i] = sum;
		}

		result.dataLength = maxLength;

		while(result.dataLength > 1 && result.data[result.dataLength-1] == 0)
			result.dataLength--;

		return result;
	}


	//***********************************************************************
	// Returns Max(this, bi)
	//***********************************************************************

	public BigInteger Max(BigInteger bi)
	{
		if(this > bi)
			return (new BigInteger(this));
		else
			return (new BigInteger(bi));
	}

	//***********************************************************************
	// Returns Min(this, bi)
	//***********************************************************************

	public BigInteger Min(BigInteger bi)
	{
		if(this < bi)
			return (new BigInteger(this));
		else
			return (new BigInteger(bi));

	}

	//***********************************************************************
	// Returns the absolute value
	//***********************************************************************

	public BigInteger Abs
	{
		get
		{
			if((data[maxLength - 1] & 0x80000000) != 0)
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
		if(radix < 2 || radix > 36)
			throw (new ArgumentException("Radix must be >= 2 and <= 36"));

		string charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		string result = "";

		BigInteger a = this;

		bool negative = false;
		if((a.data[maxLength-1] & 0x80000000) != 0)
		{
			negative = true;
			try
			{
				a = -a;
			}
			catch(Exception) {}
		}

		BigInteger quotient = new BigInteger();
		BigInteger remainder = new BigInteger();
		BigInteger biRadix = new BigInteger(radix);

		if(a.dataLength == 1 && a.data[0] == 0)
			result = "0";
		else
		{
			while(a.dataLength > 1 || (a.dataLength == 1 && a.data[0] != 0))
			{
				singleByteDivide(a, biRadix, quotient, remainder);

				if(remainder.data[0] < 10)
					result = remainder.data[0] + result;
				else
					result = charSet[(int)remainder.data[0] - 10] + result;

				a = quotient;
			}
			if(negative)
				result = "-" + result;
		}

		return result;
	}


	//***********************************************************************
	// Returns a hex string showing the contains of the BigInteger
	//
	// Examples
	// -------
	// 1) If the value of BigInteger is 255 in base 10, then
	//    ToHexString() returns "FF"
	//
	// 2) If the value of BigInteger is -255 in base 10, then
	//    ToHexString() returns ".....FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF01",
	//    which is the 2's complement representation of -255.
	//
	//***********************************************************************

	public string ToHexString()
	{
		string result = data[dataLength - 1].ToString("X");

		for(int i = dataLength - 2; i >= 0; i--)
		{
			result += data[i].ToString("X8");
		}

		return result;
	}

	//***********************************************************************
	// Modulo Exponentiation
	//***********************************************************************

	public BigInteger modPow(BigInteger exp, BigInteger n)
	{
		if((exp.data[maxLength-1] & 0x80000000) != 0)
			throw (new ArithmeticException("Positive exponents only."));

		BigInteger resultNum = 1;
		BigInteger tempNum;
		bool thisNegative = false;

		if((data[maxLength-1] & 0x80000000) != 0)   // negative this
		{
			tempNum = -this % n;
			thisNegative = true;
		}
		else
			tempNum = this % n;  // ensures (tempNum * tempNum) < b^(2k)

		if((n.data[maxLength-1] & 0x80000000) != 0)   // negative n
			n = -n;

		// calculate constant = b^(2k) / m
		BigInteger constant = new BigInteger();

		int i = n.dataLength << 1;
		constant.data[i] = 0x00000001;
		constant.dataLength = i + 1;

		constant = constant / n;
		int totalBits = exp.bitCount();
		int count = 0;

		// perform squaring and multiply exponentiation
		for(int pos = 0; pos < exp.dataLength; pos++)
		{
			uint mask = 0x01;
			for(int index = 0; index < 32; index++)
			{
				if((exp.data[pos] & mask) != 0)
					resultNum = BarrettReduction(resultNum * tempNum, n, constant);

				mask <<= 1;

				tempNum = BarrettReduction(tempNum * tempNum, n, constant);


				if(tempNum.dataLength == 1 && tempNum.data[0] == 1)
				{
					if(thisNegative && (exp.data[0] & 0x1) != 0)    //odd exp
						return -resultNum;
					return resultNum;
				}
				count++;
				if(count == totalBits)
					break;
			}
		}

		if(thisNegative && (exp.data[0] & 0x1) != 0)    //odd exp
			return -resultNum;

		return resultNum;
	}

	//***********************************************************************
	// Fast calculation of modular reduction using Barrett's reduction.
	// Requires x < b^(2k), where b is the base.  In this case, base is
	// 2^32 (uint).
	//
	// Reference [4]
	//***********************************************************************

	private BigInteger BarrettReduction(BigInteger x, BigInteger n, BigInteger constant)
	{
		int k = n.dataLength,
			kPlusOne = k+1,
			kMinusOne = k-1;

		BigInteger q1 = new BigInteger();

		// q1 = x / b^(k-1)
		for(int i = kMinusOne, j = 0; i < x.dataLength; i++, j++)
			q1.data[j] = x.data[i];
		q1.dataLength = x.dataLength - kMinusOne;
		if(q1.dataLength <= 0)
			q1.dataLength = 1;


		BigInteger q2 = q1 * constant;
		BigInteger q3 = new BigInteger();

		// q3 = q2 / b^(k+1)
		for(int i = kPlusOne, j = 0; i < q2.dataLength; i++, j++)
			q3.data[j] = q2.data[i];
		q3.dataLength = q2.dataLength - kPlusOne;
		if(q3.dataLength <= 0)
			q3.dataLength = 1;


		// r1 = x mod b^(k+1)
		// i.e. keep the lowest (k+1) words
		BigInteger r1 = new BigInteger();
		int lengthToCopy = (x.dataLength > kPlusOne) ? kPlusOne : x.dataLength;
		for(int i = 0; i < lengthToCopy; i++)
			r1.data[i] = x.data[i];
		r1.dataLength = lengthToCopy;


		// r2 = (q3 * n) mod b^(k+1)
		// partial multiplication of q3 and n

		BigInteger r2 = new BigInteger();
		for(int i = 0; i < q3.dataLength; i++)
		{
			if(q3.data[i] == 0)     continue;

			ulong mcarry = 0;
			int t = i;
			for(int j = 0; j < n.dataLength && t < kPlusOne; j++, t++)
			{
				// t = i + j
				ulong val = ((ulong)q3.data[i] * (ulong)n.data[j]) +
					(ulong)r2.data[t] + mcarry;

				r2.data[t] = (uint)(val & 0xFFFFFFFF);
				mcarry = (val >> 32);
			}

			if(t < kPlusOne)
				r2.data[t] = (uint)mcarry;
		}
		r2.dataLength = kPlusOne;
		while(r2.dataLength > 1 && r2.data[r2.dataLength-1] == 0)
			r2.dataLength--;

		r1 -= r2;
		if((r1.data[maxLength-1] & 0x80000000) != 0)        // negative
		{
			BigInteger val = new BigInteger();
			val.data[kPlusOne] = 0x00000001;
			val.dataLength = kPlusOne + 1;
			r1 += val;
		}

		while(r1 >= n)
			r1 -= n;

		return r1;
	}


	//***********************************************************************
	// Returns gcd(this, bi)
	//***********************************************************************

	public BigInteger gcd(BigInteger bi)
	{
		BigInteger x;
		BigInteger y;

		if((data[maxLength-1] & 0x80000000) != 0)     // negative
			x = -this;
		else
			x = this;

		if((bi.data[maxLength-1] & 0x80000000) != 0)     // negative
			y = -bi;
		else
			y = bi;

		BigInteger g = y;

		while(x.dataLength > 1 || (x.dataLength == 1 && x.data[0] != 0))
		{
			g = x;
			x = y % x;
			y = g;
		}

		return g;
	}


	//***********************************************************************
	// Populates "this" with the specified amount of random bits
	//***********************************************************************

	public void genRandomBits(int bits, Random rand)
	{
		int dwords = bits >> 5;
		int remBits = bits & 0x1F;

		if(remBits != 0)
			dwords++;

		if(dwords > maxLength)
			throw (new ArithmeticException("Number of required bits > maxLength."));

		for(int i = 0; i < dwords; i++)
			data[i] = (uint)(rand.NextDouble() * 0x100000000);

		for(int i = dwords; i < maxLength; i++)
			data[i] = 0;

		if(remBits != 0)
		{
			uint mask = (uint)(0x01 << (remBits-1));
			data[dwords-1] |= mask;

			mask = (uint)(0xFFFFFFFF >> (32 - remBits));
			data[dwords-1] &= mask;
		}
		else
			data[dwords-1] |= 0x80000000;

		dataLength = dwords;

		if(dataLength == 0)
			dataLength = 1;
	}


	//***********************************************************************
	// Returns the position of the most significant bit in the BigInteger.
	//
	// Eg.  The result is 0, if the value of BigInteger is 0...0000 0000
	//      The result is 1, if the value of BigInteger is 0...0000 0001
	//      The result is 2, if the value of BigInteger is 0...0000 0010
	//      The result is 2, if the value of BigInteger is 0...0000 0011
	//
	//***********************************************************************

	public int bitCount()
	{
		while(dataLength > 1 && data[dataLength-1] == 0)
			dataLength--;

		uint value = data[dataLength - 1];
		uint mask = 0x80000000;
		int bits = 32;

		while(bits > 0 && (value & mask) == 0)
		{
			bits--;
			mask >>= 1;
		}
		bits += ((dataLength - 1) << 5);

		return bits;
	}

	//***********************************************************************
	// Returns the lowest 4 bytes of the BigInteger as an int.
	//***********************************************************************

	public static explicit operator int(BigInteger bi) 
	{
		return (int)bi.data[0];
	}

	//***********************************************************************
	// Returns the lowest 8 bytes of the BigInteger as a long.
	//***********************************************************************

	public static explicit operator long(BigInteger bi)
	{
		long val = 0;

		val = (long)bi.data[0];
		try
		{       // exception if maxLength = 1
			val |= (long)bi.data[1] << 32;
		}
		catch(Exception)
		{
			if((bi.data[0] & 0x80000000) != 0) // negative
				val = (int)bi.data[0];
		}

		return val;
	}

	//***********************************************************************
	// Computes the Jacobi Symbol for a and b.
	// Algorithm adapted from [3] and [4] with some optimizations
	//***********************************************************************

	public static int Jacobi(BigInteger a, BigInteger b)
	{
		// Jacobi defined only for odd integers
		if((b.data[0] & 0x1) == 0)
			throw (new ArgumentException("Jacobi defined only for odd integers."));

		if(a >= b)      a %= b;
		if(a.dataLength == 1 && a.data[0] == 0)      return 0;  // a == 0
		if(a.dataLength == 1 && a.data[0] == 1)      return 1;  // a == 1

		if(a < 0)
		{
			if( (((b-1).data[0]) & 0x2) == 0)       //if( (((b-1) >> 1).data[0] & 0x1) == 0)
				return Jacobi(-a, b);
			else
				return -Jacobi(-a, b);
		}

		int e = 0;
		for(int index = 0; index < a.dataLength; index++)
		{
			uint mask = 0x01;

			for(int i = 0; i < 32; i++)
			{
				if((a.data[index] & mask) != 0)
				{
					index = a.dataLength;      // to break the outer loop
					break;
				}
				mask <<= 1;
				e++;
			}
		}

		BigInteger a1 = a >> e;

		int s = 1;
		if((e & 0x1) != 0 && ((b.data[0] & 0x7) == 3 || (b.data[0] & 0x7) == 5))
			s = -1;

		if((b.data[0] & 0x3) == 3 && (a1.data[0] & 0x3) == 3)
			s = -s;

		if(a1.dataLength == 1 && a1.data[0] == 1)
			return s;
		else
			return (s * Jacobi(b % a1, a1));
	}

	//***********************************************************************
	// Generates a random number with the specified number of bits such
	// that gcd(number, this) = 1
	//***********************************************************************

	public BigInteger genCoPrime(int bits, Random rand)
	{
		bool done = false;
		BigInteger result = new BigInteger();

		while(!done)
		{
			result.genRandomBits(bits, rand);

			// gcd test
			BigInteger g = result.gcd(this);
			if(g.dataLength == 1 && g.data[0] == 1)
				done = true;
		}

		return result;
	}

	//***********************************************************************
	// Returns the modulo inverse of this.  Throws ArithmeticException if
	// the inverse does not exist.  (i.e. gcd(this, modulus) != 1)
	//***********************************************************************

	public BigInteger modInverse(BigInteger modulus)
	{
		BigInteger[] p = { 0, 1 };
		BigInteger[] q = new BigInteger[2];    // quotients
		BigInteger[] r = { 0, 0 };             // remainders

		int step = 0;

		BigInteger a = modulus;
		BigInteger b = this;

		while(b.dataLength > 1 || (b.dataLength == 1 && b.data[0] != 0))
		{
			BigInteger quotient = new BigInteger();
			BigInteger remainder = new BigInteger();

			if(step > 1)
			{
				BigInteger pval = (p[0] - (p[1] * q[0])) % modulus;
				p[0] = p[1];
				p[1] = pval;
			}

			if(b.dataLength == 1)
				singleByteDivide(a, b, quotient, remainder);
			else
				multiByteDivide(a, b, quotient, remainder);

			q[0] = q[1];
			r[0] = r[1];
			q[1] = quotient; r[1] = remainder;

			a = b;
			b = remainder;

			step++;
		}

		if(r[0].dataLength > 1 || (r[0].dataLength == 1 && r[0].data[0] != 1))
			throw (new ArithmeticException("No inverse!"));

		BigInteger result = ((p[0] - (p[1] * q[0])) % modulus);

		if((result.data[maxLength - 1] & 0x80000000) != 0)
			result += modulus;  // get the least positive modulus

		return result;
	}


	//***********************************************************************
	// Returns the value of the BigInteger as a byte array.  The lowest
	// index contains the MSB.
	//***********************************************************************

	public byte[] Bytes
	{
		get
		{
			int numBits = bitCount();

			int numBytes = numBits >> 3;
			if((numBits & 0x7) != 0)
				numBytes++;

			byte[] result = new byte[numBytes];

			int pos = 0;
			uint tempVal, val = data[dataLength - 1];

			if((tempVal = (val >> 24 & 0xFF)) != 0)
				result[pos++] = (byte)tempVal;
			if((tempVal = (val >> 16 & 0xFF)) != 0)
				result[pos++] = (byte)tempVal;
			if((tempVal = (val >> 8 & 0xFF)) != 0)
				result[pos++] = (byte)tempVal;
			if((tempVal = (val & 0xFF)) != 0)
				result[pos++] = (byte)tempVal;

			for(int i = dataLength - 2; i >= 0; i--, pos += 4)
			{
				val = data[i];
				result[pos+3] = (byte)(val & 0xFF);
				val >>= 8;
				result[pos+2] = (byte)(val & 0xFF);
				val >>= 8;
				result[pos+1] = (byte)(val & 0xFF);
				val >>= 8;
				result[pos] = (byte)(val & 0xFF);
			}

			return result;
		}
	}

	//***********************************************************************
	// Sets the value of the specified bit to 1
	// The Least Significant Bit position is 0.
	//***********************************************************************

	public void setBit(uint bitNum)
	{
		uint bytePos = bitNum >> 5;             // divide by 32
		byte bitPos = (byte)(bitNum & 0x1F);    // get the lowest 5 bits

		uint mask = (uint)1 << bitPos;
		data[bytePos] |= mask;

		if(bytePos >= dataLength)
			dataLength = (int)bytePos + 1;
	}


	//***********************************************************************
	// Sets the value of the specified bit to 0
	// The Least Significant Bit position is 0.
	//***********************************************************************

	public void unsetBit(uint bitNum)
	{
		uint bytePos = bitNum >> 5;

		if(bytePos < dataLength)
		{
			byte bitPos = (byte)(bitNum & 0x1F);

			uint mask = (uint)1 << bitPos;
			uint mask2 = 0xFFFFFFFF ^ mask;

			data[bytePos] &= mask2;

			if(dataLength > 1 && data[dataLength - 1] == 0)
				dataLength--;
		}
	}

	//***********************************************************************
	// Returns a value that is equivalent to the integer square root
	// of the BigInteger.
	//
	// The integer square root of "this" is defined as the largest integer n
	// such that (n * n) <= this
	//
	//***********************************************************************

	public BigInteger Sqrt
	{
		get
		{
			uint numBits = (uint)bitCount();

			if((numBits & 0x1) != 0)        // odd number of bits
				numBits = (numBits >> 1) + 1;
			else
				numBits = (numBits >> 1);

			uint bytePos = numBits >> 5;
			byte bitPos = (byte)(numBits & 0x1F);

			uint mask;

			BigInteger result = new BigInteger();
			if(bitPos == 0)
				mask = 0x80000000;
			else
			{
				mask = (uint)1 << bitPos;
				bytePos++;
			}
			result.dataLength = (int)bytePos;

			for(int i = (int)bytePos - 1; i >= 0; i--)
			{
				while(mask != 0)
				{
					// guess
					result.data[i] ^= mask;

					// undo the guess if its square is larger than this
					if((result * result) > this)
						result.data[i] ^= mask;

					mask >>= 1;
				}
				mask = 0x80000000;
			}
			return result;
		}
	}

	public BigInteger Pow(int exp)
	{
		if (exp < 0)
			throw new FormatException("Exponent is negative");
 
		if (exp == 0)
			return new BigInteger(0);

		BigInteger result = new BigInteger(this);
		for (int i = 0; i < exp - 1; i++)
			result *= this;

		return result;
	}
}

public class BigDecimal
{
	private BigInteger m_int;
	private int m_scale;
	private string m_sep = CultureInfo.CurrentCulture.NumberFormat.CurrencyDecimalSeparator;

	public BigDecimal()
	{
		m_int = new BigInteger(0);
		m_scale = 0;
	}

	public BigDecimal(long num)
	{
		m_int = new BigInteger(num);
		m_scale = 0;
	}

	public BigDecimal(ulong num)
	{
		m_int = new BigInteger(num);
		m_scale = 0;
	}

	public BigDecimal(double num) : this(num.ToString())
	{
	}

	public BigDecimal(BigInteger num) : this(num, 0)
	{
	}

	public BigDecimal(BigInteger num, int scale)
	{
		m_int = num;
		m_scale = scale;
	}

	public BigDecimal(string num)
	{
		string int_part;
		string float_part;
		int s = 0;
		int cur_off = 0;
			
		if (num.Length > 0)
		{
			num = num.Replace(CultureInfo.CurrentCulture.NumberFormat.CurrencyGroupSeparator, string.Empty);
			int dot_index = num.IndexOf(m_sep, cur_off);

			if (dot_index >= 0)
			{
				if (num.IndexOf(m_sep, dot_index + 1) >=0 )
					throw new FormatException();

				int_part = num.Substring(0, dot_index - cur_off).TrimStart('0');
				cur_off = dot_index + 1;
			}
			else
			{
				int_part = num.TrimStart('0');
				m_int = new BigInteger(long.Parse(int_part));
				m_scale = 0;
				return;
			}

			int exp_index = num.IndexOf('e', cur_off);
			if (exp_index < 0) exp_index = num.IndexOf('E', cur_off);

			if (exp_index >= 0)
			{
				int last_exp_index = num.IndexOf('e', exp_index + 1);
				if (last_exp_index < 0) last_exp_index = num.IndexOf('E', exp_index + 1);
				if (last_exp_index >= 0)
					throw new FormatException();
			
				float_part = num.Substring(cur_off, exp_index - cur_off).TrimEnd('0');
				cur_off = exp_index + 1; 
			}
			else
			{
				float_part = num.Substring(cur_off).TrimEnd('0');
				m_int = new BigInteger(long.Parse(int_part + float_part));
				m_scale = float_part.Length;
				return;
			}

			if (cur_off < num.Length)
				s = int.Parse(num.Substring(cur_off)); 

			m_int = new BigInteger(long.Parse(int_part+float_part));
			m_scale = float_part.Length - s;
		}
		else
		{
			m_int = new BigInteger(0);
			m_scale = 0;
		}
	}

	public int Scale
	{
		get
		{
			return m_scale;
		}
	}

	public BigDecimal setScale(int val)
	{
		BigInteger ten = new BigInteger(10);
		BigInteger num = m_int;
		if (val > m_scale)
			for (int i = 0; i < val - m_scale; i++)
				num *= ten;
		else
			for (int i = 0; i < m_scale - val; i++)
				num /= ten;

		return new BigDecimal(num, val);
	}

	public BigInteger unscaledValue
	{
		get
		{
			return m_int;
		}
	}

	public static BigDecimal operator + (BigDecimal bd1, BigDecimal bd2)
	{
		int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
		return new BigDecimal(bd1.setScale(scale).unscaledValue + bd2.setScale(scale).unscaledValue, scale);
	}

	public static BigDecimal operator - (BigDecimal bd)
	{
		return new BigDecimal( - bd.unscaledValue, bd.Scale);
	}

	public static BigDecimal operator - (BigDecimal bd1, BigDecimal bd2)
	{
		int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
		return new BigDecimal(bd1.setScale(scale).unscaledValue - bd2.setScale(scale).unscaledValue, scale);
	}

	public static BigDecimal operator * (BigDecimal bd1, BigDecimal bd2)
	{
		return new BigDecimal(bd1.unscaledValue * bd2.unscaledValue, bd1.Scale + bd2.Scale);
	}

	public static BigDecimal operator / (BigDecimal bd1, BigDecimal bd2)
	{
		throw new NotImplementedException();
	}

	public static bool operator < (BigDecimal bd1, BigDecimal bd2)
	{
		int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
		return bd1.setScale(scale).unscaledValue < bd2.setScale(scale).unscaledValue;
	}

	public static bool operator > (BigDecimal bd1, BigDecimal bd2)
	{
		int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
		return bd1.setScale(scale).unscaledValue > bd2.setScale(scale).unscaledValue;
	}

	public static bool operator <= (BigDecimal bd1, BigDecimal bd2)
	{
		return !(bd1 > bd2);
	}

	public static bool operator >= (BigDecimal bd1, BigDecimal bd2)
	{
		return !(bd1 < bd2);
	}

	public static bool operator == (BigDecimal bd1, BigDecimal bd2)
	{
		int scale = (bd1.Scale > bd2.Scale ? bd1.Scale : bd2.Scale);
		return bd1.setScale(scale).unscaledValue == bd2.setScale(scale).unscaledValue;
	}

	public static bool operator != (BigDecimal bd1, BigDecimal bd2)
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

	public static explicit operator double(BigDecimal val)
	{
		return double.Parse(val.ToString());
	}

	public static explicit operator decimal(BigDecimal val)
	{
		return decimal.Parse(val.ToString());
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

	public BigDecimal Clone()
	{
		return new BigDecimal(m_int, m_scale);
	}

	public BigDecimal movePointLeft(int n)
	{
		if (n >= 0)
			return new BigDecimal(m_int, m_scale + n);
		else
			return movePointRight(-n);
	}

	public BigDecimal movePointRight(int n)
	{
		if (n >= 0)
		{
			if (m_scale >= n)
				return new BigDecimal(m_int, m_scale -n);
			else
			{
				BigInteger ten = new BigInteger(10);
				BigInteger num = m_int;
				for (int i = 0; i < n - m_scale; i++)
					num *= ten;

				return new BigDecimal(num);
			}
		}
		else
			return movePointLeft(-n);
	}

	public override string ToString()
	{
		string s_num = m_int.ToString();
		int s_scale = m_scale;
		if (m_int < 0)
			s_num = s_num.Remove(0, 1);
		if (s_scale < 0)
		{
			s_num = s_num.PadRight(s_num.Length - s_scale, '0');
			s_scale = 0;
		}
		if (s_scale >= s_num.Length)
			s_num = s_num.PadLeft(s_scale + 1, '0');

		s_num = (m_int >= 0 ? string.Empty : "-") + s_num.Insert(s_num.Length - s_scale, m_sep);
		if (s_num.EndsWith(m_sep))
			s_num += "0";

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
