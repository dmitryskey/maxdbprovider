using System;
using System.Text;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
#if NATIVE

	public abstract class VDNNumber 
	{
		private const int zeroExponentValue = 128;
		private const int tensComplement = 9;
		private const int numberBytes = 20;
		private const int numberDigits = 38;
		private readonly BigDecimal zero = new BigDecimal(0);
		private const String zerostring = "0000000000000000000000000000000000000000000000000000000000000000";

		public static byte[] BigDecimal2Number(BigDecimal dec) 
		{
			return BigDecimal2Number(dec, numberDigits);
		}

		public static string BigDecimal2PlainString(BigDecimal val) 
		{
			string res;
			int scale = val.Scale;
			if (scale < 0)
			{
				val = val.setScale(0);
				scale = 0;
			}
			if (scale == 0)
				res = ((BigInteger)val).ToString();
			else
			{
				string unsignedIntVal = val.unscaledValue.Abs.ToString();
				string prefix = (val < 0 ? "-0." : "0.");
				int pointPos = unsignedIntVal.Length - scale;
				if (pointPos == 0) 
					res = prefix + unsignedIntVal;
				else if (pointPos > 0) 
				{
					StringBuilder buf = new StringBuilder(unsignedIntVal);
					buf.Insert(pointPos, '.');
					if (val < 0)
						buf.Insert(0, '-');
					res = buf.ToString();
				} 
				else 
				{
					StringBuilder buf = new StringBuilder(3-pointPos + unsignedIntVal.Length);
					buf.Append(prefix);
					for (int i=0; i< -pointPos; i++)
						buf.Append('0');
					buf.Append(unsignedIntVal);
					res = buf.ToString();
				}
			}
			return res;
		}

		public static byte [] BigDecimal2Number (BigDecimal dec, int validDigits) 
		{
			byte[] number;
			string plain = BigDecimal2PlainString(dec);
			int scale = (dec.Scale < 0) ? 0 : dec.Scale;
			char[] chars = plain.ToCharArray();
			bool isNegative;
			int firstDigit;
			int exponent;
			int digitCount = chars.Length;

			if (chars [0] == '-') 
			{
				isNegative = true;
				firstDigit = 1;
			}
			else 
			{
				isNegative = false;
				firstDigit = 0;
			}
			while ((chars [firstDigit] == '0' || chars [firstDigit] == '.') && firstDigit < digitCount - 1) 
				firstDigit++;

			exponent = chars.Length - firstDigit - scale;
			digitCount = chars.Length - firstDigit;
			if ((digitCount == 1) && (chars [firstDigit] == '0')) 
				return new byte[]{(byte) zeroExponentValue};
        
			if (exponent > 0 && scale > 0) 
			{
				// adjust place taken by decimal point
				exponent--;
				digitCount--;
				Array.Copy(chars, chars.Length - scale, chars, chars.Length - scale - 1, scale);
			}
			if (digitCount > validDigits) 
			{
				if (chars [firstDigit + validDigits] >= '5') 
					chars[firstDigit + validDigits]++;
            
				digitCount = validDigits;
			}
			for (int i = firstDigit; i < digitCount + firstDigit; ++i) 
				chars [i] -= '0';
        
			number = new byte [digitCount + 1];
			packDigits (chars, firstDigit, digitCount, isNegative, number);
			if (isNegative) 
				exponent = 64 - exponent;
			else 
				exponent += 192;
        
			number[0] = (byte) exponent;
			return number;
		}

		public static byte[] Long2Number (long val) 
		{
			bool isNegative = false;
			int negativeVal = 1;
			byte[] number ;
			char[] scratch = new char[numberDigits + 1];
			char digit;
			int scratchPos = numberDigits - 1; 
			int exponent;

			if (val == 0) 
				return new byte[]{(byte) zeroExponentValue};
        
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
				digit = (char)(negativeVal*(val % 10));
				scratch [scratchPos] = digit;
				val /= 10;
				scratchPos--;
			}
			exponent = numberDigits - scratchPos - 1;
			scratchPos++;
			number = new byte[numberDigits - scratchPos +1 ];
			packDigits(scratch, scratchPos, numberDigits - scratchPos, isNegative, number);
			if (isNegative) 
				exponent = 64 - exponent;
			else 
				exponent += 192;
        
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
				if (characteristic == zeroExponentValue) 
					return new BigDecimal(0);
				digits = new byte [digitCount + 2];
				if (characteristic < zeroExponentValue) 
				{
					exponent = - (characteristic - 64);
					digits[0] = (byte) '-';
					digits[1] = (byte) '.';

					for (int i = 1; i < rawNumber.Length; i++) 
					{
						int val;
						val = ((((char) rawNumber [i]) & 0xff) >> 4);
						if (val != 0) 
							lastSignificant = i * 2;
                  
						digits [i * 2] = (byte) (tensComplement - val + '0');
						val = (((char)rawNumber [i] ) & 0x0f);
						if (val != 0) 
							lastSignificant = i * 2 + 1;
                  
						digits [i * 2 + 1] = (byte)(tensComplement - val + '0');
					}
					digits [lastSignificant]++;
				}
				else 
				{
					exponent = characteristic - 192;
					digits [0] = (byte) '0';
					digits [1] = (byte) '.';
					for (int i = 1; i < rawNumber.Length; ++i) 
					{
						int val;
						val = ((((char) rawNumber [i]) & 0xff) >> 4);
						if (val != 0) 
							lastSignificant = i * 2;
                  
						digits [i * 2] = (byte) (val + '0');
						val = (((char)rawNumber [i] ) & 0x0f);
						if (val != 0) 
							lastSignificant = i * 2 + 1;
                  
						digits [i * 2 + 1] = (byte) (val + '0');
					}
				}
				numberString = Encoding.ASCII.GetString(digits, 0, lastSignificant + 1);
				result = new BigDecimal(numberString);
				result = result.movePointRight(exponent);
				return result;
			}
			catch (Exception) 
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONVDNnumber, Logger.ToHexString(rawNumber)));
			}
		}

		public static double ShortNumber2Double(byte[] rawNumber)
		{
			try 
			{
				long result = 0;
				int characteristic;
				int exponent;
				bool isNegative;

				characteristic = rawNumber[0] & 0xff;
				if (characteristic == zeroExponentValue)
					return 0;
        
				if (characteristic < zeroExponentValue) 
				{
					exponent = - (characteristic - 64);
					isNegative = true;
					int val;
					int nullValCnt=1;

					for (int i = 1; i < rawNumber.Length; ++i) 
					{
						//first halfbyte
						val = ((((int) rawNumber [i]) & 0xff) >> 4);
						if (val == 9) 
							nullValCnt++;
						else 
						{
							result *= (long)double10pow[nullValCnt + zeor10powindex];
							exponent -= nullValCnt;
							nullValCnt = 1;
							result += tensComplement - val;
						}

						//second halfbyte
						val = (((int)rawNumber [i] ) & 0x0f);
						if (val == 9) 
							nullValCnt++;
              
						else 
						{
							result *= (long)double10pow[nullValCnt + zeor10powindex];
							exponent -= nullValCnt;
							nullValCnt = 1;
							result += tensComplement - val;
						}
					}
					result++;
				}
				else 
				{
					exponent = characteristic - 192;
					isNegative = false;
					int val;
					int nullValCnt = 1;

					for (int i = 1; i < rawNumber.Length; ++i) 
					{
						//first halfbyte
						val = ((((int) rawNumber [i]) & 0xf0) >> 4);
						if (val == 0) 
							nullValCnt++;
						else 
						{
							result *= (long)double10pow[nullValCnt + zeor10powindex];
							exponent -= nullValCnt;
							nullValCnt = 1;
							result += val;
						}

						//second halfbyte
						val = (((int)rawNumber [i] ) & 0x0f);
						if (val == 0) 
							nullValCnt++;
						else 
						{
							result *= (long)double10pow[nullValCnt + zeor10powindex];
							exponent -= nullValCnt;
							nullValCnt = 1;
							result += val;
						}
					}
				}
				double myresult = result;
				myresult *= double10pow[exponent + zeor10powindex];
				if (isNegative) 
					myresult = -myresult;
        
				return myresult;
			}
			catch(Exception) 
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONVDNnumber, Logger.ToHexString(rawNumber)));
			}
		}

		public static long Number2Long(byte[] rawNumber)
		{
			long result = 0;
			int characteristic;
			int exponent;
			bool isNegative;
			int numberDigits = rawNumber.Length * 2 - 2;

			characteristic = rawNumber[0] & 0xff;
			if (characteristic == zeroExponentValue) 
				return 0;
        
			if (characteristic < zeroExponentValue) 
			{
				exponent = - (characteristic - 64);
				if (exponent < 0 || exponent > numberDigits) 
					return (long)(Number2BigDecimal(rawNumber));

				isNegative = true;
				for (int i = 0; i < exponent; i++) 
				{
					int val = rawNumber [i / 2 + 1];
					if ((i % 2) == 0) 
					{
						val &= 0xf0;
						val >>= 4; 
					}
					else 
						val &= 0x0f;
                
					result *= 10;
					result += tensComplement - val;
				}
				result++;
			}
			else 
			{
				exponent = characteristic - 192;
				if (exponent < 0 || exponent > numberDigits) 
					return (long)Number2BigDecimal (rawNumber);

				isNegative = false;
				for (int i = 0; i < exponent; i++) 
				{
					int val = rawNumber [i / 2 + 1];
					if ((i % 2) == 0) 
					{
						val &= 0xf0;
						val >>= 4; 
					}
					else 
						val &= 0x0f;
                
					result *= 10;
					result += val;
				}
			}
			if (isNegative) 
				result = -result;
        
			return result;
		}

		public static int Number2Int (byte[] rawBytes)
		{
			return (int) Number2Long(rawBytes);
		}

		private static void packDigits(char[] digits, int start, int count, bool isNegative, byte[] number)
		{
			int lastDigit = start + count - 1;
			byte highNibble;
			byte lowNibble;

			if (isNegative) 
			{
				// 10s complement
				for (int i = start; i < lastDigit; ++i) 
					digits [i] = (char)(9 - digits [i]);
            
				digits [lastDigit] = (char)(10 - digits [lastDigit]);
				// handle overflow
				int digitPos = lastDigit;
				while (digits [digitPos] == 10) 
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
				highNibble = (byte)digits [start];
				if ((start + 1) <= lastDigit) 
					lowNibble = (byte) digits [start + 1];
				else 
					lowNibble = 0;
            
				number [i] = (byte) (highNibble << 4 | lowNibble);
			}
		}

		public static string Number2String(byte[] number, bool fixedtype, int logicalLength, int frac)
		{
			int characteristic;

			try 
			{
				characteristic = number[0] & 0xFF;
				if(characteristic == zeroExponentValue) 
					return "0";
            
				char[] digits = new char[logicalLength];
				int exponent;
				int lastsignificant=0;
				bool isnegative=false;
				if(characteristic < zeroExponentValue) 
				{
					isnegative=true;
					exponent = - (characteristic - 64);
					for(int i=0; i < logicalLength; i++) 
					{
						int v1;
						if(i % 2 == 0) 
							v1 = (number[1 + i/2] & 0xff) >> 4;
						else 
							v1 = (number[1 + i/2] & 0xF);
                    
						if(v1 != 0) 
							lastsignificant=i;
                    
						digits[i]= (char)((9 - v1) + '0');
					}
					digits[lastsignificant]++;
				} 
				else 
				{
					exponent = characteristic - 192;
					for(int i=0; i< logicalLength; i++) 
					{
						int v1;
						if(i % 2 == 0) 
							v1 = (number[1 + i/2] & 0xFF) >> 4;
						else 
							v1 = (number[1 + i/2] & 0xF);
                    
						if(v1 != 0) 
							lastsignificant=i;
                    
						digits[i]= (char)((v1) + '0');
					}
				}
				string sign = (isnegative ? "-" : "");
				string numberstr = new string(digits, 0, lastsignificant+1);
				if(fixedtype) 
				{
					if(exponent > 0) 
					{
						if(numberstr.Length < logicalLength) 
							numberstr = numberstr + zerostring.Substring(0, logicalLength - numberstr.Length);
                    
						if(frac!=0) 
							return sign + numberstr.Substring(0, exponent) + "." + numberstr.Substring(exponent, exponent + frac);
						else 
							return sign + numberstr.Substring(0, exponent);
                    
					} 
					else 
					{
						int zeroend = frac - (-exponent) - numberstr.Length;
						if (zeroend < 0) zeroend = 0;
						return sign + "0." + zerostring.Substring(0, -exponent) + numberstr + zerostring.Substring(0, zeroend);
					}
				} 
				else 
				{
					if(exponent < -3 || exponent > 7) 
						return sign + "0." + numberstr + "E" + exponent;
					else 
					{
						switch(exponent) 
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
						if(numberstr.Length <= exponent) 
							return sign + numberstr + zerostring.Substring(0, exponent - numberstr.Length);
						else 
							return sign + numberstr.Substring(0, exponent) + "." + numberstr.Substring(exponent);
                    
					}
				}
			} 
			catch(Exception) 
			{
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONVERSIONVDNnumber,  Logger.ToHexString(number)));
			}
		}

		private const int zeor10powindex = 64;
		private static readonly double[] double10pow = {
													1.0e-64, 1.0e-63, 1.0e-62, 1.0e-61,
													1.0e-60, 1.0e-59, 1.0e-58, 1.0e-57, 1.0e-56,
													1.0e-55, 1.0e-54, 1.0e-53, 1.0e-52, 1.0e-51,
													1.0e-50, 1.0e-49, 1.0e-48, 1.0e-47, 1.0e-46,
													1.0e-45, 1.0e-44, 1.0e-43, 1.0e-42, 1.0e-41,
													1.0e-40, 1.0e-39, 1.0e-38, 1.0e-37, 1.0e-36,
													1.0e-35, 1.0e-34, 1.0e-33, 1.0e-32, 1.0e-31,
													1.0e-30, 1.0e-29, 1.0e-28, 1.0e-27, 1.0e-26,
													1.0e-25, 1.0e-24, 1.0e-23, 1.0e-22, 1.0e-21,
													1.0e-20, 1.0e-19, 1.0e-18, 1.0e-17, 1.0e-16,
													1.0e-15, 1.0e-14, 1.0e-13, 1.0e-12, 1.0e-11,
													1.0e-10, 1.0e-9 , 1.0e-8 , 1.0e-7 , 1.0e-6 ,
													1.0e-5 , 1.0e-4 , 1.0e-3 , 1.0e-2 , 1.0e-1 ,
													1.0e0,
													1.0e1 , 1.0e2 , 1.0e3 , 1.0e4 , 1.0e5,
													1.0e6 , 1.0e7 , 1.0e8 , 1.0e9 , 1.0e10,
													1.0e11, 1.0e12, 1.0e13, 1.0e14, 1.0e15,
													1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20,
													1.0e21, 1.0e22, 1.0e23, 1.0e24, 1.0e25,
													1.0e26, 1.0e27, 1.0e28, 1.0e29, 1.0e30,
													1.0e31, 1.0e32, 1.0e33, 1.0e34, 1.0e35,
													1.0e36, 1.0e37, 1.0e38, 1.0e39, 1.0e40,
													1.0e41, 1.0e42, 1.0e43, 1.0e44, 1.0e45,
													1.0e46, 1.0e47, 1.0e48, 1.0e49, 1.0e50,
													1.0e51, 1.0e52, 1.0e53, 1.0e54, 1.0e55,
													1.0e56, 1.0e57, 1.0e58, 1.0e59, 1.0e60,
													1.0e61, 1.0e62, 1.0e63, 1.0e64,
		};
	}

#endif
}
