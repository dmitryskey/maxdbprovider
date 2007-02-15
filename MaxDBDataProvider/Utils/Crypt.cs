//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.Text;
using System.Security.Cryptography;
using MaxDB.Data.MaxDBProtocol;
using System.Globalization;

namespace MaxDB.Data.Utilities
{
#if SAFE

	#region "Sample HMACMD5 implementation"

	internal class HMACMD5 : KeyedHashAlgorithm
	{
		private MD5 hashOne;
		private MD5 hashTwo;
		private bool bHashing;

		private byte[] rgbInner = new byte[64];
		private byte[] rgbOuter = new byte[64];

		public HMACMD5(byte[] rgbKey)
		{
			HashSizeValue = 128;
			// Create the hash algorithms.
			hashOne = MD5.Create();
			hashTwo = MD5.Create();
			// Get the key.
			if (rgbKey.Length > 64)
			{
				KeyValue = hashOne.ComputeHash(rgbKey);
				// No need to call Initialize; ComputeHash does it automatically.
			}
			else
			{
				KeyValue = (byte[])rgbKey.Clone();
			}
			// Compute rgbInner and rgbOuter.
			int i = 0;
			for (i = 0; i < 64; i++)
			{
				rgbInner[i] = 0x36;
				rgbOuter[i] = 0x5C;
			}
			for (i = 0; i < KeyValue.Length; i++)
			{
				rgbInner[i] ^= KeyValue[i];
				rgbOuter[i] ^= KeyValue[i];
			}
		}

		public override byte[] Key
		{
			get
			{
				return (byte[])KeyValue.Clone();
			}
			set
			{
				if (bHashing)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HASH_CHANGE_KEY));
				if (value.Length > 64)
				{
					KeyValue = hashOne.ComputeHash(value);
					// No need to call Initialize; ComputeHash does it automatically.
				}
				else
				{
					KeyValue = (byte[])value.Clone();
				}
				// Compute rgbInner and rgbOuter.
				int i = 0;
				for (i = 0; i < 64; i++)
				{
					rgbInner[i] = 0x36;
					rgbOuter[i] = 0x5C;
				}
				for (i = 0; i < KeyValue.Length; i++)
				{
					rgbInner[i] ^= KeyValue[i];
					rgbOuter[i] ^= KeyValue[i];
				}
			}
		}
		public override void Initialize()
		{
			hashOne.Initialize();
			hashTwo.Initialize();
			bHashing = false;
		}
		protected override void HashCore(byte[] rgb, int ib, int cb)
		{
			if (bHashing == false)
			{
				hashOne.TransformBlock(rgbInner, 0, 64, rgbInner, 0);
				bHashing = true;
			}
			hashOne.TransformBlock(rgb, ib, cb, rgb, ib);
		}

		protected override byte[] HashFinal()
		{
			if (bHashing == false)
			{
				hashOne.TransformBlock(rgbInner, 0, 64, rgbInner, 0);
				bHashing = true;
			}
			// Finalize the original hash.
			hashOne.TransformFinalBlock(new byte[0], 0, 0);
			// Write the outer array.
			hashTwo.TransformBlock(rgbOuter, 0, 64, rgbOuter, 0);
			// Write the inner hash and finalize the hash.
			hashTwo.TransformFinalBlock(hashOne.Hash, 0, hashOne.Hash.Length);
			bHashing = false;
			return hashTwo.Hash;
		}
	}

	#endregion

	#region "Cryptography class"

	/// <summary>
	/// Summary description for Crypt.
	/// </summary>
	internal class Crypt
	{
		Crypt()
		{
		}

		public static string ScramMD5Name
		{
			get
			{
				return "SCRAMMD5";
			}
		}

		//		  This section is designed to provide a quick understanding of SCRAM for
		//		  those who like functional notation.
		//		   + octet concatenation XOR the exclusive-or function AU is the
		//		  authentication user identity (NUL terminated) AZ is the authorization
		//		  user identity (NUL terminated) if AZ is the same as AU, a single NUL is
		//		  used instead. csecinfo client security layer option bits and buffer size
		//		  ssecinfo server security layer option bits and buffer size service is the
		//		  name of the service and server (NUL terminated) pass is the plain-text
		//		  passphrase H(x) is a one-way hash function applied to "x", such as MD5
		//		  MAC(x,y) is a message authentication code (MAC) such as HMAC-MD5 "y" is
		//		  the key and "x" is the text signed by the key. salt is a per-user salt
		//		  value the server stores Us is a unique nonce the server sends to the
		//		  client Uc is a unique nonce the client sends to the server
		//		  
		//		  The SCRAM computations and exchange are as follows:
		//		  
		//		  client-msg-1 = AZ + AU + Uc (1) client -> server: client-msg-1
		//		  server-msg-1 = salt + ssecinfo + service + Us (2) server -> client:
		//		  server-msg-1 salted-pass = MAC(salt, pass) client-key = H(salted-pass)
		//		  client-verifier = H(client-key) shared-key = MAC(server-msg-1 +
		//		  client-msg-1 + csecinfo, client-verifier) client-proof = client-key XOR
		//		  shared-key (3) client -> server: csecinfo + client-proof server-key =
		//		  MAC(salt, salted-pass) server-proof = MAC(client-msg-1 + server-msg-1 +
		//		  csecinfo, server-key) (4) server -> client: server-proof

		public static byte[] ScrammMD5(byte[] salt, byte[] password, byte[] clientkey, byte[] serverkey)
		{
			MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();

			byte[] salted_pass = (new HMACMD5(password)).ComputeHash(salt);
			byte[] client_key = md5.ComputeHash(salted_pass);
			byte[] client_verifier = md5.ComputeHash(client_key);

			int saltLen = salt.Length;
			int serverkeyLen = serverkey.Length;
			int clientkeyLen = clientkey.Length;
			byte[] content = new byte[saltLen + serverkeyLen + clientkeyLen];
			Array.Copy(salt, 0, content, 0, saltLen);
			Array.Copy(serverkey, 0, content, saltLen, serverkeyLen);
			Array.Copy(clientkey, 0, content, saltLen + serverkeyLen, clientkey.Length);

			byte[] shared_key = (new HMACMD5(client_verifier)).ComputeHash(content);

			byte[] client_proof = new byte[shared_key.Length];
			for (int i = shared_key.Length - 1; i >= 0; i--)
				client_proof[i] = (byte)(shared_key[i] ^ client_key[i]);
			return client_proof;
		}

		public static byte[] Mangle(string passwd, bool isUnicode)
		{
			const int vp1 = 2;
			const int vp2 = 521;
			const int vp3 = 133379;
			const int maxPasswdLen = 18;
			ByteArray passwdBytes = new ByteArray(maxPasswdLen);
			if (passwd.Length > maxPasswdLen)
				passwd = passwd.Substring(0, maxPasswdLen);
			else
				passwd = passwd.PadRight(maxPasswdLen);

			if (isUnicode)
				passwdBytes.WriteUnicode(passwd, 0);
			else
				passwdBytes.WriteAscii(passwd, 0);

			int[] crypt = new int[6];
			int left, right;
			ByteArray result;

			for (int i = 1; i <= 6; ++i)
				crypt[i - 1] = passwdBytes.ReadByte(3 * i - 3) * vp3 + passwdBytes.ReadByte(3 * i - 2) * vp2 + passwdBytes.ReadByte(3 * i - 1) * vp1;

			for (int i = 1; i <= 6; ++i)
			{
				if (i > 1)
					left = crypt[i - 2];
				else
					left = vp3;
				crypt[i - 1] += left % 61 * (vp3 * 126 - 1);
			}

			for (int i = 6; i >= 1; --i)
			{
				if (i < 5)
					right = crypt[i];
				else
					right = vp2;
				crypt[i - 1] += right % 61 * (vp3 * 128 - 1);
			}

			for (int i = 0; i < 6; ++i)
				if ((crypt[i] & 1) != 0)
					crypt[i] = -crypt[i];

			result = new ByteArray(6 * 4);

			for (int i = 0; i < 6; ++i)
				result.WriteInt32(crypt[i], i * 4);

			return result.GetArrayData();
		}
	}

	#endregion

	#region "Authentication Class"

	internal class Auth
	{
		private byte[] bySalt;

		private byte[] byClientChallenge;

		private byte[] byServerChallenge;

		private int iMaxPasswordLen;

		public Auth()
		{
			byClientChallenge = new byte[64];
			(new RNGCryptoServiceProvider()).GetBytes(byClientChallenge);
		}

		public byte[] ClientChallenge
		{
			get
			{
				return byClientChallenge;
			}
		}

		public int MaxPasswordLength
		{
			get
			{
				return iMaxPasswordLen;
			}
		}

		public byte[] GetClientProof(byte[] password)
		{
			return Crypt.ScrammMD5(bySalt, password, byClientChallenge, byServerChallenge);
		}

		// Parses the serverchallenge and split it into salt and real server challenge.
		public void ParseServerChallenge(DataPartVariable vData)
		{
			if (!vData.NextRow() || !vData.NextField())
				throw new MaxDBException(MaxDBMessages.Extract
					(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED, Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

			string alg = vData.ReadAscii(vData.CurrentOffset, vData.CurrentFieldLen);
			if (alg.ToUpper(CultureInfo.InvariantCulture).Trim() != Crypt.ScramMD5Name)
				throw new MaxDBException(MaxDBMessages.Extract
					(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED, Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

			if (!vData.NextField() || vData.CurrentFieldLen < 8)
				throw new MaxDBException(MaxDBMessages.Extract
					(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED, Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

			if (vData.CurrentFieldLen == 40)
			{
				// first version of challenge response should only occurs with database version 7.6.0.0 <= kernel <= 7.6.0.7
				bySalt = vData.ReadBytes(vData.CurrentOffset, 8);
				byServerChallenge = vData.ReadBytes(vData.CurrentOffset + 8, vData.CurrentFieldLen - 8);
			}
			else
			{
				DataPartVariable vd = new DataPartVariable(new ByteArray(vData.ReadBytes(vData.CurrentOffset, vData.CurrentFieldLen), 0, vData.baOrigData.Swapped), 1);
				if (!vd.NextRow() || !vd.NextField())
					throw new MaxDBException(MaxDBMessages.Extract
						(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
							Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

				bySalt = vd.ReadBytes(vd.CurrentOffset, vd.CurrentFieldLen);
				if (!vd.NextField())
					throw new MaxDBException(MaxDBMessages.Extract
						(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
							Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

				byServerChallenge = vd.ReadBytes(vd.CurrentOffset, vd.CurrentFieldLen);

				// from Version 7.6.0.10 on also the max password length will be delivered
				if (vData.NextField())
				{
					DataPartVariable mp_vd = new DataPartVariable(new ByteArray(vData.ReadBytes(vData.CurrentOffset, vData.CurrentFieldLen), 0, vData.baOrigData.Swapped), 1);
					if (!mp_vd.NextRow() || !mp_vd.NextField())
						throw new MaxDBException(MaxDBMessages.Extract
							(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
								Consts.ToHexString(vData.ReadBytes(0, vData.Length))));

					do
					{
						if (string.Compare(mp_vd.ReadAscii(mp_vd.CurrentOffset, mp_vd.CurrentFieldLen).Trim(),
							Packet.MaxPasswordLenTag, true, CultureInfo.InvariantCulture) == 0)
						{
							if (!mp_vd.NextField())
								throw new MaxDBException(MaxDBMessages.Extract
									(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
										Consts.ToHexString(vData.ReadBytes(0, vData.Length))));
							else
							{
								try
								{
									iMaxPasswordLen = int.Parse(mp_vd.ReadAscii(mp_vd.CurrentOffset, mp_vd.CurrentFieldLen), CultureInfo.InvariantCulture);
								}
								catch
								{
									throw new MaxDBException(MaxDBMessages.Extract
										(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
											Consts.ToHexString(vData.ReadBytes(0, vData.Length))));
								}
							}
						}
						else
						{
							if (!mp_vd.NextField())
								throw new MaxDBException(MaxDBMessages.Extract
									(MaxDBError.CONNECTION_WRONGSERVERCHALLENGERECEIVED,
										Consts.ToHexString(vData.ReadBytes(0, vData.Length))));
						}
					}
					while (mp_vd.NextField());
				}
			}
		}
	}

	#endregion

#endif // SAFE
}
