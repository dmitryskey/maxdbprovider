using System;
using System.Security.Cryptography;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	#region "Sample HMACMD5 implementation"

	public class HMACMD5 : KeyedHashAlgorithm 
	{
		private MD5            hash1;
		private MD5            hash2;
		private bool            bHashing = false;

		private byte[]          rgbInner = new byte[64];
		private byte[]          rgbOuter = new byte[64];

		public HMACMD5 (byte[] rgbKey) 
		{
			HashSizeValue = 128;
			// Create the hash algorithms.
			hash1 = MD5.Create();
			hash2 = MD5.Create();
			// Get the key.
			if (rgbKey.Length > 64) 
			{
				KeyValue = hash1.ComputeHash(rgbKey);
				// No need to call Initialize; ComputeHash does it automatically.
			}
			else 
			{
				KeyValue = (byte[]) rgbKey.Clone();
			}
			// Compute rgbInner and rgbOuter.
			int i = 0;
			for (i=0; i<64; i++) 
			{ 
				rgbInner[i] = 0x36;
				rgbOuter[i] = 0x5C;
			}
			for (i=0; i<KeyValue.Length; i++) 
			{
				rgbInner[i] ^= KeyValue[i];
				rgbOuter[i] ^= KeyValue[i];
			}        
		}    

		public override byte[] Key 
		{
			get { return (byte[]) KeyValue.Clone(); }
			set 
			{
				if (bHashing) 
				{
					throw new Exception("Cannot change key during hash operation");
				}
				if (value.Length > 64) 
				{
					KeyValue = hash1.ComputeHash(value);
					// No need to call Initialize; ComputeHash does it automatically.
				}
				else 
				{
					KeyValue = (byte[]) value.Clone();
				}
				// Compute rgbInner and rgbOuter.
				int i = 0;
				for (i=0; i<64; i++) 
				{ 
					rgbInner[i] = 0x36;
					rgbOuter[i] = 0x5C;
				}
				for (i=0; i<KeyValue.Length; i++) 
				{
					rgbInner[i] ^= KeyValue[i];
					rgbOuter[i] ^= KeyValue[i];
				}
			}
		}
		public override void Initialize() 
		{
			hash1.Initialize();
			hash2.Initialize();
			bHashing = false;
		}
		protected override void HashCore(byte[] rgb, int ib, int cb) 
		{
			if (bHashing == false) 
			{
				hash1.TransformBlock(rgbInner, 0, 64, rgbInner, 0);
				bHashing = true;                
			}
			hash1.TransformBlock(rgb, ib, cb, rgb, ib);
		}

		protected override byte[] HashFinal() 
		{
			if (bHashing == false) 
			{
				hash1.TransformBlock(rgbInner, 0, 64, rgbInner, 0);
				bHashing = true;                
			}
			// Finalize the original hash.
			hash1.TransformFinalBlock(new byte[0], 0, 0);
			// Write the outer array.
			hash2.TransformBlock(rgbOuter, 0, 64, rgbOuter, 0);
			// Write the inner hash and finalize the hash.
			hash2.TransformFinalBlock(hash1.Hash, 0, hash1.Hash.Length);
			bHashing = false;
			return hash2.Hash;
		}        
	}

	#endregion

	#region "Cryptography class"

	/// <summary>
	/// Summary description for Crypt.
	/// </summary>
	public class Crypt
	{
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
				client_proof[i] = (byte) (shared_key[i] ^ client_key[i]);
			return client_proof;
		}

		public static byte[] Mangle(string passwd,	bool isUnicode)
		{
			const int vp1 = 2;
			const int vp2 = 521;
			const int vp3 = 133379;
			const int maxPasswdLen = 18;
			ByteArray passwdBytes = new ByteArray(maxPasswdLen, false);
			if (passwd.Length > maxPasswdLen)
				passwd = passwd.Substring(0, maxPasswdLen);
			else
				passwd = passwd.PadRight(maxPasswdLen, ' ');
        
			if (isUnicode) 
				passwdBytes.writeUnicode(passwd, 0);
			else 
				passwdBytes.writeASCII(passwd, 0);

			int[] crypt = new int[6];
			int left, right;
			ByteArray result;

			for (int i = 1; i <= 6; ++i) 
				crypt[i - 1] = passwdBytes.ReadByte(3 * i - 3) * vp3 + passwdBytes.ReadByte(3 * i - 2) * vp2 + passwdBytes.ReadByte(3 * i - 1) * vp1;

			for (int i = 1; i <= 6; ++i) 
			{
				if (i > 1) 
					left = crypt[i-2];
				else 
					left = vp3;
				crypt[i-1] += left % 61 * (vp3 * 126 - 1);
			}

			for (int i = 6; i >= 1; --i) 
			{
				if (i < 5) 
					right = crypt[i];
				else 
					right = vp2;
				crypt[i-1] += right % 61 * (vp3 * 128 - 1);
			}

			for (int i = 0; i < 6; ++i) 
				if ((crypt[i] & 1) != 0) 
					crypt[i] = -crypt[i];

			result = new ByteArray(6 * 4, false);
			
			for (int i = 0; i < 6; ++i) 
				result.WriteInt32(crypt [i], i * 4);

			return result.arrayData;
		}
	}

	#endregion

	#region "Authentication Class"

	public class Auth
	{
		private byte[] salt;
    
		private byte[] clientchallenge;
    
		private byte[] serverchallenge;
    
		private int maxPasswordLen = 0;

		public Auth()
		{
			clientchallenge = new byte[64];
			(new RNGCryptoServiceProvider()).GetBytes(clientchallenge);
		}

		public byte[] ClientChallenge
		{
			get
			{
				return clientchallenge;
			}
		}

		public int MaxPasswordLength
		{
			get
			{
				return maxPasswordLen;
			}
		}

		public byte[] GetClientProof(byte[] password)
		{
			return Crypt.ScrammMD5(salt, password, clientchallenge, serverchallenge);
		}

		// Parses the serverchallenge and split it into salt and real server challenge.
		public void ParseServerChallenge(DataPartVariable vData)
		{
			if (!vData.NextRow() || !vData.NextField())
				throw new MaxDBSQLException(MessageTranslator.Translate
					(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

			string alg = vData.readASCII(vData.CurrentOffset, vData.CurrentFieldLen);
			if (alg.ToUpper().Trim() != Crypt.ScramMD5Name)
				throw new MaxDBSQLException(MessageTranslator.Translate
					(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

			if (!vData.NextField() || vData.CurrentFieldLen < 8)
				throw new MaxDBSQLException(MessageTranslator.Translate
					(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

			if (vData.CurrentFieldLen == 40)
			{
				// first version of challenge response should only occurs with database version 7.6.0.0 <= kernel <= 7.6.0.7
				salt = vData.ReadBytes(vData.CurrentOffset, 8);
				serverchallenge = vData.ReadBytes(vData.CurrentOffset + 8, vData.CurrentFieldLen - 8);
			}
			else
			{
				DataPartVariable vd = new DataPartVariable(new ByteArray(vData.ReadBytes(vData.CurrentOffset, vData.CurrentFieldLen), 0, vData.origData.Swapped), 1);
				if (!vd.NextRow() || !vd.NextField())
					throw new MaxDBSQLException(MessageTranslator.Translate
						(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
							Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

				salt = vd.ReadBytes(vd.CurrentOffset, vd.CurrentFieldLen);
				if (!vd.NextField())
					throw new MaxDBSQLException(MessageTranslator.Translate
						(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
							Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

				serverchallenge = vd.ReadBytes(vd.CurrentOffset, vd.CurrentFieldLen);

				// from Version 7.6.0.10 on also the max password length will be delivered
				if (vData.NextField())
				{
					DataPartVariable mp_vd = new DataPartVariable(new ByteArray(vData.ReadBytes(vData.CurrentOffset, vData.CurrentFieldLen), 0, vData.origData.Swapped), 1);
					if (!mp_vd.NextRow() || !mp_vd.NextField()) 
						throw new MaxDBSQLException(MessageTranslator.Translate
							(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
								Logger.ToHexString(vData.ReadBytes(0, vData.Length))));

					do 
					{
						if (mp_vd.readASCII(mp_vd.CurrentOffset, mp_vd.CurrentFieldLen).ToLower().Trim() == Packet.MaxPasswordLenTag)
						{
							if (!mp_vd.NextField()) 
								throw new MaxDBSQLException(MessageTranslator.Translate
									(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
										Logger.ToHexString(vData.ReadBytes(0, vData.Length))));
							else
							{
								try 
								{
									maxPasswordLen = int.Parse(mp_vd.readASCII(mp_vd.CurrentOffset, mp_vd.CurrentFieldLen));
								} 
								catch
								{
									throw new MaxDBSQLException(MessageTranslator.Translate
										(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
											Logger.ToHexString(vData.ReadBytes(0, vData.Length))));
								} 
							}
						} 
						else 
						{
							if (!mp_vd.NextField()) 
								throw new MaxDBSQLException(MessageTranslator.Translate
									(MessageKey.ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED, 
										Logger.ToHexString(vData.ReadBytes(0, vData.Length))));
						}     
					} 
					while (mp_vd.NextField());
				}
			}
		}
	}

	#endregion
}
