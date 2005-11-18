using System;
using System.Security.Cryptography;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for Crypt.
	/// </summary>
	public class SCRAMMD5
	{
		public static string AlgName
		{
			get
			{
				return "SCRAMMD5";
			}
		}

		private static byte[] hmacMD5(byte[] data, byte[] key)  
		{
			byte[] ipad = new byte[64];
			byte[] opad = new byte[64];
			for (int i = 0; i < 64; i++) 
			{
				ipad[i] = (byte) 0x36;
				opad[i] = (byte) 0x5c;
			}
			for (int i = key.Length - 1; i >= 0; i--) 
			{
				ipad[i] ^= key[i];
				opad[i] ^= key[i];
			}
			byte[] content = new byte[data.Length + 64];
			Array.Copy(ipad, 0, content, 0, 64);
			Array.Copy(data, 0, content, 64, data.Length);
			MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();
			data = md5.ComputeHash(content);
			content = new byte[data.Length + 64];
			Array.Copy(opad, 0, content, 0, 64);
			Array.Copy(data, 0, content, 64, data.Length);
			return md5.ComputeHash(content);
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

		public static byte[] scrammMD5(byte[] salt, byte[] password, byte[] clientkey, byte[] serverkey) 
		{
			MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();

			byte[] salted_pass = hmacMD5(salt, password);

			byte[] client_key = md5.ComputeHash(salted_pass);

			byte[] client_verifier = md5.ComputeHash(client_key);

			int saltLen = salt.Length;
			int serverkeyLen = serverkey.Length;
			int clientkeyLen = clientkey.Length;
			byte[] content = new byte[saltLen + serverkeyLen + clientkeyLen];
			Array.Copy(salt, 0, content, 0, saltLen);
			Array.Copy(serverkey, 0, content, saltLen, serverkeyLen);
			Array.Copy(clientkey, 0, content, saltLen + serverkeyLen, clientkey.Length);

			byte[] shared_key = hmacMD5(content, client_verifier);

			byte[] client_proof = new byte[shared_key.Length];
			for (int i = shared_key.Length - 1; i >= 0; i--) 
				client_proof[i] = (byte) (shared_key[i] ^ client_key[i]);
			return client_proof;
		}
	}
}
