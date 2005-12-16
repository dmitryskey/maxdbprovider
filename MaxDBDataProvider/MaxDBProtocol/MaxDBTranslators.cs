using System;
using System.IO;
using System.Resources;
using System.Text;
using System.Threading;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Message translator class"

	public class MessageTranslator
	{
		private static ResourceManager rm = new ResourceManager("MaxDBDataProvider.MaxDBProtocol.MaxDBMessages", typeof(MessageTranslator).Assembly);

		public static string Translate(string key)
		{
			return Translate(key, null);
		}

		public static string Translate(string key, object o1)
		{
			return Translate(key, new object[]{ o1 });
		}

		public static string Translate(string key, object o1, object o2)
		{
			return Translate(key, new object[]{ o1, o2 });
		}

		public static string Translate(string key, object o1, object o2, object o3)
		{
			return Translate(key, new object[]{ o1, o2, o3 });
		}

		public static string Translate(string key, object[] args) 
		{
			try 
			{
				// retrieve text and format it
				string msg = rm.GetString(key);
				if (args != null)
					return string.Format(msg, args);
				else
					return msg;
			} 
			catch(MissingManifestResourceException) 
			{
				// emergency - create an informative message in this case at least
				StringBuilder result = new StringBuilder("No message available for locale ");
				result.Append(Thread.CurrentThread.CurrentUICulture.EnglishName);
				result.Append(", key ");
				result.Append(key);
				// if arguments given append them
				if(args == null || args.Length==0) 
					result.Append(".");
				else 
				{
					result.Append(", arguments [");
					for(int i=0; i<args.Length - 1; i++) 
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length-1].ToString());
					result.Append("].");
				}
				return result.ToString();
			} 
			catch 
			{
				StringBuilder result = new StringBuilder("No message available for default locale ");
				result.Append("for key ");
				result.Append(key);
				// if arguments given append them
				if(args == null || args.Length==0) 
					result.Append(".");
				else 
				{
					result.Append(", arguments [");
					for(int i=0; i< args.Length - 1; i++) 
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length-1].ToString());
					result.Append("].");
				}
				return result.ToString();
			}
		}
	}

	#endregion

	#region "Put Value class"

	public class PutValue
	{
		private byte[] desc;
		//
		// Set if the Putval instance was created from a byte array (to that byte array).
		//
		private byte[] sourceBytes;
    
		protected ByteArray descMark;
		private Stream stream;
		//
		// the following is used to reread data to recover from a timeout
		//
		protected ByteArray reqData = null;
		protected int reqLength;
		private int bufpos;

		public PutValue(Stream stream, int length, int bufpos)
		{
			if (length >= 0)
				stream = new StreamFilter(stream, length);
			else
				this.stream = stream;
			this.bufpos = bufpos;
		}
    
		public PutValue(byte[] bytes, int bufpos)
		{
			stream = new MemoryStream(bytes);
			sourceBytes = bytes;
			bufpos = bufpos;
		}
    
		protected PutValue(int bufpos)
		{
			bufpos = bufpos;
			stream = null;
		}

		public int BufPos
		{
			get
			{
				return bufpos;
			}
		}

		public bool atEnd
		{
			get
			{
				return stream == null;
			}
		}

		public void writeDescriptor(DataPart mem, int pos)
		{
			if (desc == null) 
				desc = new byte[LongDesc.Size];
        
			descMark = mem.writeDescriptor(pos, desc);
		}

		public void setDescriptor(byte[] desc)
		{
			this.desc = desc;
		}

		public void transferStream(DataPart dataPart)
		{
			if (atEnd)
				dataPart.markEmptyStream (descMark);
			else 
				if (dataPart.fillWithStream(stream, descMark, this)) 
				{
					try 
					{
						stream.Close();
					}
					catch 
					{
						// ignore
					}
					stream = null;
				}
		}

		public void transferStream(DataPart dataPart, short streamIndex)
		{
			transferStream(dataPart);
			descMark.writeInt16(streamIndex, LongDesc.Valind);
		}

		public void markAsLast(DataPart dataPart)
		{
			// avoid putting it in if this would break the aligned boundary.
			if(dataPart.Length - dataPart.Extent - 8 - LongDesc.Size - 1 < 0) 
				throw new IndexOutOfRangeException();
        
			int descriptorPos = dataPart.Extent;
			writeDescriptor(dataPart, descriptorPos);
			dataPart.addArg(descriptorPos, LongDesc.Size + 1);
			descMark.writeByte(LongDesc.LastPutval, LongDesc.Valmode);
		}

		public void markRequestedChunk(ByteArray reqData, int reqLength)
		{
			this.reqData = reqData;
			this.reqLength = reqLength;
		}

		public void reset ()
		{
			if (reqData != null) 			
			{
				byte[] data = reqData.readBytes(0, reqLength);
				Stream firstChunk = new MemoryStream(data);
				if (stream == null) 
					stream = firstChunk;
				else 
					stream = new JoinStream (new Stream[]{firstChunk, stream});
			}
			reqData = null;
		}
	}

	#endregion
}
