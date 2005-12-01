using System;
using System.IO;

namespace MaxDBDataProvider.MaxDBProtocol
{
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
