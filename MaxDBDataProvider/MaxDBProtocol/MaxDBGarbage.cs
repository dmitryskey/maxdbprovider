using System;
using System.Collections;
using System.Runtime.CompilerServices;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if NATIVE

	public class GarbageParseid 
	{
		protected int canTreshold = 20;
		protected bool objPending = false;
		protected bool currentEmptyRun = false;
		protected bool currentEmptyRun2 = false;
		private ArrayList m_garbage;
		private bool supportsMultipleDropParseIDs;

		public GarbageParseid(bool asupportsMultipleDropParseIDs) : base() 
		{
			supportsMultipleDropParseIDs = asupportsMultipleDropParseIDs;
			m_garbage = new ArrayList(canTreshold);
		}

		public bool isPending 
		{
			get
			{
				objPending = (GarbageSize >= canTreshold);
				return objPending;
			}
		}

		public void forceGarbageCollection()
		{
			objPending = true;
		}

		public void emptyCan(MaxDBConnection conn) 
		{
			if(currentEmptyRun)
				return;
			currentEmptyRun=true;

			MaxDBRequestPacket requestPacket;
			objPending = false;
			while(GarbageSize > 0) 
			{
				try 
				{
					requestPacket = conn.GetRequestPacket();
					requestPacket.Init(short.MaxValue);
					emptyCan(requestPacket);
					conn.Exec(requestPacket, this, GCMode.GC_NONE);
				} 
				catch  
				{ 
					/* ignore */
				}
			}
			currentEmptyRun = false;
		}


		public void throwIntoGarbageCan (object obj)
		{
			m_garbage.Add(obj);
		}

		protected int GarbageSize
		{
			get
			{
				return m_garbage.Count;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public bool emptyCan(MaxDBRequestPacket requestPacket)
		{
			if (currentEmptyRun2)
				return false;
			
			currentEmptyRun2 = true;

			bool packetActionFailed = false;
			int sz = GarbageSize;

			if (!supportsMultipleDropParseIDs)
				while(sz > 0 && !packetActionFailed) 
				{
						object obj = m_garbage[sz - 1];
						m_garbage.RemoveAt(sz - 1);
						packetActionFailed = !requestPacket.dropPid((byte[])obj, false);
						if(packetActionFailed) 
							m_garbage.Add(obj);
						sz--;
				}
			else 
			{
				if (sz > 0)
				{
					object obj = m_garbage[sz - 1];
					m_garbage.RemoveAt(sz - 1);
					packetActionFailed = !requestPacket.dropPid((byte[])obj, false);
					if(packetActionFailed) 
						m_garbage.Add(obj);
					else 
					{
						sz--;
						while(sz>0 && !packetActionFailed) 
						{
							obj = m_garbage[sz - 1];
							m_garbage.RemoveAt(sz - 1);
							packetActionFailed = !requestPacket.dropPidAddtoParsidPart((byte[])obj);
							if(packetActionFailed) 
								m_garbage.Add(obj);
							sz--;
						}
					}
				}
			}

			currentEmptyRun2 = false;
			return !packetActionFailed;
		}

		public void emptyCan() 
		{
			m_garbage.Clear();
		}
	}
#endif
}
