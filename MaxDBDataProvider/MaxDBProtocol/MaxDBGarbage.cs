using System;
using System.Collections;
using System.Runtime.CompilerServices;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE

	public class GarbageParseid 
	{
		protected int m_canTreshold = 20;
		protected bool m_objPending = false;
		protected bool m_currentEmptyRun = false;
		protected bool m_currentEmptyRun2 = false;
		private ArrayList m_garbage;
		private bool m_supportsMultipleDropParseIDs;

		public GarbageParseid(bool asupportsMultipleDropParseIDs) : base() 
		{
			m_supportsMultipleDropParseIDs = asupportsMultipleDropParseIDs;
			m_garbage = new ArrayList(m_canTreshold);
		}

		public bool IsPending 
		{
			get
			{
				m_objPending = (GarbageSize >= m_canTreshold);
				return m_objPending;
			}
		}

		public void ForceGarbageCollection()
		{
			m_objPending = true;
		}

		public void EmptyCan(MaxDBConnection conn) 
		{
			if(m_currentEmptyRun)
				return;
			m_currentEmptyRun=true;

			MaxDBRequestPacket requestPacket;
			m_objPending = false;
			while(GarbageSize > 0) 
			{
				try 
				{
					requestPacket = conn.GetRequestPacket();
					requestPacket.Init(short.MaxValue);
					EmptyCan(requestPacket);
					conn.Execute(requestPacket, this, GCMode.GC_NONE);
				} 
				catch  
				{ 
					/* ignore */
				}
			}
			m_currentEmptyRun = false;
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
		public bool EmptyCan(MaxDBRequestPacket requestPacket)
		{
			if (m_currentEmptyRun2)
				return false;
			
			m_currentEmptyRun2 = true;

			bool packetActionFailed = false;
			int sz = GarbageSize;

			if (!m_supportsMultipleDropParseIDs)
				while(sz > 0 && !packetActionFailed) 
				{
						object obj = m_garbage[sz - 1];
						m_garbage.RemoveAt(sz - 1);
						packetActionFailed = !requestPacket.DropPid((byte[])obj, false);
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
					packetActionFailed = !requestPacket.DropPid((byte[])obj, false);
					if(packetActionFailed) 
						m_garbage.Add(obj);
					else 
					{
						sz--;
						while(sz>0 && !packetActionFailed) 
						{
							obj = m_garbage[sz - 1];
							m_garbage.RemoveAt(sz - 1);
							packetActionFailed = !requestPacket.DropPidAddtoParsidPart((byte[])obj);
							if(packetActionFailed) 
								m_garbage.Add(obj);
							sz--;
						}
					}
				}
			}

			m_currentEmptyRun2 = false;
			return !packetActionFailed;
		}

		public void emptyCan() 
		{
			m_garbage.Clear();
		}
	}
#endif
}
