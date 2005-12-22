using System;
using System.Collections;
using System.Runtime.CompilerServices;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if NATIVE

	#region "GarbageCan Class"

	/// <summary>
	/// Summary description for MaxDBGarbage.
	/// </summary>
	public abstract class GarbageCan 
	{
		protected int canTreshold = 20;
		protected bool objPending = false;
		protected bool currentEmptyRun = false;
		protected bool currentEmptyRun2 = false;
		
		public GarbageCan() : this(20)
		{
		}

		public GarbageCan (int aTreshhold) 
		{
			canTreshold = aTreshhold;
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
					requestPacket = conn.CreateRequestPacket();
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

		public abstract void emptyCan();
		public abstract void throwIntoGarbageCan(object obj);
		public abstract bool emptyCan(MaxDBRequestPacket requestPacket);
		protected abstract int GarbageSize{get;}
	}
	#endregion

	public class GarbageParseid : GarbageCan 
	{
		private ArrayList m_garbage;
		private bool supportsMultipleDropParseIDs;

		public GarbageParseid(bool asupportsMultipleDropParseIDs) : base() 
		{
			supportsMultipleDropParseIDs = asupportsMultipleDropParseIDs;
			m_garbage = new ArrayList(canTreshold);
		}

		public override void throwIntoGarbageCan (object obj)
		{
			m_garbage.Add(obj);
		}

		protected override int GarbageSize
		{
			get
			{
				return m_garbage.Count;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public override bool emptyCan(MaxDBRequestPacket requestPacket)
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

		public override void emptyCan () 
		{
			m_garbage.Clear();
		}
	}

	public class GarbageCursor : GarbageCan 
	{
		private ArrayList m_garbage;

		public GarbageCursor() : base(1)
		{
			m_garbage = new ArrayList();
		}

		public override void throwIntoGarbageCan (object obj) 
		{
			m_garbage.Add(obj);
		}

		protected override int GarbageSize
		{
			get
			{
				return m_garbage.Count;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public override bool emptyCan(MaxDBRequestPacket requestPacket)
		{
			if(currentEmptyRun2)
				return false;
			currentEmptyRun2=true;

			bool packetActionFailed = false;
			while(m_garbage.Count > 0 && !packetActionFailed) 
			{
				object obj = m_garbage[0];
				m_garbage.Remove(obj);
				packetActionFailed = !packetAction(requestPacket, obj);
			
				if(packetActionFailed) 
					m_garbage.Add(obj);
			}

			currentEmptyRun2 = false;
			return !packetActionFailed;
		}

		public override void emptyCan () 
		{
			m_garbage.Clear();
		}

		public bool packetAction(MaxDBRequestPacket requestPacket, object obj)
		{
			return requestPacket.initDbsCommand("CLOSE \"" + ((string)obj) + "\"", false, false);
		}

		public bool restoreFromGarbageCan(object obj)
		{
			if (m_garbage.Contains(obj))
			{
				m_garbage.Remove(obj);
				return true;
			}
			else
				return false;
		}
	}
#endif
}
