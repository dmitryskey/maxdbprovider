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
using System.Collections;

namespace MaxDBDataProvider.MaxDBProtocol
{
#if SAFE

	#region "Least-Recently-Used cache class"

	/// <summary>
	/// Least-Recently-Used cache class
	/// </summary>
	public class LRUCache
	{
		/// <summary>
		/// double list class for internal purpuses
		/// </summary>
		private class DoubleList
		{
			private DoubleList m_prevLink, m_nextLink;
			private object m_data;

			/// <summary>
			/// class costructor
			/// </summary>
			/// <param name="data">list element</param>
			public DoubleList(object data)
			{
				m_data = data;
			}

			/// <summary>
			/// property to get list element
			/// </summary>
			public object Data
			{
				get
				{
					return m_data;
				}
			}

			public DoubleList Next
			{
				get
				{
					return m_nextLink;
				}
			}

			public DoubleList Prev
			{
				get
				{
					return m_prevLink;
				}
			}

			public bool	AtStart
			{
				get
				{
					return m_prevLink == null;
				}
			}

			public bool AtEnd
			{
				get
				{
					return m_nextLink == null;
				}
			}

			public void Remove()
			{
				if (m_prevLink != null) 
					m_prevLink.m_nextLink = m_nextLink;

				if (m_nextLink != null) 
					m_nextLink.m_prevLink = m_prevLink;
			
				m_prevLink = null;
				m_nextLink = null;
			}

			public void Prepend(DoubleList newHead)
			{
				newHead.m_nextLink = this;
				m_prevLink = newHead;
			}

			public void Append(DoubleList newTail)
			{
				m_nextLink = newTail;
				newTail.m_prevLink = this;
			}

			public void InsertAfter(DoubleList newHead)
			{
				DoubleList newTail = newHead.m_nextLink;
				newHead.m_nextLink = this;
				m_prevLink = newHead;
				m_nextLink = newTail;
				if (newTail != null) 
					newTail.m_prevLink = this;
			}
		}

		private class Association : DoubleList
		{
			object m_key;

			public Association(object key, object val) : base(val)
			{
				m_key = key;
			}

			public object Key
			{
				get
				{
					return m_key;
				}
			}
		}

		private Hashtable lookup;
		private Association lruTop;
		private Association lruBottom;
		private int currentSize;
		private int maxSize;
    
		public LRUCache(int cacheSize)
		{
			maxSize = cacheSize;
			Clear();
		}

		public void Clear()
		{
			currentSize = 0;
			lookup = new Hashtable(this.maxSize);
			lruTop = null;
			lruBottom = null;
		}

		public object this[object key]
		{
			get
			{
				object result = null;
				Association entry;

				entry = (Association)lookup[key];
				if (entry != null) 
				{
					result = entry.Data;
					MoveToTop(entry);
				}
				return result;
			}
			set
			{
				Association newEntry = new Association(key, value);

				lookup[key] = newEntry;
				if (lruTop != null) 
					lruTop.Prepend(newEntry);
			
				lruTop = newEntry;
				if (lruBottom == null) 
					lruBottom = newEntry;
			
				currentSize++;
				if (currentSize > maxSize) 
					RemoveLast ();
			}
		}

		private void MoveToTop(Association entry)
		{
			if (entry == lruTop) 
				return;

			if (entry == lruBottom) 
				lruBottom = (Association)entry.Prev;
			entry.Remove();
			lruTop.Prepend(entry);
			lruTop = entry;
		}

		private void RemoveLast()
		{
			Association toDelete = lruBottom;

			lruBottom = (Association) toDelete.Prev;
			lookup.Remove(toDelete.Key);
			RemoveHook (toDelete);
			currentSize--;
		}

		protected void RemoveHook(object val)
		{
		}

		public object[] ClearAll()
		{
			object[] result= new object[lookup.Count];
			lookup.Values.CopyTo(result, 0);
			Clear();
			return result;
		}
	}

	#endregion

	#region "Cache information class"

	public class CacheInfo
	{
		private string name;
		private long hits;
		private long misses;
		
		public CacheInfo(string name)
		{
			this.name = name;
			this.hits = 0;
			this.misses = 0;
		}

		public override string ToString()
		{
			return name + ": " + hits + " hits, " + misses + " misses, " + Hitrate + "%";
		}

		public void addHit()
		{
			hits++;
		}
		
		public void addMiss()
		{
			misses++;
		}

		public double Hitrate
		{
			get
			{
				long all = hits + misses;
				return (double) hits / (double) all * 100.0;
			}
		}

		public static CacheInfo Cummulate(CacheInfo[] stats)
		{
			CacheInfo result = new CacheInfo("all");

			for (int i = 0; i < stats.Length; i++) 
			{
				if (stats[i] != null) 
				{
					result.hits += stats[i].hits;
					result.misses += stats[i].misses;
				}
			}
			return result;
		}
	}

	#endregion

	#region "Parse information class"

	internal class ParseInfoCache : LRUCache
	{
		const int defaultSize = 1000;
		private const int maxFunctionCode = FunctionCode.Delete + 1;
		private bool keepStats;
		private bool[] kindFilter;
		private CacheInfo[] stats;

		public ParseInfoCache(string cache, int cacheSize) : base((cacheSize > 0)? cacheSize : defaultSize)
		{
			setOptions(cache);
		}

		private void setOptions(string cache)
		{
			string kindDecl = cache;

			kindFilter = new bool[maxFunctionCode];
			if (kindDecl.IndexOf('?') >= 0) 
				initStats();
	
			if (kindDecl.IndexOf("all") >= 0) 
				for (int i = 0; i < maxFunctionCode; ++i) 
					kindFilter[i] = true;
			else 
			{
				if (kindDecl.IndexOf("i") >= 0) 
					kindFilter[FunctionCode.Insert] = true;
				if (kindDecl.IndexOf("u") >= 0) 
					kindFilter [FunctionCode.Update] = true;
				if (kindDecl.IndexOf("d") >= 0) 
					kindFilter[FunctionCode.Delete] = true;
				if (kindDecl.IndexOf ("s") >= 0) 
					kindFilter[FunctionCode.Select] = true;
			}
		}

		private void initStats()
		{
			keepStats = true;
			stats = new CacheInfo[maxFunctionCode];
			stats[FunctionCode.Nil] = new CacheInfo("other");
			stats[FunctionCode.Insert] = new CacheInfo("insert");
			stats[FunctionCode.Select] = new CacheInfo("select");
			stats[FunctionCode.Update] = new CacheInfo("update");
			stats[FunctionCode.Delete] = new CacheInfo("delete");
		}

		public MaxDBParseInfo FindParseInfo(string sqlCmd)
		{
			MaxDBParseInfo result = null;

			result = (MaxDBParseInfo)this[sqlCmd];
			if (keepStats && result != null) 
				stats[result.FuncCode].addHit();

			return result;
		}
		/**
		 *
		 */
		public void addParseinfo(MaxDBParseInfo parseinfo)
		{
			int functionCode = mapFunctionCode(parseinfo.FuncCode);
			if (kindFilter[functionCode]) 
			{
				this[parseinfo.m_sqlCmd] = parseinfo;
				parseinfo.IsCached = true;
				if (keepStats) 
					stats[functionCode].addMiss();
			}
		}

		static private int mapFunctionCode(int functionCode)
		{
			switch (functionCode) 
			{
				case FunctionCode.Insert:
				case FunctionCode.Update:
				case FunctionCode.Delete:
				case FunctionCode.Select:
					// keep the value
					break;
				default:
					functionCode = FunctionCode.Nil;
					break;
			}
			return functionCode;
		}

		public CacheInfo[] Stats
		{
			get
			{
				if (!keepStats) 
					return null;

				CacheInfo[] result = new CacheInfo [6];

				result[0] = stats[FunctionCode.Nil];
				result[1] = stats[FunctionCode.Insert];
				result[2] = stats[FunctionCode.Update];
				result[3] = stats[FunctionCode.Delete];
				result[4] = stats[FunctionCode.Select];
				result[5] = CacheInfo.Cummulate(result);
				return result;
			}
		}
	}

	#endregion
#endif
}


