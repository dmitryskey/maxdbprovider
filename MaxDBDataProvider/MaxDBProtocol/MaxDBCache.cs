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

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE

	#region "Least-Recently-Used cache class"

	/// <summary>
	/// Least-Recently-Used cache class
	/// </summary>
	public class LeastRecentlyUsedCache
	{
		/// <summary>
		/// double list class for internal purpuses
		/// </summary>
		private class DoubleList
		{
			private DoubleList mPrevLink, mNextLink;
			private object objData;

			/// <summary>
			/// class costructor
			/// </summary>
			/// <param name="data">list element</param>
			public DoubleList(object data)
			{
				objData = data;
			}

			/// <summary>
			/// property to get list element
			/// </summary>
			public object Data
			{
				get
				{
					return objData;
				}
			}

			public DoubleList Prev
			{
				get
				{
					return mPrevLink;
				}
			}

			public void Remove()
			{
				if (mPrevLink != null) 
					mPrevLink.mNextLink = mNextLink;

				if (mNextLink != null) 
					mNextLink.mPrevLink = mPrevLink;
			
				mPrevLink = null;
				mNextLink = null;
			}

			public void Prepend(DoubleList newHead)
			{
				newHead.mNextLink = this;
				mPrevLink = newHead;
			}
		}

		private class Association : DoubleList
		{
			object objKey;

			public Association(object key, object val) : base(val)
			{
				objKey = key;
			}

			public object Key
			{
				get
				{
					return objKey;
				}
			}
		}

		private Hashtable lookup;
		private Association lruTop;
		private Association lruBottom;
		private int currentSize;
		private int maxSize;
    
		public LeastRecentlyUsedCache(int cacheSize)
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
			currentSize--;
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

	internal class CacheInfo
	{
		private string strName;
		private int iHits;
		private int iMisses;
		
		public CacheInfo(string name)
		{
			strName = name;
		}

		public override string ToString()
		{
			return strName + ": " + iHits + " hits, " + iMisses + " misses, " + Hitrate + "%";
		}

		public void AddHit()
		{
			iHits++;
		}
		
		public void AddMiss()
		{
			iMisses++;
		}

		private double Hitrate
		{
			get
			{
				long all = iHits + iMisses;
				return (double) iHits / (double) all * 100.0;
			}
		}
	}

	#endregion

	#region "Parse information class"

	internal class ParseInfoCache : LeastRecentlyUsedCache
	{
		const int iDefaultSize = 1000;
		private const int iMaxFunctionCode = FunctionCode.Delete + 1;
		private bool bKeepStats;
		private bool[] bKindFilter;
		private CacheInfo[] ciStats;

		public ParseInfoCache(string cache, int cacheSize) : base(cacheSize > 0 ? cacheSize : iDefaultSize)
		{
            string kindDecl = cache;

            bKindFilter = new bool[iMaxFunctionCode];
            if (kindDecl.IndexOf('?') >= 0)
            {
                bKeepStats = true;
                ciStats = new CacheInfo[iMaxFunctionCode];
                ciStats[FunctionCode.Nil] = new CacheInfo("other");
                ciStats[FunctionCode.Insert] = new CacheInfo("insert");
                ciStats[FunctionCode.Select] = new CacheInfo("select");
                ciStats[FunctionCode.Update] = new CacheInfo("update");
                ciStats[FunctionCode.Delete] = new CacheInfo("delete");
            }

            if (kindDecl.IndexOf("all") >= 0)
                for (int i = 0; i < iMaxFunctionCode; ++i)
                    bKindFilter[i] = true;
            else
            {
                if (kindDecl.IndexOf("i") >= 0)
                    bKindFilter[FunctionCode.Insert] = true;
                if (kindDecl.IndexOf("u") >= 0)
                    bKindFilter[FunctionCode.Update] = true;
                if (kindDecl.IndexOf("d") >= 0)
                    bKindFilter[FunctionCode.Delete] = true;
                if (kindDecl.IndexOf("s") >= 0)
                    bKindFilter[FunctionCode.Select] = true;
            }
		}

		public MaxDBParseInfo FindParseInfo(string sqlCmd)
		{
			MaxDBParseInfo result = null;

			result = (MaxDBParseInfo)this[sqlCmd];
			if (bKeepStats && result != null) 
				ciStats[result.FuncCode].AddHit();

			return result;
		}
		/**
		 *
		 */
		public void AddParseInfo(MaxDBParseInfo parseinfo)
		{
			int functionCode = MapFunctionCode(parseinfo.FuncCode);
			if (bKindFilter[functionCode]) 
			{
				this[parseinfo.strSqlCmd] = parseinfo;
				parseinfo.IsCached = true;
				if (bKeepStats) 
					ciStats[functionCode].AddMiss();
			}
		}

		static private int MapFunctionCode(int functionCode)
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
	}

	#endregion

#endif // SAFE
}


