//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBCommand.cs" company="Dmitry S. Kataev">
//     Copyright (C) 2005-2011 Dmitry S. Kataev
//     Copyright (C) 2002-2003 SAP AG
// </copyright>
//-----------------------------------------------------------------------------------------------
//
//  This program is free software; you can redistribute it and/or
//  modify it under the terms of the GNU General Public License
//  as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.MaxDBProtocol
{
#if SAFE
    using System.Collections;

    #region "Least-Recently-Used cache class"

    /// <summary>
    /// Least-Recently-Used cache class
    /// </summary>
    internal class LeastRecentlyUsedCache
    {
        private Hashtable lookup;
        private Association lruTop;
        private Association lruBottom;
        private int currentSize;
        private int maxSize;

        public LeastRecentlyUsedCache(int cacheSize)
        {
            this.maxSize = cacheSize;
            this.Clear();
        }

        public object this[object key]
        {
            get
            {
                object result = null;

                Association entry = (Association)this.lookup[key];
                if (entry != null)
                {
                    result = entry.Data;
                    this.MoveToTop(entry);
                }

                return result;
            }

            set
            {
                Association newEntry = new Association(key, value);

                this.lookup[key] = newEntry;
                if (this.lruTop != null)
                {
                    this.lruTop.Prepend(newEntry);
                }

                this.lruTop = newEntry;
                if (this.lruBottom == null)
                {
                    this.lruBottom = newEntry;
                }

                this.currentSize++;
                if (this.currentSize > this.maxSize)
                {
                    this.RemoveLast();
                }
            }
        }

        public void Clear()
        {
            this.currentSize = 0;
            this.lookup = new Hashtable(this.maxSize);
            this.lruTop = null;
            this.lruBottom = null;
        }

        public object[] ClearAll()
        {
            object[] result = new object[this.lookup.Count];
            this.lookup.Values.CopyTo(result, 0);
            this.Clear();
            return result;
        }

        private void RemoveLast()
        {
            Association toDelete = this.lruBottom;

            this.lruBottom = (Association)toDelete.Prev;
            this.lookup.Remove(toDelete.Key);
            this.currentSize--;
        }

        private void MoveToTop(Association entry)
        {
            if (entry == this.lruTop)
            {
                return;
            }

            if (entry == this.lruBottom)
            {
                this.lruBottom = (Association)entry.Prev;
            }

            entry.Remove();
            this.lruTop.Prepend(entry);
            this.lruTop = entry;
        }

        private class Association : DoubleList
        {
            readonly private object objKey;

            public Association(object key, object val)
                : base(val)
            {
                this.objKey = key;
            }

            public object Key
            {
                get
                {
                    return this.objKey;
                }
            }
        }

        /// <summary>
        /// double list class for internal purposes
        /// </summary>
        private class DoubleList
        {
            private DoubleList mPrevLink, mNextLink;
            private object objData;

            /// <summary>
            /// class costructor
            /// </summary>
            /// <param name="data">list element</param>
            protected DoubleList(object data)
            {
                this.objData = data;
            }

            /// <summary>
            /// Gets a list element
            /// </summary>
            public object Data
            {
                get
                {
                    return this.objData;
                }
            }

            public DoubleList Prev
            {
                get
                {
                    return this.mPrevLink;
                }
            }

            public void Remove()
            {
                if (this.mPrevLink != null)
                {
                    this.mPrevLink.mNextLink = this.mNextLink;
                }

                if (this.mNextLink != null)
                {
                    this.mNextLink.mPrevLink = this.mPrevLink;
                }

                this.mPrevLink = null;
                this.mNextLink = null;
            }

            public void Prepend(DoubleList newHead)
            {
                newHead.mNextLink = this;
                this.mPrevLink = newHead;
            }
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
            this.strName = name;
        }

        private double Hitrate
        {
            get
            {
                long all = this.iHits + this.iMisses;
                return (double)this.iHits / (double)all * 100.0;
            }
        }

        public override string ToString()
        {
            return this.strName + ": " + this.iHits + " hits, " + this.iMisses + " misses, " + this.Hitrate + "%";
        }

        public void AddHit()
        {
            this.iHits++;
        }

        public void AddMiss()
        {
            this.iMisses++;
        }
    }

    #endregion

    #region "Parse information class"

    internal class ParseInfoCache : LeastRecentlyUsedCache
    {
        private const int IDefaultSize = 1000;
        private const int IMaxFunctionCode = FunctionCode.Delete + 1;
        private readonly bool bKeepStats;
        private readonly bool[] bKindFilter;
        private readonly CacheInfo[] ciStats;

        public ParseInfoCache(string cache, int cacheSize)
            : base(cacheSize > 0 ? cacheSize : IDefaultSize)
        {
            string kindDecl = cache;

            this.bKindFilter = new bool[IMaxFunctionCode];
            if (kindDecl.IndexOf('?') >= 0)
            {
                this.bKeepStats = true;
                this.ciStats = new CacheInfo[IMaxFunctionCode];
                this.ciStats[FunctionCode.Nil] = new CacheInfo("other");
                this.ciStats[FunctionCode.Insert] = new CacheInfo("insert");
                this.ciStats[FunctionCode.Select] = new CacheInfo("select");
                this.ciStats[FunctionCode.Update] = new CacheInfo("update");
                this.ciStats[FunctionCode.Delete] = new CacheInfo("delete");
            }

            if (kindDecl.IndexOf("all") >= 0)
            {
                for (int i = 0; i < IMaxFunctionCode; ++i)
                {
                    this.bKindFilter[i] = true;
                }
            }
            else
            {
                if (kindDecl.IndexOf("i") >= 0)
                {
                    this.bKindFilter[FunctionCode.Insert] = true;
                }

                if (kindDecl.IndexOf("u") >= 0)
                {
                    this.bKindFilter[FunctionCode.Update] = true;
                }

                if (kindDecl.IndexOf("d") >= 0)
                {
                    this.bKindFilter[FunctionCode.Delete] = true;
                }

                if (kindDecl.IndexOf("s") >= 0)
                {
                    this.bKindFilter[FunctionCode.Select] = true;
                }
            }
        }

        public MaxDBParseInfo FindParseInfo(string sqlCmd)
        {
            MaxDBParseInfo result = (MaxDBParseInfo)this[sqlCmd];
            if (this.bKeepStats && result != null)
            {
                this.ciStats[result.FuncCode].AddHit();
            }

            return result;
        }

        public void AddParseInfo(MaxDBParseInfo parseinfo)
        {
            int functionCode = MapFunctionCode(parseinfo.FuncCode);
            if (this.bKindFilter[functionCode])
            {
                this[parseinfo.SqlCommand] = parseinfo;
                parseinfo.IsCached = true;
                if (this.bKeepStats)
                {
                    this.ciStats[functionCode].AddMiss();
                }
            }
        }

        private static int MapFunctionCode(int functionCode)
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
