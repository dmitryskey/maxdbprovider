//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBCache.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.MaxDBProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using MaxDB.Data.Interfaces.MaxDBProtocol;

    /// <summary>
    /// MaxDB Cache.
    /// </summary>
    internal class MaxDBCache
    {
        /// <summary>
        /// Least-Recently-Used cache class.
        /// </summary>
        internal class LeastRecentlyUsedCache
        {
            /// <summary>
            /// Cache maximum size.
            /// </summary>
            private readonly int maxSize;

            /// <summary>
            /// Lookup hash table.
            /// </summary>
            private Dictionary<string, Association> lookup;

            /// <summary>
            /// Top cache element.
            /// </summary>
            private Association lruTop;

            /// <summary>
            /// Bottom cache element.
            /// </summary>
            private Association lruBottom;

            /// <summary>
            /// Current cache size.
            /// </summary>
            private int currentSize;

            /// <summary>
            /// Initializes a new instance of the <see cref="LeastRecentlyUsedCache"/> class.
            /// </summary>
            /// <param name="cacheSize">Cache size.</param>
            public LeastRecentlyUsedCache(int cacheSize)
            {
                this.maxSize = cacheSize;
                this.Clear();
            }

            /// <summary>
            /// Gets or sets cached object.
            /// </summary>
            /// <param name="key">Key value.</param>
            /// <returns>Cached object.</returns>
            public object this[string key]
            {
                get
                {
                    object result = null;

                    var entry = this.lookup[key];
                    if (entry != null)
                    {
                        result = entry.Data;
                        this.MoveToTop(entry);
                    }

                    return result;
                }

                set
                {
                    var newEntry = new Association(key, value);

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

            /// <summary>
            /// Clear cache.
            /// </summary>
            public void Clear()
            {
                this.currentSize = 0;
                this.lookup = new Dictionary<string, Association>(this.maxSize);
                this.lruTop = null;
                this.lruBottom = null;
            }

            /// <summary>
            /// Clear cache and return all content.
            /// </summary>
            /// <returns>Object array.</returns>
            public Association[] ClearAll()
            {
                var result = this.lookup.Values.ToArray();
                this.Clear();
                return result;
            }

            /// <summary>
            /// Delete last cache entry.
            /// </summary>
            private void RemoveLast()
            {
                var toDelete = this.lruBottom;

                this.lruBottom = (Association)toDelete.Prev;
                this.lookup.Remove(toDelete.Key);
                this.currentSize--;
            }

            /// <summary>
            /// Move cache entry to the top of list.
            /// </summary>
            /// <param name="entry">Cache entry.</param>
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

            /// <summary>
            /// Cache element class with links.
            /// </summary>
            internal class Association : DoubleList
            {
                /// <summary>
                /// Initializes a new instance of the <see cref="Association"/> class.
                /// </summary>
                /// <param name="key">Key value.</param>
                /// <param name="val">Element value.</param>
                public Association(string key, object val)
                    : base(val) => this.Key = key;

                /// <summary>
                /// Gets the key value.
                /// </summary>
                public string Key { get; }
            }

            /// <summary>
            /// double list class for internal purposes.
            /// </summary>
            internal class DoubleList
            {
                /// <summary>
                /// Previous and next links.
                /// </summary>
                private DoubleList nextLink;

                /// <summary>
                /// Initializes a new instance of the <see cref="DoubleList"/> class.
                /// </summary>
                /// <param name="data">List element.</param>
                protected DoubleList(object data)
                {
                    this.Data = data;
                }

                /// <summary>
                /// Gets a list element.
                /// </summary>
                public object Data { get; }

                /// <summary>
                /// Gets the previous element.
                /// </summary>
                public DoubleList Prev { get; private set; }

                /// <summary>
                /// Remove the element from the list.
                /// </summary>
                public void Remove()
                {
                    if (this.Prev != null)
                    {
                        this.Prev.nextLink = this.nextLink;
                    }

                    if (this.nextLink != null)
                    {
                        this.nextLink.Prev = this.Prev;
                    }

                    this.Prev = null;
                    this.nextLink = null;
                }

                /// <summary>
                /// Add element to the list.
                /// </summary>
                /// <param name="newHead">New element.</param>
                public void Prepend(DoubleList newHead)
                {
                    newHead.nextLink = this;
                    this.Prev = newHead;
                }
            }
        }

        /// <summary>
        /// Cache information class.
        /// </summary>
        internal class CacheInfo
        {
            /// <summary>
            /// Cache element name.
            /// </summary>
            private readonly string strName;

            /// <summary>
            /// Hists counter.
            /// </summary>
            private int hits;

            /// <summary>
            /// Missed counter.
            /// </summary>
            private int misses;

            /// <summary>
            /// Initializes a new instance of the <see cref="CacheInfo"/> class.
            /// </summary>
            /// <param name="name">Cache element name.</param>
            public CacheInfo(string name) => this.strName = name;

            /// <summary>
            /// Gets the hit rate.
            /// </summary>
            private double Hitrate => this.hits / (this.hits + this.misses) * 100.0;

            /// <summary>
            /// String representation of the current cache info.
            /// </summary>
            /// <returns>String value.</returns>
            public override string ToString() => $"{this.strName}: {this.hits} hits, {this.misses} misses, {this.Hitrate}%";

            /// <summary>
            /// Increase cache hit counter.
            /// </summary>
            public void AddHit() => this.hits++;

            /// <summary>
            /// Increase cache missing counter.
            /// </summary>
            public void AddMiss() => this.misses++;
        }

        /// <summary>
        /// Parse information class.
        /// </summary>
        internal class ParseInfoCache : LeastRecentlyUsedCache, IParseInfoCache
        {
            /// <summary>
            /// Default parse info cache size.
            /// </summary>
            private const int IDefaultSize = 1000;

            /// <summary>
            /// Maximum function code.
            /// </summary>
            private const int IMaxFunctionCode = FunctionCode.Delete + 1;

            /// <summary>
            /// The flag indicating whether we keep statistic or not.
            /// </summary>
            private readonly bool keepStats;

            /// <summary>
            /// List of boolean values describing what kind of command is cached.
            /// </summary>
            private readonly bool[] kindFilter;

            /// <summary>
            /// Cache info statistic array.
            /// </summary>
            private readonly CacheInfo[] stats;

            /// <summary>
            /// Initializes a new instance of the <see cref="ParseInfoCache"/> class.
            /// </summary>
            /// <param name="cache">Cache name.</param>
            /// <param name="cacheSize">Cache size.</param>
            public ParseInfoCache(string cache, int cacheSize)
                : base(cacheSize > 0 ? cacheSize : IDefaultSize)
            {
                string kindDecl = cache;

                this.kindFilter = new bool[IMaxFunctionCode];
                if (kindDecl.IndexOf('?') >= 0)
                {
                    this.keepStats = true;
                    this.stats = new CacheInfo[IMaxFunctionCode];
                    this.stats[FunctionCode.Nil] = new CacheInfo("other");
                    this.stats[FunctionCode.Insert] = new CacheInfo("insert");
                    this.stats[FunctionCode.Select] = new CacheInfo("select");
                    this.stats[FunctionCode.Update] = new CacheInfo("update");
                    this.stats[FunctionCode.Delete] = new CacheInfo("delete");
                }

                if (kindDecl.IndexOf("all", StringComparison.InvariantCulture) >= 0)
                {
                    for (int i = 0; i < IMaxFunctionCode; ++i)
                    {
                        this.kindFilter[i] = true;
                    }
                }
                else
                {
                    if (kindDecl.IndexOf("i", StringComparison.InvariantCulture) >= 0)
                    {
                        this.kindFilter[FunctionCode.Insert] = true;
                    }

                    if (kindDecl.IndexOf("u", StringComparison.InvariantCulture) >= 0)
                    {
                        this.kindFilter[FunctionCode.Update] = true;
                    }

                    if (kindDecl.IndexOf("d", StringComparison.InvariantCulture) >= 0)
                    {
                        this.kindFilter[FunctionCode.Delete] = true;
                    }

                    if (kindDecl.IndexOf("s", StringComparison.InvariantCulture) >= 0)
                    {
                        this.kindFilter[FunctionCode.Select] = true;
                    }
                }
            }

            /// <summary>
            /// Find parse info for SQL command.
            /// </summary>
            /// <param name="sqlCmd">SQL command.</param>
            /// <returns>Parse info.</returns>
            public IMaxDBParseInfo FindParseInfo(string sqlCmd)
            {
                var result = (IMaxDBParseInfo)this[sqlCmd];
                if (this.keepStats && result != null)
                {
                    this.stats[result.FuncCode].AddHit();
                }

                return result;
            }

            /// <summary>
            /// Add parse info to the cache.
            /// </summary>
            /// <param name="parseinfo">Parse infor.</param>
            public void AddParseInfo(IMaxDBParseInfo parseinfo)
            {
                int functionCode = MapFunctionCode(parseinfo.FuncCode);
                if (this.kindFilter[functionCode])
                {
                    this[parseinfo.SqlCommand] = parseinfo;
                    parseinfo.IsCached = true;
                    if (this.keepStats)
                    {
                        this.stats[functionCode].AddMiss();
                    }
                }
            }

            /// <summary>
            /// Replace all function codes except for Insert/Update/Delete/Select by null.
            /// </summary>
            /// <param name="functionCode">Input function code.</param>
            /// <returns>Mapped function code.</returns>
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
    }
}
