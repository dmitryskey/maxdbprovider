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

#if NET20

using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace MaxDB.Data
{
	class MaxDBFactory : DbProviderFactory
	{
        public override DbCommand CreateCommand()
        {
            return new MaxDBCommand();
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new MaxDBCommandBuilder();
        }

        public override DbConnection CreateConnection()
        {
            return new MaxDBConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new MaxDBConnectionStringBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new MaxDBDataAdapter();
        }

        public override DbParameter CreateParameter()
        {
            return new MaxDBParameter();
        }
	}
}

#endif // NET20
