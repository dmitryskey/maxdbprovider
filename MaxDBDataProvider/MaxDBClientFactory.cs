#if NET20

using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace MaxDB.Data
{
	class MaxDBClientFactory : DbProviderFactory
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
