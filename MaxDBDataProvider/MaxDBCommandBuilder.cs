//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text;
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data
{
	/// <summary>
	/// Summary description for MaxDBCommandBuilder.
	/// </summary>
    [System.ComponentModel.DesignerCategory("Code")]
	public sealed class MaxDBCommandBuilder : 
#if NET20
        DbCommandBuilder
#else
        Component
#endif
	{
		private string strPrefix = "'";
		private string strSuffix = "'";
		private MaxDBDataAdapter mAdapter;
		private DataTable mSchema;
		private string strBaseTable;
		private MaxDBCommand cmdDelete;
		private MaxDBCommand cmdInsert;
		private MaxDBCommand cmdUpdate;

		public MaxDBCommandBuilder()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public MaxDBCommandBuilder(MaxDBDataAdapter adapter)
		{
			DataAdapter = adapter;
		}

#if NET20
        public override string QuotePrefix
#else
		public string QuotePrefix
#endif // NET20
		{
			get
			{
				return strPrefix;
			}
			set
			{
				strPrefix = value;
			}
		}

#if NET20
        public override string QuoteSuffix
#else
		public string QuoteSuffix
#endif // NET20
		{
			get
			{
				return strSuffix;
			}
			set
			{
				strSuffix = value;
			}
		}

#if NET20
        public new MaxDBDataAdapter DataAdapter
#else
		public MaxDBDataAdapter DataAdapter
#endif // NET20
		{
			get
			{
				return mAdapter;
			}
			set
			{
				if (mAdapter != null)
#if NET20
                    mAdapter.RowUpdating -= new EventHandler<MaxDBRowUpdatingEventArgs>(OnRowUpdating);
#else
					mAdapter.RowUpdating -= new MaxDBRowUpdatingEventHandler(OnRowUpdating);
#endif // NET20

				if (value == null)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADAPTER_NULL));
				mAdapter = value;
#if NET20
                mAdapter.RowUpdating += new EventHandler<MaxDBRowUpdatingEventArgs>(OnRowUpdating);
#else
				mAdapter.RowUpdating += new MaxDBRowUpdatingEventHandler(OnRowUpdating);
#endif // NET20
			}
		}

#if NET20
        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            DataAdapter = (MaxDBDataAdapter)adapter;
        }
#endif // NET20

#if NET20
        public override void RefreshSchema()
#else
		public void RefreshSchema()
#endif // NET20
		{
			if (mAdapter == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADAPTER_NULL));
			if (mAdapter.SelectCommand == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SELECT_NULL));
		
			MaxDBDataReader dr = mAdapter.SelectCommand.ExecuteReader(CommandBehavior.SchemaOnly);
			mSchema = dr.GetSchemaTable();

			if (mSchema.Rows.Count > 0 && !mSchema.Rows[0].IsNull("BaseTableName"))
				strBaseTable = mSchema.Rows[0]["BaseTableName"].ToString();
			else
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BASETABLE_NOTFOUND));
		}

#if NET20 
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
        }
#endif // NET20 

#if NET20
        protected override string GetParameterName(int parameterOrdinal)
#else
		private string GetParameterName(int parameterOrdinal)
#endif // NET20
        {
            if (parameterOrdinal < 0 || parameterOrdinal >= mSchema.Rows.Count)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLINDEX_NOTFOUND));

            return mSchema.Rows[parameterOrdinal]["ColumnName"].ToString();
        }

#if NET20
        protected override string GetParameterName(string parameterName)
        {
            foreach (DataRow row in mSchema.Rows)
                if (string.Compare(row["ColumnName"].ToString().Trim(), parameterName.Trim(), true, CultureInfo.InvariantCulture) == 0)
                    return parameterName;

            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAME_NOTFOUND));
        }
#endif // NET20

#if NET20
        protected override string GetParameterPlaceholder(int parameterOrdinal)
#else
		private string GetParameterPlaceholder(int parameterOrdinal)
#endif // NET20
        {
            return ":" + mSchema.Rows[parameterOrdinal]["ColumnName"].ToString();
        }

		private MaxDBCommand CreateCommand()
		{
			MaxDBCommand cmd = new MaxDBCommand(string.Empty, mAdapter.SelectCommand.Connection, mAdapter.SelectCommand.Transaction);
			cmd.CommandTimeout = mAdapter.SelectCommand.CommandTimeout;
			return cmd;
		}

		private MaxDBParameter CreateParameter(int index, DataRowVersion version)
		{
            DataRow row = mSchema.Rows[index];
			return new MaxDBParameter(
				GetParameterName(index),
				(MaxDBType)row["ProviderType"],
				(int)row["ColumnSize"],
				ParameterDirection.Input,
				(bool)row["AllowDBNull"],
				(byte)(int)row["NumericPrecision"],
				(byte)(int)row["NumericScale"],
				row["ColumnName"].ToString(),
				version,
				DBNull.Value);
		}

#if NET20
        public new MaxDBCommand GetDeleteCommand()
#else
		public MaxDBCommand GetDeleteCommand()
#endif // NET20
		{
			if (mSchema == null)
				RefreshSchema();

			if (cmdDelete != null) 
				return cmdDelete;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder whereStmt = new StringBuilder();
	
			for(int i = 0; i < mSchema.Rows.Count; i++)
			{
                DataRow row = mSchema.Rows[i];
				string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
				{
					if (whereStmt.Length > 0)
						whereStmt.Append(" AND ");

                    whereStmt.Append(columnName + "= " + GetParameterPlaceholder(i));

					cmd.Parameters.Add(CreateParameter(i, DataRowVersion.Original));
				}
			}

			cmd.CommandText = "DELETE FROM " + strBaseTable;
			if (whereStmt.Length > 0)
				cmd.CommandText += " WHERE " + whereStmt.ToString();

			cmdDelete = cmd;
			return cmdDelete;
		}

#if NET20
        public new MaxDBCommand GetInsertCommand()
#else
		public MaxDBCommand GetInsertCommand()
#endif // NET20
		{
			if (mSchema == null)
				RefreshSchema();

			if (cmdInsert != null)
				return cmdInsert;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder columns = new StringBuilder();
			StringBuilder markers = new StringBuilder();

            for (int i = 0; i < mSchema.Rows.Count; i++)
			{
                DataRow row = mSchema.Rows[i];
				string columnName = row["ColumnName"].ToString();
				if (!(bool)row["IsAutoIncrement"])
				{
					if (columns.Length > 0 && markers.Length > 0)
					{
						columns.Append(", ");
						markers.Append(", ");
					}

					columns.Append(columnName);
                    markers.Append(GetParameterPlaceholder(i));

					cmd.Parameters.Add(CreateParameter(i, DataRowVersion.Current));
				}
			}

			cmd.CommandType = CommandType.Text;
			cmd.CommandText = "INSERT INTO " + strBaseTable + "(" + columns.ToString() + ") VALUES(" + markers.ToString() + ")";

			cmdInsert = cmd;
			return cmdInsert;
		}

#if NET20
        public new MaxDBCommand GetUpdateCommand()
#else
		public MaxDBCommand GetUpdateCommand()
#endif // NET20
		{
			if (mSchema == null)
				RefreshSchema();

			if (cmdUpdate != null)
				return cmdUpdate;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder setStmt = new StringBuilder();
			MaxDBParameterCollection setParams = new MaxDBParameterCollection();
			StringBuilder whereStmt = new StringBuilder();
			MaxDBParameterCollection whereParams = new MaxDBParameterCollection();

            for (int i = 0; i < mSchema.Rows.Count; i++)
			{
                DataRow row = mSchema.Rows[i];
				string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
				{
					if (whereStmt.Length > 0)
						whereStmt.Append(" AND ");

                    whereStmt.Append(columnName + " = " + GetParameterPlaceholder(i));
					whereParams.Add(CreateParameter(i, DataRowVersion.Current));
				}
				else
				{
					if (setStmt.Length > 0)
						setStmt.Append(", ");

                    setStmt.Append(columnName + "= " + GetParameterPlaceholder(i));
					setParams.Add(CreateParameter(i, DataRowVersion.Current));
				}
			}

			cmd.CommandType = CommandType.Text;
			cmd.CommandText = "UPDATE " + strBaseTable + " SET " + setStmt.ToString();

            foreach (MaxDBParameter param in setParams)
                cmd.Parameters.Add(param);

			if (whereStmt.Length > 0)
			{
				cmd.CommandText += " WHERE " + whereStmt.ToString();
                foreach (MaxDBParameter param in whereParams)
                    cmd.Parameters.Add(param);
			}

			cmdUpdate = cmd;
			return cmdUpdate;
		}

		private void OnRowUpdating(object sender, MaxDBRowUpdatingEventArgs args)
		{
			if (args.Status != UpdateStatus.Continue) 
				return;

			if (mSchema == null)
				RefreshSchema();

			switch(args.StatementType)
			{
				case StatementType.Select:
					return;
				case StatementType.Insert:
					args.Command = GetInsertCommand();
					break;
				case StatementType.Update:
					args.Command = GetUpdateCommand();
					break;
				case StatementType.Delete:
					args.Command = GetDeleteCommand();
					break;
			}

			foreach(MaxDBParameter param in args.Command.Parameters)
			{
				if (param.SourceVersion == DataRowVersion.Original)
					param.Value = args.Row[param.SourceColumn, DataRowVersion.Original];
				else
					param.Value = args.Row[param.SourceColumn, DataRowVersion.Current];
			}
		}
	}
}
