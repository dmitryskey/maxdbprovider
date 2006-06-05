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
using System.Text;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for MaxDBCommandBuilder.
	/// </summary>
    [System.ComponentModel.DesignerCategory("Code")]
	public sealed class MaxDBCommandBuilder : Component
	{
		private string m_prefix = "'";
		private string m_suffix = "'";
		private MaxDBDataAdapter m_adapter;
		private DataTable m_schema = null;
		private string m_baseTable = null;
		private MaxDBCommand m_delCmd = null;
		private MaxDBCommand m_insCmd = null;
		private MaxDBCommand m_updCmd = null;

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

		public string QuotePrefix
		{
			get
			{
				return m_prefix;
			}
			set
			{
				m_prefix = value;
			}
		}

		public string QuoteSuffix
		{
			get
			{
				return m_suffix;
			}
			set
			{
				m_suffix = value;
			}
		}

		public MaxDBDataAdapter DataAdapter
		{
			get
			{
				return m_adapter;
			}
			set
			{
				if (m_adapter != null) 
					m_adapter.RowUpdating -= new MaxDBRowUpdatingEventHandler(OnRowUpdating);

				if (value == null)
					throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ADAPTER_NULL));
				m_adapter = value;
				m_adapter.RowUpdating += new MaxDBRowUpdatingEventHandler(OnRowUpdating);
			}
		}
        		
		public void RefreshSchema()
		{
			if (m_adapter == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_ADAPTER_NULL));
			if (m_adapter.SelectCommand == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_SELECT_NULL));
		
			MaxDBDataReader dr = m_adapter.SelectCommand.ExecuteReader(CommandBehavior.SchemaOnly);
			m_schema = dr.GetSchemaTable();

			if (m_schema.Rows.Count > 0 && !m_schema.Rows[0].IsNull("BaseTableName"))
				m_baseTable = m_schema.Rows[0]["BaseTableName"].ToString();
			else
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBMessages.ERROR_BASETABLE_NOTFOUND));
		}

		private MaxDBCommand CreateCommand()
		{
			MaxDBCommand cmd = new MaxDBCommand(string.Empty, m_adapter.SelectCommand.Connection, m_adapter.SelectCommand.Transaction);
			cmd.CommandTimeout = m_adapter.SelectCommand.CommandTimeout;
			return cmd;
		}

		private MaxDBParameter CreateParameter(DataRow row, DataRowVersion version)
		{
			return new MaxDBParameter(
				row["ColumnName"].ToString(),
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

		public MaxDBCommand GetDeleteCommand()
		{
			if (m_schema == null)
				RefreshSchema();

			if (m_delCmd != null) 
				return m_delCmd;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder whereStmt = new StringBuilder();
	
			foreach(DataRow row in m_schema.Rows)
			{
				string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
				{
					if (whereStmt.Length > 0)
						whereStmt.Append(" AND ");
					
					whereStmt.Append(columnName + "= :" + columnName);

					cmd.Parameters.Add(CreateParameter(row, DataRowVersion.Original));
				}
			}

			cmd.CommandText = "DELETE FROM " + m_baseTable;
			if (whereStmt.Length > 0)
				cmd.CommandText += " WHERE " + whereStmt.ToString();

			m_delCmd = cmd;
			return m_delCmd;
		}

		public MaxDBCommand GetInsertCommand()
		{
			if (m_schema == null)
				RefreshSchema();

			if (m_insCmd != null)
				return m_insCmd;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder columns = new StringBuilder();
			StringBuilder markers = new StringBuilder();

			foreach(DataRow row in m_schema.Rows)
			{
				string columnName = row["ColumnName"].ToString();
				if (!(bool)row["IsAutoIncrement"])
				{
					if (columns.Length > 0 && markers.Length > 0)
					{
						columns.Append(", ");
						markers.Append(", ");
					}

					columns.Append(columnName);
					markers.Append(":" + columnName);

					cmd.Parameters.Add(CreateParameter(row, DataRowVersion.Current));
				}
			}

			cmd.CommandType = CommandType.Text;
			cmd.CommandText = "INSERT INTO " + m_baseTable + "(" + columns.ToString() + ") VALUES(" + markers.ToString() + ")";

			m_insCmd = cmd;
			return m_insCmd;
		}

		public MaxDBCommand GetUpdateCommand()
		{
			if (m_schema == null)
				RefreshSchema();

			if (m_updCmd != null)
				return m_updCmd;

			MaxDBCommand cmd = CreateCommand();

			StringBuilder setStmt = new StringBuilder();
			MaxDBParameterCollection setParams = new MaxDBParameterCollection();
			StringBuilder whereStmt = new StringBuilder();
			MaxDBParameterCollection whereParams = new MaxDBParameterCollection();

			foreach(DataRow row in m_schema.Rows)
			{
				string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
				{
					if (whereStmt.Length > 0)
						whereStmt.Append(" AND ");
					
					whereStmt.Append(columnName + "= :" + columnName);
					whereParams.Add(CreateParameter(row, DataRowVersion.Current));
				}
				else
				{
					if (setStmt.Length > 0)
						setStmt.Append(", ");

					setStmt.Append(columnName + "= :" + columnName);
					setParams.Add(CreateParameter(row, DataRowVersion.Current));
				}
			}

			cmd.CommandType = CommandType.Text;
			cmd.CommandText = "UPDATE " + m_baseTable + " SET " + setStmt.ToString();
			foreach(MaxDBParameter param in setParams)
				cmd.Parameters.Add(param);
			if (whereStmt.Length > 0)
			{
				cmd.CommandText += " WHERE " + whereStmt.ToString();
				foreach(MaxDBParameter param in whereParams)
					cmd.Parameters.Add(param);
			}

			m_updCmd = cmd;
			return m_updCmd;
		}

		private void OnRowUpdating(object sender, MaxDBRowUpdatingEventArgs args)
		{
			if (args.Status != UpdateStatus.Continue) 
				return;

			if (m_schema == null)
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
