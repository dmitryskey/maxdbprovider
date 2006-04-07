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
	public sealed class MaxDBCommandBuilder : Component
	{
		private string m_prefix = "'";
		private string m_suffix = "'";
		private MaxDBDataAdapter m_adapter;
		private DataTable m_schema = null;
		private string m_marker = "?";
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
			m_adapter = adapter;
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
			MaxDBCommand cmd = new MaxDBCommand();
			cmd.Connection = m_adapter.SelectCommand.Connection;
			cmd.CommandTimeout = m_adapter.SelectCommand.CommandTimeout;
			cmd.Transaction = m_adapter.SelectCommand.Transaction;
			return cmd;
		}

		public MaxDBCommand GetDeleteCommand()
		{
			if (m_schema == null)
				RefreshSchema();

			if (m_delCmd != null) 
				return m_delCmd;

			MaxDBCommand cmd = CreateCommand();

			cmd.CommandText = "DELETE FROM " + m_baseTable;

			m_delCmd = cmd;
			return m_delCmd;
		}

		private MaxDBParameter CreateParameter(DataRow row)
		{
			return new MaxDBParameter(
					row["ColumnName"].ToString(),
					(MaxDBType)row["ProviderType"],
					(int)row["ColumnSize"],
					ParameterDirection.Input,
					(bool)row["AllowDBNull"],
					(byte)row["NumericPrecision"],
					(byte)row["NumericScale"],
					row["ColumnName"].ToString(),
					DataRowVersion.Current,
					DBNull.Value);
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
				if (columns.Length > 0 && markers.Length > 0)
				{
					columns.Append(", ");
					markers.Append(", ");
				}

				columns.Append(row["ColumnName"].ToString());
				markers.Append(m_marker);

				m_insCmd.Parameters.Add(CreateParameter(row));
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

			StringBuilder setstmt = new StringBuilder();

			foreach(DataRow row in m_schema.Rows)
			{
				if (setstmt.Length > 0)
					setstmt.Append(", ");

				setstmt.Append(row["ColumnName"].ToString() + "=" + m_marker);

				m_insCmd.Parameters.Add(CreateParameter(row));
			}

			cmd.CommandType = CommandType.Text;
			cmd.CommandText = "UPDATE " + m_baseTable + " SET " + setstmt.ToString();

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
