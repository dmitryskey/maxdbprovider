//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBCommandBuilder.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data
{
    using System;
    using System.ComponentModel;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// Automatically generates single-table commands used to reconcile changes made to a DataSet with the associated MaxDB database.
    /// This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>The <see cref="MaxDBDataAdapter"/> does not automatically generate the SQL statements required to
    /// reconcile changes made to a <see cref="System.Data.DataSet">DataSet</see> with the associated instance of MaxDB.
    /// However, you can create a <B>MaxDBCommandBuilder</B> object to automatically generate SQL statements for
    /// single-table updates if you set the <see cref="MaxDBDataAdapter.SelectCommand">SelectCommand</see> property
    /// of the <B>MaxDBDataAdapter</B>. Then, any additional SQL statements that you do not set are generated by the
    /// <B>MaxDBCommandBuilder</B>.</para>
    /// <para>The select statement must have "FOR UPDATE" suffix.</para>
    /// </remarks>
    [DesignerCategory("Code")]
    public sealed class MaxDBCommandBuilder : DbCommandBuilder
    {
        private string strPrefix = "'";
        private string strSuffix = "'";
        private MaxDBDataAdapter mAdapter;
        private DataTable mSchema;
        private string strBaseTable;
        private MaxDBCommand cmdDelete;
        private MaxDBCommand cmdInsert;
        private MaxDBCommand cmdUpdate;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommandBuilder"/> class.
        /// </summary>
        public MaxDBCommandBuilder()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBCommandBuilder"/> class
        /// with the associated <see cref="MaxDBDataAdapter"/> object.
        /// </summary>
        /// <param name="adapter">Given <see cref="MaxDBDataAdapter"/> to use.</param>
        public MaxDBCommandBuilder(MaxDBDataAdapter adapter)
        {
            this.DataAdapter = adapter;
        }

        /// <summary>
        /// Gets or sets the beginning character or characters to use when specifying MaxDB
        /// database objects (for example, tables or columns) whose names contain
        /// characters such as spaces or reserved tokens.
        /// </summary>
        /// <value>
        /// The beginning character or characters to use.  The default value is '.
        /// </value>
        public override string QuotePrefix
        {
            get
            {
                return this.strPrefix;
            }

            set
            {
                this.strPrefix = value;
            }
        }

        /// <summary>
        /// Gets or sets the ending character or characters to use when specifying MaxDB
        /// database objects (for example, tables or columns) whose names contain
        /// characters such as spaces or reserved tokens.
        /// </summary>
        /// <value>
        /// The ending character or characters to use.  The default value is '.
        /// </value>
        public override string QuoteSuffix
        {
            get
            {
                return this.strSuffix;
            }

            set
            {
                this.strSuffix = value;
            }
        }

        /// <summary>
        /// Gets or sets a <see cref="MaxDBDataAdapter"/> object for which SQL statements are automatically generated.
        /// </summary>
        public new MaxDBDataAdapter DataAdapter
        {
            get
            {
                return this.mAdapter;
            }

            set
            {
                if (this.mAdapter != null)
                {
                    this.mAdapter.RowUpdating -= new EventHandler<MaxDBRowUpdatingEventArgs>(this.OnRowUpdating);
                }

                this.mAdapter = value ?? throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADAPTERNULL));
                this.mAdapter.RowUpdating += new EventHandler<MaxDBRowUpdatingEventArgs>(this.OnRowUpdating);
            }
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="adapter">Given <see cref="DbDataAdapter"/> to use.</param>
        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            this.DataAdapter = (MaxDBDataAdapter)adapter;
        }

        /// <summary>
        /// Refreshes the database schema information used to generate INSERT, UPDATE, or DELETE statements.
        /// </summary>
        /// <remarks>
        /// <para>
        /// An application should call <B>RefreshSchema</B> whenever the SELECT statement or the
        /// <see cref="MaxDBDataAdapter.SelectCommand"/> value of the <see cref="MaxDBDataAdapter"/>
        /// associated with the <see cref="MaxDBCommandBuilder"/> changes.
        /// </para>
        /// </remarks>
        public override void RefreshSchema()
        {
            if (this.mAdapter == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADAPTERNULL));
            }

            if (this.mAdapter.SelectCommand == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.SELECTNULL));
            }

            var dr = this.mAdapter.SelectCommand.ExecuteReader(CommandBehavior.SchemaOnly);
            this.mSchema = dr.GetSchemaTable();

            if (this.mSchema.Rows.Count > 0 && !this.mSchema.Rows[0].IsNull("BaseTableName"))
            {
                this.strBaseTable = this.mSchema.Rows[0]["BaseTableName"].ToString();
            }
            else
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.BASETABLENOTFOUND));
            }
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameter">DbParameter</param>
        /// <param name="row">Data row</param>
        /// <param name="statementType">Statement type</param>
        /// <param name="whereClause">whether 'WHERE' clause is used or not</param>
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameterOrdinal">Parameter ordinal.</param>
        /// <returns>Parameter name.</returns>
        protected override string GetParameterName(int parameterOrdinal)
        {
            if (parameterOrdinal < 0 || parameterOrdinal >= this.mSchema.Rows.Count)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLINDEXNOTFOUND));
            }

            return this.mSchema.Rows[parameterOrdinal]["ColumnName"].ToString();
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameterName">Parameter name to check existence.</param>
        /// <returns>Parameter name.</returns>
        protected override string GetParameterName(string parameterName)
        {
            foreach (DataRow row in this.mSchema.Rows)
            {
                if (string.Compare(row["ColumnName"].ToString().Trim(), parameterName.Trim(), true, CultureInfo.InvariantCulture) == 0)
                {
                    return parameterName;
                }
            }

            throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.COLNAMENOTFOUND));
        }

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="parameterOrdinal">Parameter ordinal.</param>
        /// <returns>Parameter placeholder of the form :COLUMNNAME.</returns>
        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return ":" + this.mSchema.Rows[parameterOrdinal]["ColumnName"].ToString();
        }

        private MaxDBCommand CreateCommand() => new MaxDBCommand(string.Empty, this.mAdapter.SelectCommand.Connection, this.mAdapter.SelectCommand.Transaction)
        {
            CommandTimeout = this.mAdapter.SelectCommand.CommandTimeout,
        };

        private MaxDBParameter CreateParameter(int index, DataRowVersion version)
        {
            var row = this.mSchema.Rows[index];
            return new MaxDBParameter(
                this.GetParameterName(index),
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

        /// <summary>
        /// Gets the automatically generated <see cref="MaxDBCommand"/> object required to perform deletions on the database.
        /// </summary>
        /// <remarks>
        /// <para>
        /// An application can use the <B>GetDeleteCommand</B> method for informational or troubleshooting purposes because
        /// it returns the <see cref="MaxDBCommand"/> object to be executed.
        /// </para>
        /// <para>
        /// You can also use <B>GetDeleteCommand</B> as the basis of a modified command. For example, you might call
        /// <B>GetDeleteCommand</B> and modify the <see cref="MaxDBCommand.CommandTimeout"/> value, and then explicitly set that on the
        /// <see cref="MaxDBDataAdapter"/>.
        /// </para>
        /// <para>
        /// After the SQL statement is first generated, the application must explicitly call <see cref="RefreshSchema"/>
        /// if it changes the statement in any way. Otherwise, the <B>GetDeleteCommand</B> will be still be using information
        /// from the previous statement, which might not be correct. The SQL statements are first generated either when the application calls
        /// <see cref="System.Data.Common.DataAdapter.Update"/> or <B>GetDeleteCommand</B>.
        /// </para>
        /// </remarks>
        /// <returns>The <see cref="MaxDBCommand"/> object generated to handle delete operations.</returns>
        public new MaxDBCommand GetDeleteCommand()
        {
            if (this.mSchema == null)
            {
                this.RefreshSchema();
            }

            if (this.cmdDelete != null)
            {
                return this.cmdDelete;
            }

            var cmd = this.CreateCommand();

            var whereStmt = new StringBuilder();

            for (int i = 0; i < this.mSchema.Rows.Count; i++)
            {
                DataRow row = this.mSchema.Rows[i];
                string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
                {
                    if (whereStmt.Length > 0)
                    {
                        whereStmt.Append(" AND ");
                    }

                    whereStmt.Append(columnName + " = " + this.GetParameterPlaceholder(i));

                    cmd.Parameters.Add(this.CreateParameter(i, DataRowVersion.Original));
                }
            }

            cmd.CommandText = "DELETE FROM " + this.strBaseTable;
            if (whereStmt.Length > 0)
            {
                cmd.CommandText += " WHERE " + whereStmt.ToString();
            }

            this.cmdDelete = cmd;
            return this.cmdDelete;
        }

        /// <summary>
        /// Gets the automatically generated <see cref="MaxDBCommand"/> object required to perform insertions on the database.
        /// </summary>
        /// <remarks>
        /// <para>
        /// An application can use the <B>GetInsertCommand</B> method for informational or troubleshooting purposes because
        /// it returns the <see cref="MaxDBCommand"/> object to be executed.
        /// </para>
        /// <para>
        /// You can also use <B>GetInsertCommand</B> as the basis of a modified command. For example, you might call
        /// <B>GetInsertCommand</B> and modify the <see cref="MaxDBCommand.CommandTimeout"/> value, and then explicitly set that on the
        /// <see cref="MaxDBDataAdapter"/>.
        /// </para>
        /// <para>
        /// After the SQL statement is first generated, the application must explicitly call <see cref="RefreshSchema"/>
        /// if it changes the statement in any way. Otherwise, the <B>GetInsertCommand</B> will be still be using information
        /// from the previous statement, which might not be correct. The SQL statements are first generated either when the application calls
        /// <see cref="System.Data.Common.DataAdapter.Update"/> or <B>GetInsertCommand</B>.
        /// </para>
        /// </remarks>
        /// <returns>The <see cref="MaxDBCommand"/> object generated to handle insert operations.</returns>
        public new MaxDBCommand GetInsertCommand()
        {
            if (this.mSchema == null)
            {
                this.RefreshSchema();
            }

            if (this.cmdInsert != null)
            {
                return this.cmdInsert;
            }

            var cmd = this.CreateCommand();

            var columns = new StringBuilder();
            var markers = new StringBuilder();

            for (int i = 0; i < this.mSchema.Rows.Count; i++)
            {
                DataRow row = this.mSchema.Rows[i];
                string columnName = row["ColumnName"].ToString();
                if (!(bool)row["IsAutoIncrement"])
                {
                    if (columns.Length > 0 && markers.Length > 0)
                    {
                        columns.Append(", ");
                        markers.Append(", ");
                    }

                    columns.Append(columnName);
                    markers.Append(this.GetParameterPlaceholder(i));

                    cmd.Parameters.Add(this.CreateParameter(i, DataRowVersion.Current));
                }
            }

            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "INSERT INTO " + this.strBaseTable + "(" + columns.ToString() + ") VALUES(" + markers.ToString() + ")";

            this.cmdInsert = cmd;
            return this.cmdInsert;
        }

        /// <summary>
        /// Gets the automatically generated <see cref="MaxDBCommand"/> object required to perform updates on the database.
        /// </summary>
        /// <remarks>
        /// <para>
        /// An application can use the <B>GetUpdateCommand</B> method for informational or troubleshooting purposes because
        /// it returns the <see cref="MaxDBCommand"/> object to be executed.
        /// </para>
        /// <para>
        /// You can also use <B>GetUpdateCommand</B> as the basis of a modified command. For example, you might call
        /// <B>GetUpdateCommand</B> and modify the <see cref="MaxDBCommand.CommandTimeout"/> value, and then explicitly set that on the
        /// <see cref="MaxDBDataAdapter"/>.
        /// </para>
        /// <para>
        /// After the SQL statement is first generated, the application must explicitly call <see cref="RefreshSchema"/>
        /// if it changes the statement in any way. Otherwise, the <B>GetUpdateCommand</B> will be still be using information
        /// from the previous statement, which might not be correct. The SQL statements are first generated either when the application calls
        /// <see cref="System.Data.Common.DataAdapter.Update"/> or <B>GetInsertCommand</B>.
        /// </para>
        /// </remarks>
        /// <returns>The <see cref="MaxDBCommand"/> object generated to handle update operations.</returns>
        public new MaxDBCommand GetUpdateCommand()
        {
            if (this.mSchema == null)
            {
                this.RefreshSchema();
            }

            if (this.cmdUpdate != null)
            {
                return this.cmdUpdate;
            }

            var cmd = this.CreateCommand();

            var setStmt = new StringBuilder();
            var setParams = new MaxDBParameterCollection();
            var whereStmt = new StringBuilder();
            var whereParams = new MaxDBParameterCollection();

            for (int i = 0; i < this.mSchema.Rows.Count; i++)
            {
                DataRow row = this.mSchema.Rows[i];
                string columnName = row["ColumnName"].ToString();

                if ((bool)row["IsKeyColumn"])
                {
                    if (whereStmt.Length > 0)
                    {
                        whereStmt.Append(" AND ");
                    }

                    whereStmt.Append(columnName + " = " + this.GetParameterPlaceholder(i));
                    whereParams.Add(this.CreateParameter(i, DataRowVersion.Current));
                }
                else
                {
                    if (setStmt.Length > 0)
                    {
                        setStmt.Append(", ");
                    }

                    setStmt.Append(columnName + " = " + this.GetParameterPlaceholder(i));
                    setParams.Add(this.CreateParameter(i, DataRowVersion.Current));
                }
            }

            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "UPDATE " + this.strBaseTable + " SET " + setStmt.ToString();

            foreach (MaxDBParameter param in setParams)
            {
                cmd.Parameters.Add(param);
            }

            if (whereStmt.Length > 0)
            {
                cmd.CommandText += " WHERE " + whereStmt.ToString();
                foreach (MaxDBParameter param in whereParams)
                {
                    cmd.Parameters.Add(param);
                }
            }

            this.cmdUpdate = cmd;
            return this.cmdUpdate;
        }

        private void OnRowUpdating(object sender, MaxDBRowUpdatingEventArgs args)
        {
            if (args.Status != UpdateStatus.Continue)
            {
                return;
            }

            if (this.mSchema == null)
            {
                this.RefreshSchema();
            }

            switch (args.StatementType)
            {
                case StatementType.Select:
                    return;
                case StatementType.Insert:
                    args.Command = this.GetInsertCommand();
                    break;
                case StatementType.Update:
                    args.Command = this.GetUpdateCommand();
                    break;
                case StatementType.Delete:
                    args.Command = this.GetDeleteCommand();
                    break;
            }

            foreach (MaxDBParameter param in args.Command.Parameters)
            {
                param.Value = args.Row[param.SourceColumn, param.SourceVersion == DataRowVersion.Original ? DataRowVersion.Original : DataRowVersion.Current];
            }
        }
    }
}
