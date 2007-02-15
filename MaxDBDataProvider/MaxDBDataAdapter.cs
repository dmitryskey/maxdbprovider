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
using System.Data;
using System.Data.Common;
#if NET20
using System.Collections.Generic;
using MaxDB.Data.MaxDBProtocol;
#endif // NET20

namespace MaxDB.Data
{
	/// <summary>
	/// Represents a set of data commands and a database connection that are used to fill a dataset and update a MaxDB database. 
	/// This class cannot be inherited.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The <B>MaxDBDataAdapter</B>, serves as a bridge between a <see cref="System.Data.DataSet"/>
	/// and MaxDB for retrieving and saving data. The <B>MaxDBDataAdapter</B> provides this 
	/// bridge by mapping <see cref="DbDataAdapter.Fill(DataSet)"/>, which changes the data in the 
	/// <B>DataSet</B> to match the data in the data source, and <see cref="DbDataAdapter.Update(DataSet)"/>, 
	/// which changes the data in the data source to match the data in the <B>DataSet</B>, 
	/// using the appropriate SQL statements against the data source.
	/// </para>
	/// <para>
	/// When the <B>MaxDBDataAdapter</B> fills a <B>DataSet</B>, it will create the necessary 
	/// tables and columns for the returned data if they do not already exist. However, primary 
	/// key information will not be included in the implicitly created schema unless the 
	/// <see cref="System.Data.MissingSchemaAction"/> property is set to <see cref="System.Data.MissingSchemaAction.AddWithKey"/>. 
	/// You may also have the <B>MaxDBDataAdapter</B> create the schema of the <B>DataSet</B>, 
	/// including primary key information, before filling it with data using 
	/// <see cref="System.Data.Common.DbDataAdapter.FillSchema(System.Data.DataTable, System.Data.SchemaType)"/>. 
	/// </para>
	/// <para><B>MaxDBDataAdapter</B> is used in conjunction with <see cref="MaxDBConnection"/>
	/// and <see cref="MaxDBCommand"/> to increase performance when connecting to a MaxDB database.
	/// </para>
	/// <para>The <B>MaxDBDataAdapter</B> also includes the <see cref="MaxDBDataAdapter.SelectCommand"/>, 
	/// <see cref="MaxDBDataAdapter.InsertCommand"/>, <see cref="MaxDBDataAdapter.DeleteCommand"/>, 
	/// <see cref="MaxDBDataAdapter.UpdateCommand"/>, and <see cref="DataAdapter.TableMappings"/> 
	/// properties to facilitate the loading and updating of data.
	/// </para>
	/// </remarks>
	[System.ComponentModel.DesignerCategory("Code")]
	public sealed class MaxDBDataAdapter : DbDataAdapter, IDbDataAdapter
	{
		private MaxDBCommand cmdSelect;
		private MaxDBCommand cmdInsert;
		private MaxDBCommand cmdUpdate;
		private MaxDBCommand cmdDelete;

#if NET20
		private int batUpdateSize;
		private MaxDBCommand batInsertCmd;
		private MaxDBCommand batUpdateCmd;
		private MaxDBCommand batDeleteCmd;
		private List<MaxDBParameterCollection> lstInsertParams = new List<MaxDBParameterCollection>();
		private List<MaxDBParameterCollection> lstUpdateParams = new List<MaxDBParameterCollection>();
		private List<MaxDBParameterCollection> lstDeleteParams = new List<MaxDBParameterCollection>();
		private StatementType stCurrentType = StatementType.Select;
#endif // NET20

		static private readonly object EventRowUpdated = new object();
		static private readonly object EventRowUpdating = new object();

		/// <summary>
		/// Initializes a new instance of the MaxDBDataAdapter class.
		/// </summary>
		public MaxDBDataAdapter()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MaxDBDataAdapter"/> class with 
		/// the specified <see cref="MaxDBCommand"/> as the <see cref="SelectCommand"/> property.
		/// </summary>
		/// <param name="selectCommand"><see cref="MaxDBCommand"/> that is a SQL SELECT statement or stored procedure/function call
		/// and is set as the <see cref="SelectCommand"/> property of the <see cref="MaxDBDataAdapter"/>. 
		/// </param>
		public MaxDBDataAdapter(MaxDBCommand selectCommand)
		{
			cmdSelect = selectCommand;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MaxDBDataAdapter"/> class with 
		/// the specified <see cref="MaxDBCommand"/> as the <see cref="SelectCommand"/> property and <see cref="MaxDBConnection"/> object.
		/// </summary>
		/// <param name="selectCmdText"><see cref="MaxDBCommand"/> that is a SQL SELECT statement or stored procedure/function call
		/// and is set as the <see cref="SelectCommand"/> property of the <see cref="MaxDBDataAdapter"/>.
		/// </param>
		/// <param name="connection">The <see cref="MaxDBConnection"/> object that represents the connection.</param>
		public MaxDBDataAdapter(string selectCmdText, MaxDBConnection connection)
		{
			cmdSelect = new MaxDBCommand(selectCmdText, connection);
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MaxDBDataAdapter"/> class with 
		/// the specified <see cref="MaxDBCommand"/> as the <see cref="SelectCommand"/> property and connection string.
		/// </summary>
		/// <param name="selectCmdText"><see cref="MaxDBCommand"/> that is a SQL SELECT statement or stored procedure/function call
		/// and is set as the <see cref="SelectCommand"/> property of the <see cref="MaxDBDataAdapter"/>.
		/// </param>
		/// <param name="connectionString">The connection string.</param>
		public MaxDBDataAdapter(string selectCmdText, string connectionString)
		{
			cmdSelect = new MaxDBCommand(selectCmdText, new MaxDBConnection(connectionString));
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="dataRow">The <see cref="DataRow"/> object.</param>
		/// <param name="command">The <see cref="IDbCommand"/> object.</param>
		/// <param name="statementType">The statement type.</param>
		/// <param name="tableMapping">The table column mapping.</param>
		/// <returns>The new <see cref="MaxDBRowUpdatedEventArgs"/> object.</returns>
		override protected RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
		{
			return new MaxDBRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="dataRow">The <see cref="DataRow"/> object.</param>
		/// <param name="command">The <see cref="IDbCommand"/> object.</param>
		/// <param name="statementType">The statement type.</param>
		/// <param name="tableMapping">The table column mapping.</param>
		/// <returns>The new <see cref="MaxDBRowUpdatingEventArgs"/> object.</returns>
		override protected RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
		{
			return new MaxDBRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="value">The <see cref="RowUpdatingEventArgs"/> object.</param>
		override protected void OnRowUpdating(RowUpdatingEventArgs value)
		{
#if NET20
			stCurrentType = value.StatementType;
			EventHandler<MaxDBRowUpdatingEventArgs> handler = (EventHandler<MaxDBRowUpdatingEventArgs>)Events[EventRowUpdating];
#else
			MaxDBRowUpdatingEventHandler handler = (MaxDBRowUpdatingEventHandler) Events[EventRowUpdating];
#endif // NET20

			if ((null != handler) && (value.GetType() == typeof(MaxDBRowUpdatingEventArgs)))
				handler(this, (MaxDBRowUpdatingEventArgs)value);
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="value">The <see cref="RowUpdatedEventArgs"/> object.</param>
		override protected void OnRowUpdated(RowUpdatedEventArgs value)
		{
#if NET20
			EventHandler<MaxDBRowUpdatedEventArgs> handler = (EventHandler<MaxDBRowUpdatedEventArgs>)Events[EventRowUpdated];
#else
			MaxDBRowUpdatedEventHandler handler = (MaxDBRowUpdatedEventHandler)Events[EventRowUpdated];
#endif // NET20
			if ((null != handler) && (value.GetType() == typeof(MaxDBRowUpdatedEventArgs)))
				handler(this, (MaxDBRowUpdatedEventArgs)value);
		}

		/// <summary>
		/// Occurs during Update before a command is executed against the data source. The attempt to update is made, so the event fires.
		/// </summary>
#if NET20
		public event EventHandler<MaxDBRowUpdatingEventArgs> RowUpdating
#else
		public event MaxDBRowUpdatingEventHandler RowUpdating
#endif // NET20
		{
			add
			{
				Events.AddHandler(EventRowUpdating, value);
			}
			remove
			{
				Events.RemoveHandler(EventRowUpdating, value);
			}
		}

		/// <summary>
		/// Occurs during Update after a command is executed against the data source. The attempt to update is made, so the event fires.
		/// </summary>
#if NET20
		public event EventHandler<MaxDBRowUpdatedEventArgs> RowUpdated
#else
		public event MaxDBRowUpdatedEventHandler RowUpdated
#endif // NET20
		{
			add
			{
				Events.AddHandler(EventRowUpdated, value);
			}
			remove
			{
				Events.RemoveHandler(EventRowUpdated, value);
			}
		}

		#region IDbDataAdapter Members

		/// <summary>
		/// Gets or sets a SQL statement or stored procedure/function call used to updated records in the data source.
		/// </summary>
		/// <value>
		/// A <see cref="MaxDBCommand"/> used during <see cref="System.Data.Common.DataAdapter.Update"/> to update records in the 
		/// database with data from the <see cref="DataSet"/>.
		/// </value>
		/// <remarks>
		/// <para>During <see cref="System.Data.Common.DataAdapter.Update"/>, if this property is not set and primary key information 
		/// is present in the <see cref="DataSet"/>, the <B>UpdateCommand</B> can be generated 
		/// automatically if you set the <see cref="SelectCommand"/> property and use the 
		/// <see cref="MaxDBCommandBuilder"/>.  Then, any additional commands that you do not set are 
		/// generated by the <B>MaxDBCommandBuilder</B>. This generation logic requires key column 
		/// information to be present in the <B>DataSet</B>. 
		/// </para>
		/// <para>
		/// When <B>UpdateCommand</B> is assigned to a previously created <see cref="MaxDBCommand"/>, 
		/// the <B>MaxDBCommand</B> is not cloned. The <B>UpdateCommand</B> maintains a reference 
		/// to the previously created <B>MaxDBCommand</B> object.
		/// </para>
		/// <note>
		/// If execution of this command returns rows, these rows may be merged with the DataSet
		/// depending on how you set the <see cref="MaxDBCommand.UpdatedRowSource"/> property of the <B>MaxDBCommand</B> object.
		/// </note>
		/// </remarks>
#if NET20
		public new MaxDBCommand UpdateCommand
#else
		public MaxDBCommand UpdateCommand
#endif  // NET20
		{
			get
			{
				return cmdUpdate;
			}
			set
			{
				cmdUpdate = value;
			}
		}

		IDbCommand IDbDataAdapter.UpdateCommand
		{
			get
			{
				return UpdateCommand;
			}
			set
			{
				UpdateCommand = (MaxDBCommand)value;
			}
		}

		/// <summary>
		/// Gets or sets a SQL statement or stored procedure/function call used to select records in the data source.
		/// </summary>
		/// <value>
		/// A <see cref="MaxDBCommand"/> used during <see cref="System.Data.Common.DbDataAdapter.Fill(System.Data.DataSet)"/> 
		/// to select records from the database for placement in the <see cref="DataSet"/>.
		/// </value>
		/// <remarks>
		/// <para>When <B>SelectCommand</B> is assigned to a previously created <see cref="MaxDBCommand"/>, 
		/// the <B>MaxDBCommand</B> is not cloned. The <B>SelectCommand</B> maintains a reference to the 
		/// previously created <B>MaxDBCommand</B> object.
		/// </para>
		/// <para>
		/// If the <B>SelectCommand</B> does not return any rows, no tables are added to the 
		/// <see cref="DataSet"/>, and no exception is raised.
		/// </para>
		/// </remarks>
#if NET20
		public new MaxDBCommand SelectCommand
#else
		public MaxDBCommand SelectCommand
#endif // NET20
		{
			get
			{
				return cmdSelect;
			}
			set
			{
				cmdSelect = value;
			}
		}

		IDbCommand IDbDataAdapter.SelectCommand
		{
			get
			{
				return SelectCommand;
			}
			set
			{
				SelectCommand = (MaxDBCommand)value;
			}
		}

		/// <summary>
		/// Gets or sets a SQL statement or stored procedure/function call used to delete records from the data set.
		/// </summary>
		/// <value>
		/// A <see cref="MaxDBCommand"/> used during <see cref="System.Data.Common.DataAdapter.Update"/> to delete records in the 
		/// database that correspond to deleted rows in the <see cref="DataSet"/>.
		/// </value>
		/// <remarks>
		/// <para>During <see cref="System.Data.Common.DataAdapter.Update"/>, if this property is not set and primary key information 
		/// is present in the <see cref="DataSet"/>, the <B>DeleteCommand</B> can be generated 
		/// automatically if you set the <see cref="SelectCommand"/> property and use the 
		/// <see cref="MaxDBCommandBuilder"/>.  Then, any additional commands that you do not set are 
		/// generated by the <B>MaxDBCommandBuilder</B>. This generation logic requires key column 
		/// information to be present in the <B>DataSet</B>. 
		/// </para>
		/// <para>
		/// When <B>DeleteCommand</B> is assigned to a previously created <see cref="MaxDBCommand"/>, 
		/// the <B>MaxDBCommand</B> is not cloned. The <B>DeleteCommand</B> maintains a reference 
		/// to the previously created <B>MaxDBCommand</B> object.
		/// </para>
		/// </remarks>
#if NET20
		public new MaxDBCommand DeleteCommand
#else
		public MaxDBCommand DeleteCommand
#endif // NET20
		{
			get
			{
				return cmdDelete;
			}
			set
			{
				cmdDelete = value;
			}
		}

		IDbCommand IDbDataAdapter.DeleteCommand
		{
			get
			{
				return DeleteCommand;
			}
			set
			{
				DeleteCommand = (MaxDBCommand)value;
			}
		}

		/// <summary>
		/// Gets or sets a SQL statement or stored procedure used to insert records into the data set.
		/// </summary>
		/// <value>
		/// A <see cref="MaxDBCommand"/> used during <see cref="System.Data.Common.DataAdapter.Update"/> to insert records into the 
		/// database that correspond to new rows in the <see cref="DataSet"/>.
		/// </value>
		/// <remarks>
		/// <para>During <see cref="System.Data.Common.DataAdapter.Update"/>, if this property is not set and primary key information 
		/// is present in the <see cref="DataSet"/>, the <B>InsertCommand</B> can be generated 
		/// automatically if you set the <see cref="SelectCommand"/> property and use the 
		/// <see cref="MaxDBCommandBuilder"/>.  Then, any additional commands that you do not set are 
		/// generated by the <B>MaxDBCommandBuilder</B>. This generation logic requires key column 
		/// information to be present in the <B>DataSet</B>. 
		/// </para>
		/// <para>
		/// When <B>InsertCommand</B> is assigned to a previously created <see cref="MaxDBCommand"/>, 
		/// the <B>MaxDBCommand</B> is not cloned. The <B>InsertCommand</B> maintains a reference 
		/// to the previously created <B>MaxDBCommand</B> object.
		/// </para>
		/// <note>
		/// If execution of this command returns rows, these rows may be added to the <B>DataSet</B> 
		/// depending on how you set the <see cref="MaxDBCommand.UpdatedRowSource"/> property of the <B>MaxDBCommand</B> object.
		/// </note>
		/// </remarks>
#if NET20
		public new MaxDBCommand InsertCommand
#else
		public MaxDBCommand InsertCommand
#endif // NET20
		{
			get
			{
				return cmdInsert;
			}
			set
			{
				cmdInsert = value;
			}
		}

		IDbCommand IDbDataAdapter.InsertCommand
		{
			get
			{
				return InsertCommand;
			}
			set
			{
				InsertCommand = (MaxDBCommand)value;
			}
		}
		#endregion

#if NET20
		/// <summary>
		/// Gets or sets a value that enables or disables batch processing support, and specifies the number of commands that can be executed in a batch. 
		/// </summary>
		public override int UpdateBatchSize
		{
			get
			{
				return batUpdateSize;
			}
			set
			{
				batUpdateSize = value;
			}
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		protected override void InitializeBatching()
		{
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		protected override void TerminateBatching()
		{
			if (batInsertCmd != null)
				batInsertCmd.Cancel();
			if (batUpdateCmd != null)
				batUpdateCmd.Cancel();
			if (batDeleteCmd != null)
				batDeleteCmd.Cancel();
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="command">The <see cref="IDbCommand"/> object.</param>
		/// <returns>The number of commands in the batch before adding the IDbCommand.</returns>
		protected override int AddToBatch(IDbCommand command)
		{
			MaxDBCommand addCommand = (MaxDBCommand)command;
			switch (stCurrentType)
			{
				case StatementType.Insert:
					batInsertCmd = addCommand;
					lstInsertParams.Add(((MaxDBParameterCollection)command.Parameters).Clone());
					break;
				case StatementType.Update:
					batUpdateCmd = addCommand;
					lstUpdateParams.Add(((MaxDBParameterCollection)command.Parameters).Clone());
					break;
				case StatementType.Delete:
					batDeleteCmd = addCommand;
					lstDeleteParams.Add(((MaxDBParameterCollection)command.Parameters).Clone());
					break;
				default:
					break;
			}

			return lstInsertParams.Count + lstUpdateParams.Count + lstDeleteParams.Count - 1;
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		protected override void ClearBatch()
		{
			batInsertCmd = null;
			batUpdateCmd = null;
			batDeleteCmd = null;
			lstInsertParams.Clear();
			lstUpdateParams.Clear();
			lstDeleteParams.Clear();
		}

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		protected override int ExecuteBatch()
		{
			int rowAffected = 0;
			if (batInsertCmd != null)
			{
				batInsertCmd.ExecuteBatch(lstInsertParams.ToArray());
				rowAffected += batInsertCmd.iRowsAffected;
			}
			if (batUpdateCmd != null)
			{
				batUpdateCmd.ExecuteBatch(lstUpdateParams.ToArray());
				rowAffected += batUpdateCmd.iRowsAffected;
			}
			if (batDeleteCmd != null)
			{
				batDeleteCmd.ExecuteBatch(lstDeleteParams.ToArray());
				rowAffected += batDeleteCmd.iRowsAffected;
			}
			return rowAffected;
		}
#endif // NET20

	}

#if !NET20
	/// <summary>
	/// Represents the method that will handle the <see cref="MaxDBDataAdapter.RowUpdating"/> event of a <b>MaxDBDataAdapter</b>.
	/// </summary>
	public delegate void MaxDBRowUpdatingEventHandler(object sender, MaxDBRowUpdatingEventArgs e);
	/// <summary>
	/// Represents the method that will handle the <see cref="MaxDBDataAdapter.RowUpdated"/> event of a <b>MaxDBDataAdapter</b>.
	/// </summary>
	public delegate void MaxDBRowUpdatedEventHandler(object sender, MaxDBRowUpdatedEventArgs e);
#endif

	/// <summary>
	/// Provides data for the RowUpdating event. This class cannot be inherited.
	/// </summary>
	public sealed class MaxDBRowUpdatingEventArgs : RowUpdatingEventArgs
	{
		/// <summary>
		/// Initializes a new instance of the MaxDBRowUpdatingEventArgs class.
		/// </summary>
		/// <param name="row">The <see cref="DataRow"/> to <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		/// <param name="command">The <see cref="IDbCommand"/> to execute during <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		/// <param name="statementType">One of the <see cref="StatementType"/> values that specifies the type of query executed.</param>
		/// <param name="tableMapping">The <see cref="DataTableMapping"/> sent through an <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		public MaxDBRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
			: base(row, command, statementType, tableMapping)
		{
		}

		/// <summary>
		/// Hides the inherited implementation of the command property.
		/// </summary>
		new public MaxDBCommand Command
		{
			get
			{
				return (MaxDBCommand)base.Command;
			}
			set
			{
				base.Command = value;
			}
		}
	}

	/// <summary>
	/// Provides data for the RowUpdated event. This class cannot be inherited.
	/// </summary>
	public sealed class MaxDBRowUpdatedEventArgs : RowUpdatedEventArgs
	{
		/// <summary>
		/// Initializes a new instance of the MaxDBRowUpdatedEventArgs class.
		/// </summary>
		/// <param name="row">The <see cref="DataRow"/> to <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		/// <param name="command">The <see cref="IDbCommand"/> to execute during <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		/// <param name="statementType">One of the <see cref="StatementType"/> values that specifies the type of query executed.</param>
		/// <param name="tableMapping">The <see cref="DataTableMapping"/> sent through an <see cref="DbDataAdapter.Update(System.Data.DataSet)"/>.</param>
		public MaxDBRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
			: base(row, command, statementType, tableMapping)
		{
		}

		/// <summary>
		/// Hides the inherited implementation of the command property.
		/// </summary>
		new public MaxDBCommand Command
		{
			get
			{
				return (MaxDBCommand)base.Command;
			}
		}
	}
}

