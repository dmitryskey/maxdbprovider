using System;
using System.Data;
using System.Data.Common;

namespace MaxDBDataProvider
{
	public class MaxDBDataAdapter : DbDataAdapter, IDbDataAdapter
	{
		private MaxDBCommand m_selectCommand;
		private MaxDBCommand m_insertCommand;
		private MaxDBCommand m_updateCommand;
		private MaxDBCommand m_deleteCommand;

		/*
		 * Inherit from Component through DbDataAdapter. The event
		 * mechanism is designed to work with the Component.Events
		 * property. These variables are the keys used to find the
		 * events in the components list of events.
		 */
		static private readonly object EventRowUpdated = new object(); 
		static private readonly object EventRowUpdating = new object(); 

		public MaxDBDataAdapter()
		{
		}

		public MaxDBCommand SelectCommand 
		{
			get { return m_selectCommand; }
			set { m_selectCommand = value; }
		}

		IDbCommand IDbDataAdapter.SelectCommand 
		{
			get { return m_selectCommand; }
			set { m_selectCommand = (MaxDBCommand)value; }
		}

		public MaxDBCommand InsertCommand 
		{
			get { return m_insertCommand; }
			set { m_insertCommand = value; }
		}

		IDbCommand IDbDataAdapter.InsertCommand 
		{
			get { return m_insertCommand; }
			set { m_insertCommand = (MaxDBCommand)value; }
		}

		public MaxDBCommand UpdateCommand 
		{
			get { return m_updateCommand; }
			set { m_updateCommand = value; }
		}

		IDbCommand IDbDataAdapter.UpdateCommand 
		{
			get { return m_updateCommand; }
			set { m_updateCommand = (MaxDBCommand)value; }
		}

		public MaxDBCommand DeleteCommand 
		{
			get { return m_deleteCommand; }
			set { m_deleteCommand = value; }
		}

		IDbCommand IDbDataAdapter.DeleteCommand 
		{
			get { return m_deleteCommand; }
			set { m_deleteCommand = (MaxDBCommand)value; }
		}

		/*
		 * Implement abstract methods inherited from DbDataAdapter.
		 */
		override protected RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
		{
			return new MaxDBRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
		}

		override protected RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
		{
			return new MaxDBRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
		}

		override protected void OnRowUpdating(RowUpdatingEventArgs value)
		{
			MaxDBRowUpdatingEventHandler handler = (MaxDBRowUpdatingEventHandler) Events[EventRowUpdating];
			if ((null != handler) && (value is MaxDBRowUpdatingEventArgs)) 
			{
				handler(this, (MaxDBRowUpdatingEventArgs) value);
			}
		}

		override protected void OnRowUpdated(RowUpdatedEventArgs value)
		{
			MaxDBRowUpdatedEventHandler handler = (MaxDBRowUpdatedEventHandler) Events[EventRowUpdated];
			if ((null != handler) && (value is MaxDBRowUpdatedEventArgs)) 
			{
				handler(this, (MaxDBRowUpdatedEventArgs) value);
			}
		}

		public event MaxDBRowUpdatingEventHandler RowUpdating
		{
			add { Events.AddHandler(EventRowUpdating, value); }
			remove { Events.RemoveHandler(EventRowUpdating, value); }
		}

		public event MaxDBRowUpdatedEventHandler RowUpdated
		{
			add { Events.AddHandler(EventRowUpdated, value); }
			remove { Events.RemoveHandler(EventRowUpdated, value); }
		}
	}

	public delegate void MaxDBRowUpdatingEventHandler(object sender, MaxDBRowUpdatingEventArgs e);
	public delegate void MaxDBRowUpdatedEventHandler(object sender, MaxDBRowUpdatedEventArgs e);

	public class MaxDBRowUpdatingEventArgs : RowUpdatingEventArgs
	{
		public MaxDBRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) 
			: base(row, command, statementType, tableMapping) 
		{
		}

		// Hide the inherited implementation of the command property.
		new public MaxDBCommand Command
		{
			get  { return (MaxDBCommand)base.Command; }
			set  { base.Command = value; }
		}
	}

	public class MaxDBRowUpdatedEventArgs : RowUpdatedEventArgs
	{
		public MaxDBRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
			: base(row, command, statementType, tableMapping) 
		{
		}

		// Hide the inherited implementation of the command property.
		new public MaxDBCommand Command
		{
			get  { return (MaxDBCommand)base.Command; }
		}
	}
}

