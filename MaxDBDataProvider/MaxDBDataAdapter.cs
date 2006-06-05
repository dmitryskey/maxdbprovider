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

namespace MaxDBDataProvider
{
    [System.ComponentModel.DesignerCategory("Code")]
	public sealed class MaxDBDataAdapter : DbDataAdapter, IDbDataAdapter
	{
		private MaxDBCommand m_selectCommand;
		private MaxDBCommand m_insertCommand;
		private MaxDBCommand m_updateCommand;
		private MaxDBCommand m_deleteCommand;

#if !NET20
		private int m_updBatchSize;
#endif

		static private readonly object EventRowUpdated = new object(); 
		static private readonly object EventRowUpdating = new object(); 

		public MaxDBDataAdapter()
		{
		}

		public MaxDBDataAdapter(MaxDBCommand selectCommand)
		{
			m_selectCommand = selectCommand;
		}

		public MaxDBDataAdapter(string selectCmdText, MaxDBConnection connection) 
		{
			m_selectCommand = new MaxDBCommand(selectCmdText, connection);
		}

		public MaxDBDataAdapter(string selectCmdText, string selectConnString) 
		{
			m_selectCommand = new MaxDBCommand(selectCmdText, new MaxDBConnection(selectConnString));
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
				handler(this, (MaxDBRowUpdatingEventArgs) value);
		}

		override protected void OnRowUpdated(RowUpdatedEventArgs value)
		{
			MaxDBRowUpdatedEventHandler handler = (MaxDBRowUpdatedEventHandler) Events[EventRowUpdated];
			if ((null != handler) && (value is MaxDBRowUpdatedEventArgs)) 
				handler(this, (MaxDBRowUpdatedEventArgs) value);
		}

		public event MaxDBRowUpdatingEventHandler RowUpdating
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

		public event MaxDBRowUpdatedEventHandler RowUpdated
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

#if NET20
        public new MaxDBCommand UpdateCommand
#else
		public MaxDBCommand UpdateCommand
#endif 
		{
			get 
			{ 
				return m_updateCommand; 
			}
			set 
			{ 
				m_updateCommand = value; 
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

#if NET20
        public new MaxDBCommand SelectCommand
#else
		public MaxDBCommand SelectCommand
#endif
		{
			get 
			{ 
				return m_selectCommand; 
			}
			set 
			{ 
				m_selectCommand = value; 
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

#if NET20
        public new MaxDBCommand DeleteCommand
#else
		public MaxDBCommand DeleteCommand
#endif 
		{
			get 
			{ 
				return m_deleteCommand; 
			}
			set 
			{ 
				m_deleteCommand = value; 
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

#if NET20
        public new MaxDBCommand InsertCommand
#else
		public MaxDBCommand InsertCommand
#endif
		{
			get 
			{ 
				return m_insertCommand; 
			}
			set 
			{ 
				m_insertCommand = value; 
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

#if !NET20
		public int UpdateBatchSize
		{
			get
			{
				return m_updBatchSize;
			}
			set
			{
				m_updBatchSize = value;
			}
		}
#endif
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

	public class MaxDBRowUpdatedEventArgs : RowUpdatedEventArgs
	{
		public MaxDBRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
			: base(row, command, statementType, tableMapping) 
		{
		}

		// Hide the inherited implementation of the command property.
		new public MaxDBCommand Command
		{
			get  
			{ 
				return (MaxDBCommand)base.Command; 
			}
		}
	}
}

