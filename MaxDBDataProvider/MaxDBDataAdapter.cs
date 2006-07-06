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
    [System.ComponentModel.DesignerCategory("Code")]
    public sealed class MaxDBDataAdapter : DbDataAdapter, IDbDataAdapter
    {
        private MaxDBCommand cmdSelect;
        private MaxDBCommand cmdInsert;
        private MaxDBCommand cmdUpdate;
        private MaxDBCommand cmdDelete;

#if NET20 && !MONO
        private int batUpdateSize;
        private MaxDBCommand batInsertCmd;
        private MaxDBCommand batUpdateCmd;
        private MaxDBCommand batDeleteCmd;
        private List<MaxDBParameterCollection> lstInsertParams = new List<MaxDBParameterCollection>();
        private List<MaxDBParameterCollection> lstUpdateParams = new List<MaxDBParameterCollection>();
        private List<MaxDBParameterCollection> lstDeleteParams = new List<MaxDBParameterCollection>();
        private StatementType stCurrentType = StatementType.Select;
#endif // NET20 && !MONO

        static private readonly object EventRowUpdated = new object();
        static private readonly object EventRowUpdating = new object();

        public MaxDBDataAdapter()
        {
        }

        public MaxDBDataAdapter(MaxDBCommand selectCommand)
        {
            cmdSelect = selectCommand;
        }

        public MaxDBDataAdapter(string selectCmdText, MaxDBConnection connection)
        {
            cmdSelect = new MaxDBCommand(selectCmdText, connection);
        }

        public MaxDBDataAdapter(string selectCmdText, string connectionString)
        {
            cmdSelect = new MaxDBCommand(selectCmdText, new MaxDBConnection(connectionString));
        }

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
#if NET20
#if !MONO
            stCurrentType = value.StatementType;
#endif // !MONO
            EventHandler<MaxDBRowUpdatingEventArgs> handler = (EventHandler<MaxDBRowUpdatingEventArgs>) Events[EventRowUpdating];
#else
            MaxDBRowUpdatingEventHandler handler = (MaxDBRowUpdatingEventHandler) Events[EventRowUpdating];
#endif // NET20

            if ((null != handler) && (value.GetType() == typeof(MaxDBRowUpdatingEventArgs)))
                handler(this, (MaxDBRowUpdatingEventArgs)value);
        }

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

#if NET20 && !MONO
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

        protected override void InitializeBatching()
        {
        }

        protected override void TerminateBatching()
        {
            if (batInsertCmd != null)
                batInsertCmd.Cancel();
            if (batUpdateCmd != null)
                batUpdateCmd.Cancel();
            if (batDeleteCmd != null)
                batDeleteCmd.Cancel();
        }

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

        protected override void ClearBatch()
        {
            batInsertCmd = null;
            batUpdateCmd = null;
            batDeleteCmd = null;
            lstInsertParams.Clear();
            lstUpdateParams.Clear();
            lstDeleteParams.Clear();
        }

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
#endif // NET20 && !MONO

    }

#if !NET20
	public delegate void MaxDBRowUpdatingEventHandler(object sender, MaxDBRowUpdatingEventArgs e);
	public delegate void MaxDBRowUpdatedEventHandler(object sender, MaxDBRowUpdatedEventArgs e);
#endif

    public class MaxDBRowUpdatingEventArgs : RowUpdatingEventArgs
    {
        public MaxDBRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
            : base( row, command, statementType, tableMapping)
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
            : base( row, command, statementType, tableMapping)
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

