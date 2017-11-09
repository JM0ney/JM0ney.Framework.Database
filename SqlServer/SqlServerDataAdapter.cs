//
//  Do you like this project? Do you find it helpful? Pay it forward by hiring me as a consultant!
//  https://jason-iverson.com
//
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace JM0ney.Framework.Database.SqlServer {

    public class SqlServerDataAdapter : Data.IDataAdapter {

        public Result<int> Delete<TDataObject>( Guid identity )
            where TDataObject : class, Data.IDataObject, new() {
            SqlParameter[ ] parameters = new SqlParameter[ ] { SqlServerHelper.GetSqlParameter( "Identity", identity ) };
            String commandText = SqlServerHelper.PrepareCommandText<TDataObject>( CommonCommands.DeleteRecord, parameters );
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText, parameters );

            Result<int> result = Result.SuccessResult<int>( 0 );
            TDataObject dataObj = new TDataObject( );
            if ( dataObj is Data.IDataObjectMapping ) {
                Result<TDataObject> loadResult = this.Load<TDataObject>( identity );
                if ( loadResult.IsSuccess ) {
                    Data.IDataObjectMapping mapping = ( Data.IDataObjectMapping ) loadResult.ReturnValue;
                    result = this.AssertMapping( mapping, false );
                }
            }

            if ( result.IsSuccess )
                result = SqlServerHelper.ExecuteStatement( ref command );

            return result;
        }

        public Result<IEnumerable<TDataObject>> List<TDataObject>( )
            where TDataObject : class, Data.IDataObjectBase, new() {

            String prefix = SqlServerHelper.GetFieldNamePrefix( new TDataObject( ) );
            String commandText = SqlServerHelper.PrepareCommandText<TDataObject>( CommonCommands.RecordList );
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText );
            Result<IEnumerable<TDataObject>> returnValue = null;
            Result<System.Data.DataSet> readResult = SqlServerHelper.ExecuteFill( ref command );
            if ( !readResult.IsSuccess ) {
                returnValue = Result.ErrorResult<IEnumerable<TDataObject>>( readResult.Message, readResult.Exception );
            }
            else {
                List<TDataObject> returnObjs = new List<TDataObject>( );
                for(int i = 0; i < readResult.ReturnValue.Tables[0].Rows.Count - 1; i++ ) {
                    TDataObject dataObj = new TDataObject( );
                    dataObj.Adapter = this;
                    dataObj.Load( prefix, false, 0, i, readResult.ReturnValue );
                    returnObjs.Add( dataObj );
                }

                if(command != null ) {
                    if ( command.Connection.State != System.Data.ConnectionState.Closed )
                        command.Connection.Close( );
                    command.Connection.Dispose( );
                    command.Dispose( );
                }

                returnValue = Result.SuccessResult<IEnumerable<TDataObject>>( returnObjs );
            }
            return returnValue;
        }

        public Result<IEnumerable<TDataObject>> ListBy<TDataObject>( String fieldName, Object fieldValue )
            where TDataObject : class, Data.IDataObjectBase, new() {
            KeyValuePair<String, Object> keyValue = new KeyValuePair<string, object>( fieldName, fieldValue );
            return this.ListBy<TDataObject>( keyValue );
        }

        public Result<IEnumerable<TDataObject>> ListBy<TDataObject>( params KeyValuePair<String, Object>[] keyValuePairs )
            where TDataObject : class, Data.IDataObjectBase, new() {

            if ( keyValuePairs == null || keyValuePairs.Length < 1 )
                throw new InvalidOperationException( ErrorMessages.ListByArgumentError );

            Result<DataSet> readResult = null;
            Result<IEnumerable<TDataObject>> returnValue = null;
            List<SqlParameter> parameterList = new System.Collections.Generic.List<SqlParameter>( );
            String commandText = String.Empty;
            String prefix = String.Empty;
            SqlCommand command = null;

            parameterList.AddRange( SqlServerHelper.GetSqlParameters( keyValuePairs ) );
            commandText = SqlServerHelper.PrepareCommandText<TDataObject>( CommonCommands.RecordList, parameterList.ToArray( ) );
            prefix = SqlServerHelper.GetFieldNamePrefix( new TDataObject( ) );
            command = SqlServerHelper.GetSqlCommand( commandText, parameterList.ToArray( ) );

            readResult = SqlServerHelper.ExecuteFill( ref command );
            if ( !readResult.IsSuccess ) {
                returnValue = Result.ErrorResult<IEnumerable<TDataObject>>( readResult.Message, readResult.Exception );
            }
            else {
                List<TDataObject> returnObjs = new System.Collections.Generic.List<TDataObject>( );
                for (int i = 0; i < readResult.ReturnValue.Tables[0].Rows.Count; i++ ) {
                    TDataObject dataObj = new TDataObject( );
                    dataObj.Adapter = this;
                    dataObj.Load( prefix, false, 0, i, readResult.ReturnValue );
                    returnObjs.Add( dataObj );
                }

                if (command != null ) {
                    if ( command.Connection.State != ConnectionState.Closed )
                        command.Connection.Close( );
                    command.Connection.Dispose( );
                    command.Dispose( );
                }
                returnValue = Result.SuccessResult<IEnumerable<TDataObject>>( returnObjs );
            }

            return returnValue;
        }

        public Result<TDataObject> Load<TDataObject>( Guid identity ) 
            where TDataObject : class, Data.IDataObjectBase, new() {
            TDataObject dataObj = new TDataObject( );
            dataObj.Adapter = this;

            bool wasRead = false;
            SqlParameter[ ] parameters = new SqlParameter[ ] { SqlServerHelper.GetSqlParameter( "Identity", identity ) };
            String prefix = SqlServerHelper.GetFieldNamePrefix( dataObj );
            String commandText = SqlServerHelper.PrepareCommandText<TDataObject>( CommonCommands.LoadRecord, parameters );
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText, parameters );

            Result<TDataObject> returnValue;
            Result<System.Data.DataSet> readResult = SqlServerHelper.ExecuteFill( ref command );
            if ( !readResult.IsSuccess ) {
                returnValue = Result.ErrorResult<TDataObject>( readResult.Message, readResult.Exception );
            }
            else {
                wasRead = readResult.ReturnValue.Tables[ 0 ].Rows.Count > 0;
                if ( !wasRead ) {
                    returnValue = Result.ErrorResult<TDataObject>( String.Format( ErrorMessages.LoadError_FS, dataObj.Metadata.FriendlyNameSingular, identity ) );
                }
                else {
                    dataObj.Load( prefix, true, 0, 0, readResult.ReturnValue );
                    returnValue = Result.SuccessResult<TDataObject>( dataObj );
                }
            }

            if ( command != null ) {
                if ( command.Connection.State != System.Data.ConnectionState.Closed )
                    command.Connection.Close( );
                command.Connection.Dispose( );
                command.Dispose( );
            }

            return returnValue;
        }

        public Result<int> Save<TDataObject>( TDataObject dataObject )
            where TDataObject : class, Data.IDataObject, new() {
            String commandText = String.Empty;
            List<SqlParameter> parameters = new List<SqlParameter>( );
            SqlCommand command;

            parameters.AddRange( SqlServerHelper.GetSqlParameters( dataObject.GetValues( ) ) );
            commandText = SqlServerHelper.PrepareCommandText<TDataObject>( CommonCommands.SaveRecord, parameters.ToArray( ) );
            command = SqlServerHelper.GetSqlCommand( commandText, parameters.ToArray( ) );

            Result<int> result = SqlServerHelper.ExecuteStatement( ref command );
            if (dataObject is Data.IDataObjectMapping ) {
                Data.IDataObjectMapping mappedObject = ( Data.IDataObjectMapping ) dataObject;
                if ( result.IsSuccess )
                    result = this.AssertMapping( mappedObject, true );
            }
            return result;
        }

        public Result<DataSet> ExecuteFill(String commandText, params KeyValuePair<String, Object>[] parameters ) {
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText, SqlServerHelper.GetSqlParameters( parameters ).ToArray( ) );
            Result<System.Data.DataSet> returnValue = SqlServerHelper.ExecuteFill( ref command );
            if (command != null ) {
                if ( command.Connection.State != System.Data.ConnectionState.Closed )
                    command.Connection.Close( );
                command.Connection.Dispose( );
                command.Dispose( );
            }
            return returnValue;
        }

        public Result<int> ExecuteStatement( String commandText, params KeyValuePair<String, Object>[] parameters ) {
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText, SqlServerHelper.GetSqlParameters( parameters ).ToArray( ) );
            Result<int> returnValue = SqlServerHelper.ExecuteStatement( ref command );
            if (command != null ) {
                if ( command.Connection.State != System.Data.ConnectionState.Closed )
                    command.Connection.Close( );
                command.Connection.Dispose( );
                command.Dispose( );
            }
            return returnValue;
        }

        public Result<int> AssertMapping( Data.IDataObjectMapping mappedObject, bool ensureExists ) {
            List<SqlParameter> parameters = new List<SqlParameter>( );
            parameters.AddRange( SqlServerHelper.GetSqlParameters( mappedObject, ensureExists ) );

            String commandText = SqlServerHelper.PrepareCommandText( mappedObject );
            SqlCommand command = SqlServerHelper.GetSqlCommand( commandText, parameters.ToArray( ) );

            return SqlServerHelper.ExecuteStatement( ref command );
        }
    }

}
