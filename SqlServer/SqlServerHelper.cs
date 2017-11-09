//
//  Do you like this project? Do you find it helpful? Pay it forward by hiring me as a consultant!
//  https://jason-iverson.com
//
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JM0ney.Framework.Database.SqlServer {

    public static class SqlServerHelper {

        #region Fields

        private static String _ConnectionString = String.Empty;
        private static String _StoredProcedureNamePrefix = "sp";

        public const String ConnectionStringSettingName = "JM0ney:SqlServerHelperConnectionString";
        public const String StoredProcedureNamePrefixSettingName = "JM0ney:StoredProcedureNamePrefix";

        #endregion Fields

        #region Constructor

        static SqlServerHelper( ) {
            String errorMessage = String.Empty;
            if ( !ConfigurationHelper.IsAppSettingConfigured( ConnectionStringSettingName, ref errorMessage ) ) {
                throw new InvalidOperationException( errorMessage );
            }
            else {
                SqlServerHelper.ConnectionString = ConfigurationHelper.TryGetValue<String>( ConnectionStringSettingName, String.Empty );
            }
            if ( ConfigurationHelper.IsAppSettingConfigured( StoredProcedureNamePrefixSettingName, ref errorMessage, false ) ) {
                SqlServerHelper.StoredProcedureNamePrefix = ConfigurationHelper.TryGetValue( StoredProcedureNamePrefixSettingName, String.Empty );
            }
        }


        #endregion Constructor

        #region Methods

        public static String GetFieldNamePrefix( Data.IDataObjectBase dataObject ) {
            String returnValue = String.Empty;
            Data.IDataObjectView dataObjectView = dataObject as Data.IDataObjectView;
            if (dataObjectView != null ) {
                returnValue = dataObjectView.PrepareFieldNamePrefix( dataObject.Metadata );
            }
            return returnValue;
        }

        public static String GetSqlParameterName( String propertyName ) {
            return propertyName.StartsWith( "@" ) ?
                propertyName :
                "@" + propertyName;
        }

        public static SqlParameter GetSqlParameter( String name, Object value ) {
            if ( value == null )
                value = DBNull.Value;
            return new SqlParameter( GetSqlParameterName( name ), value );
        }

        public static IEnumerable<SqlParameter> GetSqlParameters( Dictionary<String, Object> valueDictionary ) {
            List<SqlParameter> parameters = new List<SqlParameter>( );
            foreach ( String key in valueDictionary.Keys ) {
                parameters.Add( GetSqlParameter( key, valueDictionary[ key ] ) );
            }
            return parameters.ToArray( );
        }

        public static IEnumerable<SqlParameter> GetSqlParameters( params KeyValuePair<String, Object>[ ] keyValuePairs ) {
            List<SqlParameter> parameters = new List<SqlParameter>( );
            foreach ( var keyValue in keyValuePairs ) {
                parameters.Add( GetSqlParameter( keyValue.Key, keyValue.Value ) );
            }
            return parameters.ToArray( );
        }

        public static IEnumerable<SqlParameter> GetSqlParameters( Data.IDataObjectMapping dataObjectMapping, bool ensureExists ) {
            const String ident = "Identity";
            const String ensureParamName = "EnsureExists";
            List<SqlParameter> parameters = new List<SqlParameter>( );
            String parentParameterName = String.Format( "{0}{1}", dataObjectMapping.ParentDataObjectNameSingular, ident );
            String childParameterName = String.Format( "{0}{1}", dataObjectMapping.Metadata.DataObjectNameSingular, ident );
            Dictionary<String, Object> addlParams = dataObjectMapping.GetAdditionalValues( );

            parameters.Add( GetSqlParameter( parentParameterName, dataObjectMapping.GetParentIdentity( ) ) );
            parameters.Add( GetSqlParameter( childParameterName, dataObjectMapping.GetIdentity( ) ) );

            if ( addlParams != null ) {
                foreach ( String key in addlParams.Keys ) {
                    parameters.Add( GetSqlParameter( key, addlParams[ key ] ) );
                }
            }

            parameters.Add( GetSqlParameter( ensureParamName, ensureExists ) );

            return parameters.ToArray( );
        }

        public static SqlConnection GetSqlConnection( bool openConnection ) {
            SqlConnection connection = new SqlConnection( SqlServerHelper.ConnectionString );
            if ( openConnection )
                connection.Open( );
            return connection;
        }

        public static SqlCommand GetSqlCommand( String commandText, params SqlParameter[ ] parameters ) {
            SqlCommand sqlCommand = new SqlCommand( commandText );
            sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
            sqlCommand.Connection = SqlServerHelper.GetSqlConnection( false );
            if ( parameters != null && parameters.Any( ) )
                sqlCommand.Parameters.AddRange( parameters );
            return sqlCommand;
        }

        public static String PrepareCommandText<TDataObject>( CommonCommands command, params SqlParameter[ ] parameters )
            where TDataObject : class, Framework.Data.IDataObjectBase, new() {
            TDataObject dataObj = new TDataObject( );
            String dataObjName = dataObj.Metadata.DataObjectNameSingular;
            String action = command.ToString( ).Replace( "Record", String.Empty );
            String tailEnd = String.Empty;

            if ( command == CommonCommands.RecordList ) {
                dataObjName = dataObj.Metadata.DataObjectNamePlural;
                if ( parameters != null && parameters.Any( ) ) {
                    List<String> paramNames = new List<String>( );
                    foreach ( SqlParameter param in parameters ) {
                        paramNames.Add( param.ParameterName.Replace( "@", String.Empty ) );
                    }
                    tailEnd = String.Format( "By{0}", String.Join( String.Empty, paramNames.ToArray( ) ) );
                }
            }

            return String.Format( "[{0}].[{1}{2}{3}{4}]", dataObj.Metadata.DataObjectSchemaName, StoredProcedureNamePrefixSettingName, dataObjName, action, tailEnd );
        }

        public static String PrepareCommandText( Data.IDataObjectMapping dataObjectMapping ) {
            return String.Format( "[{0}].[{1}{2}{3}{4}]", dataObjectMapping.SchemaName, StoredProcedureNamePrefix, dataObjectMapping.ParentDataObjectNameSingular,
                dataObjectMapping.Metadata.DataObjectNameSingular, "AssertMapping" );
        }

        public static Result<int> ExecuteStatement( ref SqlCommand command ) {
            String action = String.Format( Actions.Error_FS, Actions.DbConnection );
            int count = 0;
            Result<int> result = null;

            try {
                if ( command.Connection.State != System.Data.ConnectionState.Open )
                    command.Connection.Open( );
                action = String.Format( Actions.Error_FS, Actions.DbExecutingCommand );
                count = command.ExecuteNonQuery( );
                result = Result.SuccessResult<int>( count );
            }
            catch ( Exception ex ) {
                result = Result.ErrorResult<int>( action, ex );
            }
            finally {
                if ( command.Connection.State != System.Data.ConnectionState.Closed )
                    command.Connection.Close( );
                command.Connection.Dispose( );
            }
            return result;
        }

        public static Result<System.Data.DataSet> ExecuteFill( ref SqlCommand command ) {
            String action = String.Format( Actions.Error_FS, Actions.DbConnection );
            int count = 0;
            Result<System.Data.DataSet> result = null;
            System.Data.DataSet dataSet = new System.Data.DataSet( );

            try {
                using (SqlDataAdapter dataAdapter = new SqlDataAdapter( command ) ) {
                    if ( command.Connection.State != System.Data.ConnectionState.Open )
                        command.Connection.Open( );
                    action = String.Format( Actions.Error_FS, Actions.DbExecutingCommand );
                    dataAdapter.Fill( dataSet );
                    result = Result.SuccessResult<System.Data.DataSet>( dataSet );
                }
            }
            catch (Exception ex) {
                result = Result.ErrorResult<System.Data.DataSet>( action, ex );
            }
            finally {
                if ( command.Connection.State != System.Data.ConnectionState.Closed )
                    command.Connection.Close( );
                command.Connection.Dispose( );
            }

            return result;
        }

        #endregion Methods

        private static String ConnectionString {
            get {
                if ( String.IsNullOrWhiteSpace( SqlServerHelper._ConnectionString ) )
                    throw new InvalidOperationException( String.Format( ErrorMessages.NoConnectionString_FS, typeof( SqlServerHelper ).FullName ) );
                return SqlServerHelper._ConnectionString;
            }
            set { SqlServerHelper._ConnectionString = value; }
        }

        private static String StoredProcedureNamePrefix {
            get { return _StoredProcedureNamePrefix; }
            set { _StoredProcedureNamePrefix = value; }
        }

    }

}
