//
//  Do you like this project? Do you find it helpful? Pay it forward by hiring me as a consultant!
//  https://jason-iverson.com
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JM0ney.Framework.Database {
    
    /// <summary>
    /// Common commands that can be executed by all IDataAdapter
    /// </summary>
    public enum CommonCommands : byte {

        DeleteRecord = 0,
        RecordList = 1,
        LoadRecord = 2,
        SaveRecord = 3

    }


}
