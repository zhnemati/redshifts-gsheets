//Limitation to include secret values in the code since appscript does not support default aws jdk and all requests and their auth headers are hashed, that requires secret and access key.
var secretkey="<Enter AWS secret key>" 
var accesskey="<Enter AWS access key>"
AWS.init(secretkey, accesskey);

 function runGetSecretValue_(secretId) {
 
  var resultJson =  AWS.request(
    	"secretsmanager",
    	"<AWS REGION>",
    	'secretsmanager.GetSecretValue',
    	{"Version": "997cd326-0af7-4a10-bc1d-026645ea49e9"},
    	method='POST',
    	payload={          
      	"SecretId" : secretId
    	},
    	headers={
      	"X-Amz-Target": "secretsmanager.GetSecretValue",
      	"Content-Type": "application/x-amz-json-1.1"
    	}
  );
 
  return JSON.parse(JSON.parse(resultJson).SecretString);

}
 function describe(id) {
  var secrets=runGetSecretValue_("<AWS SECRET NAME WITH CREDENTIALS FOR REDSHIFT CLUSTER>")
  var resultJson = AWS.request(
    	"redshift-data",
    	"<AWS REGION>",
    	'RedshiftData.DescribeStatement',
    	{"Version": " "},
    	method='POST',
    	payload={
      	"ClusterIdentifier": secrets.dbClusterIdentifier,
      	"Database":secrets.database,
      	"DbUser": secrets.username,
        "includeResultMetadata": 1,
      	"Id": id
    	},
    	headers={
      	"X-Amz-Target": "RedshiftData.ExecuteStatement",
      	"Content-Type": "application/x-amz-json-1.1"
    	}
    
  );
  return JSON.parse(resultJson)
} 
  function runExecuteStatement_(sql) {
  var secrets=runGetSecretValue_("<AWS SECRET NAME WITH CREDENTIALS FOR REDSHIFT CLUSTER>")
  var resultJson =  AWS.request(
    	"redshift-data",
    	"<AWS REGION>",
    	'RedshiftData.ExecuteStatement',
    	{"Version": "2.2.47"},
    	method='POST',
    	payload={
      	"ClusterIdentifier": secrets.dbClusterIdentifier,
      	"Database":secrets.database,
      	"DbUser": secrets.username,
        "includeResultMetadata": 1,
      	"Sql": sql
    	},
    	headers={
      	"X-Amz-Target": "RedshiftData.ExecuteStatement",
      	"Content-Type": "application/x-amz-json-1.1"
    	}
  ); 
 
  return JSON.parse(resultJson)
}

 function  runrecords(Id,Token=' ') {
 if(Token===' ') {
 
  var resultJson =  AWS.request(
    	"redshift-data",
    	"<AWS REGION>",
    	'RedshiftData.GetStatementResult',
    	{"Version": "2.2.47"},
    	method='POST',
    	payload={          
      	"Id" : Id
        },
    	headers={
      	"X-Amz-Target": "RedshiftData.GetStatementResult",
      	"Content-Type": "application/x-amz-json-1.1"
    	}
  );
 }
else
{
    Logger.log("Token passed is " + Token)
    var resultJson =  AWS.request(
    	"redshift-data",
    	"<AWS REGION>",
    	'RedshiftData.GetStatementResult',
    	{"Version": "2.2.47"},
    	method='POST',
    	payload={          
      	"Id" : Id,
        "NextToken":Token

    	},
    	headers={
      	"X-Amz-Target": "RedshiftData.GetStatementResult",
      	"Content-Type": "application/x-amz-json-1.1"
    	}
  );
}
return JSON.parse(resultJson);
 
}

var parser_function = function(data_unwrapped) 
{
var totalRows = data_unwrapped.Records.length;
var keys = [];
var values = [];
var col_names =[];
var final = [];

for (var i=0; i<totalRows;i++) {
  var currentRecordData = data_unwrapped.Records[i];
  try {
      currentRecordData.forEach(obj => {
    keys.push(...Object.keys(obj));
    values.push(...Object.values(obj));
  })} 
catch (e) {
  Logger.log(e)
  continue;
}}
for (let i = 0; i < data_unwrapped.ColumnMetadata.length; i++) {
  col_names.push(data_unwrapped.ColumnMetadata[i].name)
}
var modifedArrtemp = values.map(val => val === true ? ' ' : val);
var modifedArr=modifedArrtemp.map(val=> typeof val !=="string" ? val.toString() : val);
for(var i = 0; i<values.length; i+=col_names.length) {
  final.push(modifedArr.slice(i,i+col_names.length));
}
return {k: keys, v: values,col:col_names,row:final}
}

function write_column_names_to_sheet(id){
var data_run_temp = runrecords(id);
data=parser_function(data_run_temp)
var someSheet = SpreadsheetApp.getActiveSpreadsheet().getSheets()[0];
someSheet.appendRow(data.col);
Logger.log("Columns writing completed")

}
function write_rows(results,row_num) { 
output=parser_function(results)
var row_num=getFirstEmptyRowByColumnArray()
var mainSheet = SpreadsheetApp.getActiveSheet();
  range=mainSheet.getRange(row_num,1,results.Records.length,output.col.length);
  range.setValues(output.row);
Logger.log("Rows writing completed")
 
}
function getFirstEmptyRowByColumnArray() {
  var spr = SpreadsheetApp.getActiveSpreadsheet();
  var column = spr.getRange('A:A');
  var values = column.getValues(); 
  var ct = 0;
  while ( values[ct] && values[ct][0] != "" ) {
    ct++;
  }
  return (ct+1);
}

function get_data_after_wait(results) {
while(true)
    {
        var data=describe(results.Id)
        Logger.log('Status is ' + data.Status)
        if(data.Status==='FINISHED'){
            var identifier=data.Id
            break;
        }
        Logger.log("Waiting for Status to be Finished")
        Utilities.sleep(5000)
    }
    return identifier
}

function main_execution_procedure(sql_query) {
    var token=' '
    var exec_data= runExecuteStatement_(sql_query);
    var identifier=get_data_after_wait(exec_data)
    write_column_names_to_sheet(identifier);
    var data_run_temp =''
    while (true)
    {
        data_run_temp = runrecords(identifier,token);
        
        if (typeof data_run_temp.NextToken=="string")
            {
            write_rows(data_run_temp);
            token=data_run_temp.NextToken
            }
        else if (typeof data_run_temp.NextToken!=="string") 
        {
            write_rows(data_run_temp);
            break;
        }
    }
    
    }