//node "mongodb_monitor_performance.js" "2372" "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" "192.168.69.3" "27017" """" """"
	
var fs = require('fs');

var tempDir = "/tmp";

var metricsId =[];

metricsId["opcounters.insert"] =  {id:"103:4",ratio:true}; // # of inserts per second (* means replicated op).
metricsId["opcounters.query"] =  {id:"29:4",ratio:true}; // # of queries per second.
metricsId["opcounters.update"] =  {id:"186:4",ratio:true}; // # of updates per second.
metricsId["opcounters.delete"] =  {id:"214:4",ratio:true}; // # of deletes per second.
metricsId["opcounters.getmore"] =  {id:"183:4",ratio:true};	 // # of get mores (cursor batch) per second.
metricsId["opcounters.command"] = {id:"55:4",ratio:true}; // # of commands per second, on a slave its local|replicated.
metricsId["connections.current"] = {id:"106:4",ratio:false}; // Number of open connections.
metricsId["extra_info.page_faults"] =  {id:"175:4",ratio:true}; // # of pages faults per sec (linux only).
metricsId["backgroundFlushing.flushes"] =  {id:"189:4",ratio:true}; // # of fsync flushes per second.
metricsId["globalLock.activeClients.total"] =  {id:"139:4",ratio:false}; // Active clients (read and write).
metricsId["globalLock.currentQueue.total"] =  {id:"155:4",ratio:false}; // The current number of operations queued waiting for the global lock.
metricsId["network.bytesIn"] =  {id:"14:4",ratio:true}; // Network traffic in - bits.
metricsId["network.bytesOut"] =  {id:"113:4",ratio:true}; // Network traffic out - bits.

metricsId["mem.mapped"] =  {id:"180:4",ratio:false}; // Amount of data mmaped (total data size) megabytes.
metricsId["mem.virtual"] =  {id:"22:4",ratio:false}; // Virtual size of process in megabytes.
metricsId["mem.resident"] = {id:"19:4",ratio:false}; // Resident size of process in megabytes.


metricsId["GLOBAL_LOCK_RATIO_UUID"] =  {id:"95:6",ratio:false}; // Percent of time in global write lock.
metricsId["BTREE_MISS_PAGE_RATIO_UUID"] = {id:"8:6",ratio:false}; // Percent of btree page misses (sampled).


metricsLength = 18;

var mongodb = "local";
var dbcommand = "serverStatus";

//####################### EXCEPTIONS ################################

function InvalidParametersNumberError() {
    this.name = "InvalidParametersNumberError";
    this.message = ("Wrong number of parameters.");
}
InvalidParametersNumberError.prototype = Object.create(Error.prototype);
InvalidParametersNumberError.prototype.constructor = InvalidParametersNumberError;

function InvalidMetricStateError() {
    this.name = "InvalidMetricStateError";
    this.message = ("Invalid number of metrics.");
}
InvalidMetricStateError.prototype = Object.create(Error.prototype);
InvalidMetricStateError.prototype.constructor = InvalidMetricStateError;

function InvalidAuthenticationError() {
    this.name = "InvalidAuthenticationError";
    this.message = ("Invalid authentication.");
}
InvalidAuthenticationError.prototype = Object.create(Error.prototype);
InvalidAuthenticationError.prototype.constructor = InvalidAuthenticationError;

function DatabaseConnectionError(message) {
	this.name = "DatabaseConnectionError";
    this.message = message;
}
DatabaseConnectionError.prototype = Object.create(Error.prototype);
DatabaseConnectionError.prototype.constructor = DatabaseConnectionError;

function CreateTmpDirError(message)
{
	this.name = "CreateTmpDirError";
    this.message = message;
}
CreateTmpDirError.prototype = Object.create(Error.prototype);
CreateTmpDirError.prototype.constructor = CreateTmpDirError;


function WriteOnTmpFileError(message)
{
	this.name = "WriteOnTmpFileError";
    this.message = message;
}
WriteOnTmpFileError.prototype = Object.create(Error.prototype);
WriteOnTmpFileError.prototype.constructor = WriteOnTmpFileError;

// ############# INPUT ###################################

(function() {
	try
	{
		monitorInput(process.argv.slice(2));
	}
	catch(err)
	{	
		if(err instanceof InvalidParametersNumberError)
		{
			console.log(err.message);
			process.exit(3);
		}
		else if(err instanceof InvalidMetricStateError)
		{
			console.log(err.message);
			process.exit(9);
		}
		else if(err instanceof InvalidAuthenticationError)
		{
			console.log(err.message);
			process.exit(2);
		}
		else if(err instanceof DatabaseConnectionError)
		{
			console.log(err.message);
			process.exit(11);
		}
		else
		{
			console.log(err.message);
			process.exit(1);
		}
	}
}).call(this)



function monitorInput(args)
{
	if(args.length === 6)
	{
		monitorInputProcess(args);
	}
	else
	{
		throw new InvalidParametersNumberError()
	}
}


function monitorInputProcess(args)
{
	//"2372" "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" "192.168.69.3" "27017" """" """"

	//host
	var hostname = args[2];
	
	//target
	var targetUUID = args[0];
	
	//metric state
	var metricState = args[1].replace("\"", "");
	
	var tokens = metricState.split(",");

	var metricsExecution = new Array(metricsLength);

	if (tokens.length === metricsLength)
	{
		for(var i in tokens)
		{
			metricsExecution[i] = (tokens[i] === "1")
		}
	}
	else
	{
		throw new InvalidMetricStateError();
	}
	
	//port
	var port = args[3];
	
	
	// Username
	var username = args[4];
	
	username = username.length === 0 ? "" : username;
	username = username === "\"\"" ? "" : username;
	if(username.length === 1 && username === "\"")
		username = "";
	
	// Password
	var passwd = args[5];
	
	passwd = passwd.length === 0 ? "" : passwd;
	passwd = passwd === "\"\"" ? "" : passwd;
	if(passwd.length === 1 && passwd === "\"")
		passwd = "";
	
	
	var connectionURI = "";

	if (username.length === 0)
	{
		if (port.length === 0)
		{
			connectionURI = "mongodb://" + hostname;
		}
		else
		{
			connectionURI = "mongodb://" + hostname + ":" + port;
		}
	}
	else
	{
		if (port.length === 0)
		{
			connectionURI = "mongodb://" + username + ":" + passwd + "@" + hostname;
		}
		else
		{
			connectionURI = "mongodb://" + username + ":" + passwd + "@" + hostname + ":" + port;
		}
	}
	
	
	
	var requests = []
	
	var request = new Object();
	request.connectionURI = connectionURI+"/"+mongodb;
	request.targetUUID = targetUUID;
	request.metricsExecution = metricsExecution;
	
	requests.push(request)

	//console.log(JSON.stringify(requests));
	
	monitorDatabasePerformance(requests);
	
}




//################### OUTPUT ###########################

function output(metrics, targetId)
{
	for(var i in metrics)
	{
		var out = "";
		var metric = metrics[i];
		
		out += new Date(metric.ts).toISOString();
		out += "|";
		out += metric.id;
		out += "|";
		out += targetId;
		out += "|";
		out += metric.val;
		
		console.log(out);
	}
	
	
}


function errorHandler(err)
{
	if(err instanceof InvalidAuthenticationError)
	{
		console.log(err.message);
		process.exit(2);
	}
	else if(err instanceof DatabaseConnectionError)
	{
		console.log(err.message);
		process.exit(11);
	}
	else if(err instanceof CreateTmpDirError)
	{
		console.log(err.message);
		process.exit(21);
	}
	else if(err instanceof WriteOnTmpFileError)
	{
		console.log(err.message);
		process.exit(22);
	}
	else
	{
		console.log(err.message);
		process.exit(1);
	}
}


// ################# MONITOR ###########################
function monitorDatabasePerformance(requests) 
{
	var mongodb = require('mongodb');
	
	var MongoClient = mongodb.MongoClient
	
	for(var i in requests)
	{
		var request = requests[i];
		
		MongoClient.connect(request.connectionURI, function(err, db) {

			if (err && err.message === "auth failed") 
			{
				errorHandler(new InvalidAuthenticationError());
			}
			else if(err)
			{
				errorHandler(new DatabaseConnectionError(err.message));
			}
			
			
			db.command({serverStatus:1}, function(err, result) {
				
				if(err)
				{
					errorHandler(new DatabaseConnectionError(err.message));
				}
				
				
				//console.log(JSON.stringify(result))
				
				var metricsName = Object.keys(metricsId);
				
				var jsonString = "[";
				
				var dateTime = new Date().toISOString();
				
				for(var i in metricsName)
				{
					if(request.metricsExecution[i])
					{	
						var path = metricsName[i].split("\.")
						//console.log(result[path[0]][path[1]]);
						
						if(path.length > 2 && typeof result[path[0]][path[1]][path[2]] != 'undefined')
						{
							//console.log(result[path[0]][path[1]][path[2]])
							//console.log(metricsName[i] + ": "+result[path[0]][path[1]][path[2]]);
							jsonString += "{";
								
							jsonString += "\"variableName\":\""+metricsName[i]+"\",";
							jsonString += "\"metricUUID\":\""+metricsId[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ result[path[0]][path[1]][path[2]] +"\"";
							
							jsonString += "},";
						}
						else if(path.length > 1 && typeof result[path[0]][path[1]] != 'undefined')
						{
							//console.log(metricsName[i] + ": "+result[path[0]][path[1]]);
							
							jsonString += "{";
								
							jsonString += "\"variableName\":\""+metricsName[i]+"\",";
							jsonString += "\"metricUUID\":\""+metricsId[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ result[path[0]][path[1]] +"\"";
							
							jsonString += "},";
						}
						else
						{
							var value = 0;
							if(metricsName[i] === "GLOBAL_LOCK_RATIO_UUID")
							{
								//locktime / totaltime
								var locktime = result.globalLock.lockTime;
								var totaltime = result.globalLock.totalTime;
								
								if(typeof locktime != 'undefined' && typeof totaltime != 'undefined' && totaltime != 0)
								{
									value = (locktime / totaltime).toFixed(2);
								}
							}
							else if(metricsName[i] === "BTREE_MISS_PAGE_RATIO_UUID")
							{
								var missRatio = result.indexCounters.missRatio;
								
								if(typeof missRatio != 'undefined')
									value = missRatio.toFixed(2);
							}
							
							jsonString += "{";
								
							jsonString += "\"variableName\":\""+metricsName[i]+"\",";
							jsonString += "\"metricUUID\":\""+metricsId[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ value +"\"";
							
							jsonString += "},";
						}
					}
				}
				
				if(jsonString.length > 1)
					jsonString = jsonString.slice(0, jsonString.length-1);
				
				jsonString += "]";
				
				//console.log(jsonString)
				
				processDeltas(request, jsonString);
				
				db.close();

			});
		});
	}
}


function processDeltas(request, results)
{
	var file = getFile(request.targetUUID);
	
	var toOutput = [];
	
	if(file)
	{		
		var previousData = JSON.parse(file);
		var newData = JSON.parse(results);
			
		for(var i = 0; i < newData.length; i++)
		{
			var endMetric = newData[i];
			var initMetric = null;
			
			for(var j = 0; j < previousData.length; j++)
			{
				if(previousData[j].metricUUID === newData[i].metricUUID)
				{
					initMetric = previousData[j];
					break;
				}
			}
			
			if (initMetric != null)
			{
				var deltaValue = getDelta(initMetric, endMetric, request);
				
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = deltaValue;
				
				toOutput.push(rateMetric);
			}
			else
			{	
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = 0;
				
				toOutput.push(rateMetric);
			}
		}
		
		setFile(request.targetUUID, results);

		for (var m = 0; m < toOutput.length; m++)
		{
			for (var z = 0; z < newData.length; z++)
			{
				var systemMetric = metricsId[newData[z].variableName];
				
				if (systemMetric.ratio === false && newData[z].metricUUID === toOutput[m].id)
				{
					toOutput[m].value = newData[z].value;
					break;
				}
			}
		}

		processOutput(request, toOutput)
		
	}
	else
	{
		setFile(request.targetUUID, results);
		process.exit(0);
	}
}



function processOutput(request, toOutput)
{
	var date = new Date().toISOString();

	for(var i in toOutput)
	{
		var output = "";
		
		output += date + "|";
		output += toOutput[i].id + "|";
		output += request.targetUUID + "|";
		
		var value = 0;
		
		if (toOutput[i].id === "14:4" || toOutput[i].id === "113:4")
		{
			value = (parseFloat(toOutput[i].value) / 8 / 1024).toFixed(2);
		}
		else
		{
			value = toOutput[i].value
		}
		
		output += value;
		
		console.log(output);
	}
}



function getDelta(initMetric, endMetric, request)
{
	var deltaValue = 0;

	var decimalPlaces = 2;

	var date = new Date().toISOString();
	
	if (parseFloat(endMetric.value) < parseFloat(initMetric.value))
	{	
		deltaValue = parseFloat(endMetric.value).toFixed(decimalPlaces);
	}
	else
	{	
		var elapsedTime = (new Date(endMetric.timestamp).getTime() - new Date(initMetric.timestamp).getTime()) / 1000;	
		deltaValue = ((parseFloat(endMetric.value) - parseFloat(initMetric.value))/elapsedTime).toFixed(decimalPlaces);
	}
	
	return deltaValue;
}





//########################################

function getFile(monitorId)
{
		var dirPath =  __dirname +  tempDir + "/";
		var filePath = dirPath + ".mongodb_"+ monitorId+".dat";
		
		try
		{
			fs.readdirSync(dirPath);
			
			var file = fs.readFileSync(filePath, 'utf8');
			
			if (file.toString('utf8').trim())
			{
				return file.toString('utf8').trim();
			}
			else
			{
				return null;
			}
		}
		catch(e)
		{
			return null;
		}
}

function setFile(monitorId, json)
{
	var dirPath =  __dirname +  tempDir + "/";
	var filePath = dirPath + ".mongodb_"+ monitorId+".dat";
		
	if (!fs.existsSync(dirPath)) 
	{
		try
		{
			fs.mkdirSync( __dirname+tempDir);
		}
		catch(e)
		{
			errorHandler(new CreateTmpDirError(e.message));
		}
	}

	try
	{
		fs.writeFileSync(filePath, json);
	}
	catch(err)
	{
		errorHandler(new WriteOnTmpFileError(err.message));
	}
}
