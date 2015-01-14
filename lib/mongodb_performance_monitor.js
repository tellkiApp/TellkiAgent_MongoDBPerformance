//node "mongodb_monitor_performance.js" "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" "192.168.69.3" "27017" """" """"
	
var fs = require('fs');

var tempDir = "/tmp";

var metrics =[];

metrics["opcounters.insert"] =  {id:"103:Inserts/Sec:4",ratio:true}; // # of inserts per second (* means replicated op).
metrics["opcounters.query"] =  {id:"29:Queries/Sec:4",ratio:true}; // # of queries per second.
metrics["opcounters.update"] =  {id:"186:Updates/Sec:4",ratio:true}; // # of updates per second.
metrics["opcounters.delete"] =  {id:"214:Deletes/Sec:4",ratio:true}; // # of deletes per second.
metrics["opcounters.getmore"] =  {id:"183:Get mores/Sec:4",ratio:true};     // # of get mores (cursor batch) per second.
metrics["opcounters.command"] = {id:"55:Commands/Sec:4",ratio:true}; // # of commands per second, on a slave its local|replicated.
metrics["connections.current"] = {id:"106:Open connections:4",ratio:false}; // Number of open connections.
metrics["extra_info.page_faults"] =  {id:"175:Page Faults/Sec:4",ratio:true}; // # of pages faults per sec (linux only).
metrics["backgroundFlushing.flushes"] =  {id:"189:Fsync flushes/Sec:4",ratio:true}; // # of fsync flushes per second.
metrics["globalLock.activeClients.total"] =  {id:"139:Active clients:4",ratio:false}; // Active clients (read and write).
metrics["globalLock.currentQueue.total"] =  {id:"155:Queued Operations:4",ratio:false}; // The current number of operations queued waiting for the global lock.
metrics["network.bytesIn"] =  {id:"14:Network traffic in:4",ratio:true}; // Network traffic in - bits.
metrics["network.bytesOut"] =  {id:"113:Network traffic out:4",ratio:true}; // Network traffic out - bits.
metrics["mem.mapped"] =  {id:"180:Data mmaped:4",ratio:false}; // Amount of data mmaped (total data size) megabytes.
metrics["mem.virtual"] =  {id:"22:Process Virtual size:4",ratio:false}; // Virtual size of process in megabytes.
metrics["mem.resident"] = {id:"19:Process Resident size:4",ratio:false}; // Resident size of process in megabytes.
metrics["GLOBAL_LOCK_RATIO_UUID"] =  {id:"95:% Time in global write lock:6",ratio:false}; // Percent of time in global write lock.
metrics["BTREE_MISS_PAGE_RATIO_UUID"] = {id:"8:% Btree page misses:6",ratio:false}; // Percent of btree page misses (sampled).

metricsLength = 18;

var mongodb = "local";
var dbcommand = "serverStatus";

//####################### EXCEPTIONS ################################

function InvalidParametersNumberError() {
    this.name = "InvalidParametersNumberError";
    this.message = "Wrong number of parameters.";
	this.code = 3;
}
InvalidParametersNumberError.prototype = Object.create(Error.prototype);
InvalidParametersNumberError.prototype.constructor = InvalidParametersNumberError;

function InvalidMetricStateError() {
    this.name = "InvalidMetricStateError";
    this.message = "Invalid number of metrics.";
	this.code = 9;
}
InvalidMetricStateError.prototype = Object.create(Error.prototype);
InvalidMetricStateError.prototype.constructor = InvalidMetricStateError;

function InvalidAuthenticationError() {
    this.name = "InvalidAuthenticationError";
    this.message = "Invalid authentication.";
	this.code = 2;
}
InvalidAuthenticationError.prototype = Object.create(Error.prototype);
InvalidAuthenticationError.prototype.constructor = InvalidAuthenticationError;

function DatabaseConnectionError() {
	this.name = "DatabaseConnectionError";
    this.message = "";
	this.code = 11;
}
DatabaseConnectionError.prototype = Object.create(Error.prototype);
DatabaseConnectionError.prototype.constructor = DatabaseConnectionError;

function CreateTmpDirError()
{
	this.name = "CreateTmpDirError";
    this.message = "";
	this.code = 21;
}
CreateTmpDirError.prototype = Object.create(Error.prototype);
CreateTmpDirError.prototype.constructor = CreateTmpDirError;


function WriteOnTmpFileError()
{
	this.name = "WriteOnTmpFileError";
    this.message = "";
	this.code = 22;
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
			process.exit(err.code);
		}
		else if(err instanceof InvalidMetricStateError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof InvalidAuthenticationError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof DatabaseConnectionError)
		{
			console.log(err.message);
			process.exit(err.code);
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
	if(args.length === 5)
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

	//host
	var hostname = args[1];
	
	//metric state
	var metricState = args[0].replace("\"", "");
	
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
	var port = args[2];
	
	
	// Username
	var username = args[3];
	
	username = username.length === 0 ? "" : username;
	username = username === "\"\"" ? "" : username;
	if(username.length === 1 && username === "\"")
		username = "";
	
	// Password
	var passwd = args[4];
	
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
	request.metricsExecution = metricsExecution;
	request.hostname = hostname;
	request.port = port
	
	requests.push(request)

	
	monitorDatabasePerformance(requests);
	
}




//################### OUTPUT ###########################

function output(toOutput)
{
	for(var i in toOutput)
	{
		var out = "";
		
		out += toOutput[i].id + "|";
	
		var value = 0;
		
		if (toOutput[i].id === "14:Network traffic in:4" || toOutput[i].id === "113:Network traffic out:4")
		{
			value = (parseFloat(toOutput[i].value) / 8 / 1024).toFixed(2);
		}
		else
		{
			value = toOutput[i].value
		}
		
		out += value;
		out += "|";
		
		console.log(out);
	}
}


function errorHandler(err)
{
	if(err instanceof InvalidAuthenticationError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof DatabaseConnectionError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof CreateTmpDirError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof WriteOnTmpFileError)
	{
		console.log(err.message);
		process.exit(err.code);
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
				var e = new DatabaseConnectionError();
				e.message = err.message;
				errorHandler(e);
			}
			
			
			db.command({serverStatus:1}, function(err, result) {
				
				if(err)
				{
					var e = new DatabaseConnectionError();
					e.message = err.message;
					errorHandler(e);
				}
				
				
				var metricsName = Object.keys(metrics);
				
				var jsonString = "[";
				
				var dateTime = new Date().toISOString();
				
				for(var i in metricsName)
				{
					if(request.metricsExecution[i])
					{	
						var path = metricsName[i].split("\.")
						
						if(path.length > 2 && typeof result[path[0]][path[1]][path[2]] != 'undefined')
						{
							jsonString += "{";
								
							jsonString += "\"variableName\":\""+metricsName[i]+"\",";
							jsonString += "\"metricUUID\":\""+metrics[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ result[path[0]][path[1]][path[2]] +"\"";
							
							jsonString += "},";
						}
						else if(path.length > 1 && typeof result[path[0]][path[1]] != 'undefined')
						{
							jsonString += "{";
								
							jsonString += "\"variableName\":\""+metricsName[i]+"\",";
							jsonString += "\"metricUUID\":\""+metrics[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ result[path[0]][path[1]] +"\"";
							
							jsonString += "},";
						}
						else
						{
							var value = 0;
							if(metricsName[i] === "GLOBAL_LOCK_RATIO_UUID")
							{
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
							jsonString += "\"metricUUID\":\""+metrics[metricsName[i]].id+"\",";
							jsonString += "\"timestamp\":\""+ dateTime +"\",";
							jsonString += "\"value\":\""+ value +"\"";
							
							jsonString += "},";
						}
					}
				}
				
				if(jsonString.length > 1)
					jsonString = jsonString.slice(0, jsonString.length-1);
				
				jsonString += "]";
				
				
				processDeltas(request, jsonString);
				
				db.close();

			});
		});
	}
}


function processDeltas(request, results)
{
	var file = getFile(request.hostname, request.port);
	
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
		
		setFile(request.hostname, request.port, results);

		for (var m = 0; m < toOutput.length; m++)
		{
			for (var z = 0; z < newData.length; z++)
			{
				var systemMetric = metrics[newData[z].variableName];
				
				if (systemMetric.ratio === false && newData[z].metricUUID === toOutput[m].id)
				{
					toOutput[m].value = newData[z].value;
					break;
				}
			}
		}

		output(toOutput)
		
	}
	else
	{
		setFile(request.hostname, request.port, results);
		process.exit(0);
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

function getFile(hostname, port)
{
		var dirPath =  __dirname +  tempDir + "/";
		var filePath = dirPath + ".mongodb_"+ hostname +"_"+ port +".dat";
		
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

function setFile(hostname, port, json)
{
	var dirPath =  __dirname +  tempDir + "/";
	var filePath = dirPath + ".mongodb_"+ hostname +"_"+ port +".dat";
		
	if (!fs.existsSync(dirPath)) 
	{
		try
		{
			fs.mkdirSync( __dirname+tempDir);
		}
		catch(e)
		{
			var ex = new CreateTmpDirError();
			ex.message = e.message;
			errorHandler(ex);
		}
	}

	try
	{
		fs.writeFileSync(filePath, json);
	}
	catch(err)
	{
		var ex = new WriteOnTmpFileError();
		ex.message = err.message;
		errorHandler(ex);
	}
}
