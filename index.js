const async = require('async');
const moment = require('moment');
const request = require('request');
const runner = require('./google-spreadsheet-control-runner');

const zuoraBaseUrl = ''
const zuoraUsername = '';
const zuoraPassword = ''; 

const zuoraAuthHeaders = {
    apiAccessKeyId: zuoraUsername,
    apiSecretAccessKey: zuoraPassword
};

function isSuccess(res) {
	return (res.statusCode == 200 && res.body.success);
}
function updateAccount(accountid, crmid) {
    return (asyncCallback) => request({
        url: `${zuoraBaseUrl}/rest/v1/accounts/${accountid}`,
        method: 'PUT',
        headers: zuoraAuthHeaders,
        json: true,
        body : {"crmId" : crmid}
    }, (error, response) => {
    	if (error) {
  		    asyncCallback(`error while updating account: ${error}.`);
    	} else if(isSuccess(response)) {
            asyncCallback(null,response.body);
        } else {
            asyncCallback(JSON.stringify(response.body));

        }
    });
}

function setRowProgress(row, state) {
	row.progress = state;
    row.save();
}

function processSubscription(spreadsheetRow) {
   console.log(`processing ${spreadsheetRow.accountid}`); 
   return (step) => {
   	 setRowProgress(spreadsheetRow, 'PROCESSING');
   	 function handleResponse(error, res) {
   	 	if(error){
     		console.error(`ERROR => for account ${spreadsheetRow.accountid} error response : ${error}.`)
     		setRowProgress(spreadsheetRow, 'FAILED');
   	  	} else {
     		console.log(`for account ${spreadsheetRow.accountid} response: ${JSON.stringify(res)}.`)
     		setRowProgress(spreadsheetRow, 'DONE');
     	}
 	step();
   	 }
     updateAccount(spreadsheetRow.accountid, spreadsheetRow.crmid)(handleResponse)
   }
}

function endCallback(error, results) {
	if (error) {
		console.error(error)
	}
	console.log(`Processed ${results.length} subscriptions`)
}

runner('[SPREADSHEET_ID]', processSubscription, endCallback, 1, 57);
