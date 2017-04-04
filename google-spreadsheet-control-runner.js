const async = require('async');
const GoogleSpreadsheet = require('google-spreadsheet');

function isRowProcessable(row) {
    return row.progress === 'NOT_STARTED' && row.skippable === 'FALSE';
}

function fallbackRowProcessor(row) {
    return (step) => {
        console.log(`Skipping row: ${row.id}`);
        if (row.progress === 'NOT_STARTED') {
            row.progress = 'SKIPPED';
            row.save();
        }
        step(null, `Skipped row: ${row.id}`);
    }
}

function getControlSteps(spreadsheetUUID, GoogleCreds, startingRow, numberOfRows, providedRowProcessor) {
    const doc = new GoogleSpreadsheet(spreadsheetUUID);
    return [
        (step) => doc.useServiceAccountAuth(GoogleCreds, step),
        (step) => doc.getInfo((error, info) => {
            console.log(`Loaded doc: ${info.title}`);
            console.log(`Number of worksheets: ${info.worksheets.length}. Only processing first worksheet.`);
            const sheet = info.worksheets[0];
            console.log(`Sheet 1: ${sheet.title} ${sheet.rowCount}x${sheet.colCount}`);
            step(null, sheet);
        }),
        (sheet, step) => {
            sheet.getRows({
                offset: startingRow,
                limit: numberOfRows
            }, (error, rows) => {
                console.log(`Processing ${rows.length} rows`);
                async.series(rows.map(row => isRowProcessable(row) ? providedRowProcessor(row) : fallbackRowProcessor(row)), step);
            });
        },
        (results, step) => {
            console.log(`Processed ${results.length} rows`);
            step(null, results);
        }
    ];
}

module.exports = (spreadsheetUUID, rowProcessor, errorResultsCallback, startingRow, numberOfRows, googleCredsLocation) => {
    let allGood = true;
    console.log('...INITIALISING GOOGLE SPREADSHEET CONTROL RUNNER...');

    if (!spreadsheetUUID) {
        console.error("Please provide a Google Spreadsheet ID as the first argument. The first worksheet must have a column " +
            "labelled 'progress'. For a row to be processed, the 'progress' cell must have a value of 'NOT_STARTED'");
        allGood = false;
    }
    if (!rowProcessor) {
        console.error("Please provide a row processing curried function as the second argument. It should take a row and return an async.js function to be executed in time which actually processes the row.");
        allGood = false;
    }
    if (!errorResultsCallback) {
        console.error("Please provide a callback function (error, results) as the third argument to receive notification when the processing is complete.");
        allGood = false;
    }
    if (!startingRow) {
        console.warn("Starting at row 1. To start at a different row, please provide a positive integer as the fourth argument.");
        startingRow = 1;
    }
    if (!numberOfRows) {
        console.warn("Will process 1 row only. To process more rows, please provide a positive integer as the fifth argument.");
        numberOfRows = 1;
    }
    if (!googleCredsLocation) {
        console.warn("Reading Google credentials from: './google-creds.json'. Please provide an alternative path as the sixth and final argument.");
        googleCredsLocation = './google-creds.json';
    }
    if (allGood) {
        console.log('...PROCESSING GOOGLE SPREADSHEET...');
        const GoogleCreds = require(googleCredsLocation);
        const steps = getControlSteps(spreadsheetUUID, GoogleCreds, startingRow, numberOfRows, rowProcessor);
        async.waterfall(steps, (error, results) => {
            console.log('...PROCESSING COMPLETE. CALLING BACK...');
            errorResultsCallback(error, results);
        });
        console.log('...INTIALISATION COMPLETE. PLEASE WAIT FOR CALLBACK.');
    } else {
        console.log('GOOGLE SPREADSHEET CONTROL RUNNER FINISHED.');
    }

}
