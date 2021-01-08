const fs = require('fs');
const ClickHouse = require('@apla/clickhouse');
const Batch = require('stream-json/utils/Batch');
const StreamArray = require('stream-json/streamers/StreamArray');
const {chain} = require('stream-chain');

const CH = new ClickHouse({ host: 'localhost', port: '8123', user: 'default', password: 'hiclick10' })
let path = 'C:/Users/mne21/Desktop/array.json';
const maxArrLen = 300;
let recDbStream;
let cntPush = 1;

const pipeline = chain([
    fs.createReadStream(path,{encoding: 'utf8'}),
    StreamArray.withParser(),
    new Batch({batchSize: maxArrLen})
]);


pipeline.on('data', data => {
    cntPush = cntPush + maxArrLen;
    let objArrForDb = GetDataArr(data);
    let str = '';
    let i;
    for (i = 0; i < objArrForDb.length; i++) {
        str = str + JSON.stringify(objArrForDb[i]) + ' ';
    } 

    recDbStream = CreateDbStream();
    recDbStream.write(str);
    recDbStream.end();
    console.log('Insert part complete' + '__' + cntPush)
});


pipeline.on('end', () => console.log('All done!'));
pipeline.on('error', error => console.log(error));
//------------------------------------------------------------------------------------------
function CreateDbStream() {
    const writableStream = CH.query(`INSERT INTO WellData.SearchRes`, { format: 'JSONEachRow' }, (err) => {
        if (err) {
            console.error(err)
        }
    })
    return writableStream;
}

function GetDataArr(data) {
    let arrObj = [];

    data.forEach(item => {
        arrObj.push({
            "Keyword": item['value']['Keyword'],
            "Total": item['value']['Total'],
            "Items" : item['value']['Items'].join(";"),
            "SR.Title": GetArrElements(item['value']['SR'],'Title')
        });
        oneObj = null;
    });

    return arrObj;
}

function GetArrElements(arr , nameElement) {
    let res = [];

     arr.forEach(element => {
        if (element[nameElement] !== undefined) {
            res.push(element[nameElement]);
        }
    });

    return res;
}