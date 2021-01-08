const fs = require('fs');
const ClickHouse = require('@apla/clickhouse');
const Batch = require('stream-json/utils/Batch');
const StreamArray = require('stream-json/streamers/StreamArray');
const {chain} = require('stream-chain');

const CH = new ClickHouse({ host: 'localhost', port: '8123', user: 'default', password: 'hiclick10' })
let path = 'C:/Users/mne21/Desktop/example_2.json';
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
    //let strForDb = GetDataString(data);
    const stream = CH.query(`INSERT INTO WellData.SearchRes`, { format: 'JSONEachRow' })
    let jsO = 
        {
            "Keyword": "HI_bsd",
            "Total": "3",
            "Items" : "asf",
            "SR.Title": ["dfsa","dfafa"]
        }
    let jsOg = 
        {
            "Keyword": "HI_bsd",
            "Total": "3",
            "Items" : "asf",
            "SR.Title": ["dfsa","dfafa"]
        }
    let jsARR = [
        {
            "Keyword": "HI_bsd",
            "Total": "3",
            "Items" : "asf",
            "SR.Title": ["dfsa","dfafa"]
        },
        {
            "Keyword": "HI_bsd",
            "Total": "3",
            "Items" : "asf",
            "SR.Title": ["dfsa","dfafa"]
        }
    ]


    stream.write(...jsARR);
    stream.end();

    // recDbStream = CreateDbStream();
    // recDbStream.write(strForDb);
    // recDbStream.end();
    console.log('Insert part complete' + '__' + cntPush)
});


pipeline.on('end', () => console.log('All done!'));
pipeline.on('error', error => console.log(error));
//------------------------------------------------------------------------------------------
function CreateDbStream() {
    const writableStream = CH.query(`INSERT INTO WellData.SearchRes FORMAT TSV`, (err) => {
        if (err) {
            console.error(err)
        }
    })
    return writableStream;
}

function GetDataString(data) {
    let strDB = '';
    data.forEach(item => {
        strDB = strDB + item['value']['Keyword'] + "\t" + item['value']['Total'] + "\t" + item['value']['Items'].join(";") + "\n\r";//"\t" + ["r"] + "\t" + ["r"] + "\t" + ["r"] + "\t" + ["r"] +
    });
    strDB = strDB.substring(0, strDB.length - 2);
    return strDB;
}