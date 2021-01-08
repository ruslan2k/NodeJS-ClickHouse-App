const fs = require('fs');
const ClickHouse = require('@apla/clickhouse');
const StreamArray = require('stream-json/streamers/StreamArray');
const { Writable } = require('stream');

let path = 'C:/Users/mne21/Desktop/array.json';
const ch = new ClickHouse({ host: 'localhost', port: '8123', user: 'default', password: 'hiclick10' })
const maxArrLen = 100;

let arr = [];
let cnt = 0;
let cntPush = 1;
let recDbStream;

const fileStream = fs.createReadStream(path,{encoding: 'utf8'});
const jsonStream = StreamArray.withParser();
const processingStream = new Writable({
    write({ key, value }, encoding, callback) {
        
        if (arr.length < maxArrLen) {
            arr.push([value["Keyword"], value["Items"][0]])
            cnt++;
            console.log(cnt + "__" + value["Keyword"]);
        }

        //some async operations
        setTimeout(() => {

            if(arr.length == maxArrLen) {
                recDbStream = CreateDbStream(cntPush);
                PushToDB(recDbStream, ConcatStringForDb(arr));
                cntPush++;
                arr = [];
            }
            
            //Runs one at a time, need to use a callback for that part to work
            callback();
        }, 100);
    },
    //Don't skip this, as we need to operate with objects, not buffers
    objectMode: true
});

//Pipe the streams as follows
fileStream.pipe(jsonStream.input);
jsonStream.pipe(processingStream);

//So we're waiting for the 'finish' event when everything is done.
processingStream.on('finish', () => {
    recDbStream = CreateDbStream(cntPush);
    PushToDB(recDbStream, ConcatStringForDb(arr));
    arr = [];
});

processingStream.on('end', () => {
    console.log('All done!');
});

function CreateDbStream(cntPush) {
    const writableStream = ch.query(`INSERT INTO WellData.SearchRes FORMAT TSV`, (err) => {
        if (err) {
            console.error(err)
        }
        console.log('Insert part complete' + '__' + cntPush)
    })

    return writableStream;
}

function PushToDB(recDbStream, arr) {
    recDbStream.write(arr);
    recDbStream.end();
}

function ConcatStringForDb(arr) {
    var concStr = "";
    arr.forEach(vals => {
        concStr = concStr + vals[0] + "\t" + vals[1] + "\n\r";
    });
    concStr = concStr.substring(0, concStr.length - 4);
    return concStr;
}