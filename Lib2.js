const fs = require('fs');
const ClickHouse = require('@apla/clickhouse');
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {streamArray} = require('stream-json/streamers/StreamArray');

const ch = new ClickHouse({ host:'localhost', port:'8123', user:'default', password:'hiclick10' })

// const readableStream = fs.createReadStream('C:/Users/mne21/Desktop/dt.csv')
// const writableStream = ch.query(`INSERT INTO WellData.codec FORMAT CSV`, (err) => {
//   if (err) {
//     console.error(err)
//   }
//   console.log('Insert complete!')
// })
// readableStream.pipe(writableStream);
// // data will be formatted for you
//writableStream.write([1, 2.22, "erbgwerg", new Date()])

// prepare data yourself
//writableStream.write("1\t1")

const pipeline = chain([
    fs.createReadStream('C:/Users/mne21/Desktop/array.json'),
    parser(),
    streamArray()
  ]);


//writableStream.end()
//const pipeline = fs.createReadStream('C:/Users/mne21/Desktop/array.json').pipe(streamArray.withParser());
//var flForLoad = require('C:/Users/mne21/Desktop/test.json');

let objectCounter = 0;
pipeline.on('data', () => ++objectCounter);
pipeline.on('end', () => console.log(`Found ${objectCounter} objects.`));
