const fs = require('fs');
const ClickHouse = require('@apla/clickhouse');
const Batch = require('stream-json/utils/Batch');
const StreamArray = require('stream-json/streamers/StreamArray');
const {chain} = require('stream-chain');

const CH = new ClickHouse({ host: 'localhost', port: '8123', user: 'default', password: 'hiclick10' })
let path = 'C:/Users/mne21/Desktop/small.json';
const maxArrLen = 10000; //разработчик ClickHouse рекомендует вставлять пачками от 100000
let recDbStream;
let cntPush = 0;

const pipeline = chain([
    fs.createReadStream(path,{encoding: 'utf8'}),
    StreamArray.withParser(),
    new Batch({batchSize: maxArrLen})
]);


pipeline.on('data', data => {
    cntPush = cntPush + maxArrLen;

    let objArrForDb = GetDataArr(data);
    recDbStream = CreateDbStream();
    recDbStream.write(objArrForDb);
    recDbStream.end();
    console.log('Insert part compete__' + cntPush)
    cntPush ++;
});

let startTime = new Date().toLocaleTimeString();
console.log('Start time__' + startTime);

pipeline.on('end', () => {
    let endTime = new Date().toLocaleTimeString();
    console.log('Start time__' + startTime + '__End Time__' + endTime);
    console.log('All done!')
});
pipeline.on('error', error => console.log(error));



/**
* DB Query
*/
function CreateDbStream() {
    const writableStream = CH.query(`INSERT INTO WellData.SearchRes`, { format: 'JSONEachRow' }, (err) => {
        if (err) {
            console.error(err)
        }
    });
    return writableStream;
}

/**
 * Transform chunk(json array) to JSONEachRow format for write to CH
 */
function GetDataArr(data) {
    let allObjects = '';
    let grMaxLen = 0;
    let srMaxLen = 0;

    //let arrObj = [];
    let oneObj = Object.create(null);

    data.forEach(item => {
        // main mapping
        oneObj =
        {
            "Keyword": item['value']['Keyword'],
            "Total": item['value']['Total'],
            "Items" : item['value']['Items'].join(";"),
            "SR.Title": GetArrElements(item['value']['SR'],'Title'),
            "SR.Snippet": GetArrElements(item['value']['SR'],'Snippet'),
            "SR.Url": GetArrElements(item['value']['SR'],'Url'),
            "SR.SiteUrls": ParseArrInArr(item['value']['SR'],'SiteUrls'),
            "GR.Title": GetArrElements(item['value']['GR'],'Title'),
            "GR.Snippet": GetArrElements(item['value']['GR'],'Snippet'),
            "GR.Url": GetArrElements(item['value']['GR'],'Url'),
            "GR.SiteUrls": ParseArrInArr(item['value']['GR'],'SiteUrls')
        }

        
        // if (oneObj["GR.Title"].length !== oneObj["GR.SiteUrls"].length) {
        //     console.log("err");
        // }

        //SR
        for (let key in oneObj) {
            if (key.indexOf('SR') !== -1) {
                if (oneObj[key].length > srMaxLen) {
                    srMaxLen = oneObj[key].length;
                }
            }
        }
        for (let key in oneObj) {
            if (key.indexOf('SR') !== -1) {
                if (oneObj[key].length < srMaxLen) {
                    while (oneObj[key].length != srMaxLen) {
                        oneObj[key].push(" ");
                    }
                }
            }
        }

        //GR 
        for (let key in oneObj) {
            if (key.indexOf('GR') !== -1) {
                if (oneObj[key].length > grMaxLen) {
                    grMaxLen = oneObj[key].length;
                }
            }
        }

        for (let key in oneObj) {
            if (key.indexOf('GR') !== -1) {
                if (oneObj[key].length < grMaxLen) {
                    while (oneObj[key].length != grMaxLen) {
                        oneObj[key].push(" ");
                    }
                }
            }
        }

        // if (oneObj["GR.Title"].length !== oneObj["GR.SiteUrls"].length) {
        //     console.log("err");
        // }

        allObjects = allObjects + JSON.stringify(oneObj) + ' ';
        //arrObj.push(oneObj);
        oneObj = null;
        grMaxLen = 0;
        srMaxLen = 0;
    });

    return allObjects;
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

function ParseArrInArr(arr , nameElement) {
    let res = [];
    arr.forEach(element => {
        if (element[nameElement] !== undefined) {
            if (element[nameElement].length != 0) {
                res.push(element[nameElement].join(";"));
            }
            else{
                res.push('');
            }
        }
    });
    return res;
}