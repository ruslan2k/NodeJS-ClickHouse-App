/*
Скрипт дл вставки данных в БД Yandex ClickHouse
*/
const fs = require('fs');
const path = require('path');
const Batch = require('stream-json/utils/Batch');
const StreamArray = require('stream-json/streamers/StreamArray');
const { chain } = require('stream-chain');

module.exports = {
    UploadToCh,
}

function UploadToCh(CH, fullFilePath, maxArrLen) {
    return new Promise((resolve, reject) => {
        //let fullFilePath = '/home/isiv/Desktop/Projects/ClickHouse_NodeJS/2020_Nov-30_19-01-11.json';
        let recDbStream = CreateDbStream(CH);
        let cntPush = 0;

        let fileDate = GetDateByFileName(path.parse(path.basename(fullFilePath)).name);

        const pipeline = chain([
            fs.createReadStream(fullFilePath, { encoding: 'utf8' }),
            StreamArray.withParser(),
            new Batch({ batchSize: maxArrLen })
        ]);

        pipeline.on('data', data => {
            cntPush = cntPush + maxArrLen;

            let startTime = new Date().toLocaleTimeString();
            console.log('Start time__' + startTime);

            let objArrForDb = GetDataArr(data, fileDate);
            recDbStream.write(objArrForDb);
            console.log('Insert part compete__' + cntPush);
            cntPush++;
        });

        pipeline.on('end', () => {
            let endTime = new Date().toLocaleTimeString();
            recDbStream.end();
            console.log('Start time__' + startTime + '__End Time__' + endTime);
            console.log('All done!');
            resolve();
        });

        pipeline.on('error', (error) => {
            console.error(error);
            reject(error);
        });
    })
}

/**
* DB Query
*/
function CreateDbStream(CH) {
    const writableStream = CH.query(`INSERT INTO WellData.SearchRes`, { format: 'JSONEachRow' }, (err) => {
        if (err) {
            console.error(err);
        }
    });
    return writableStream;
}

/**
 * Transform chunk(json array) to JSONEachRow format for write to CH
 */
function GetDataArr(data, eventDt) {
    let allObjects = '';
    let grMaxLen = 0;
    let srMaxLen = 0;

    //let arrObj = [];
    let oneObj = Object.create(null);

    data.forEach(item => {
        // main mapping
        oneObj =
        {
            "EventDate": eventDt,
            "Keyword": item['value']['Keyword'],
            "Total": item['value']['Total'],
            "Items": item['value']['Items'].join(";"),
            "SR.Title": GetArrElements(item['value']['SR'], 'Title'),
            "SR.Snippet": GetArrElements(item['value']['SR'], 'Snippet'),
            "SR.Url": GetArrElements(item['value']['SR'], 'Url'),
            "SR.SiteUrls": ParseArrInArr(item['value']['SR'], 'SiteUrls'),
            "GR.Title": GetArrElements(item['value']['GR'], 'Title'),
            "GR.Snippet": GetArrElements(item['value']['GR'], 'Snippet'),
            "GR.Url": GetArrElements(item['value']['GR'], 'Url'),
            "GR.SiteUrls": ParseArrInArr(item['value']['GR'], 'SiteUrls')
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

function GetArrElements(arr, nameElement) {
    let res = [];
    arr.forEach(element => {
        if (element[nameElement] !== undefined) {
            res.push(element[nameElement]);
        }
    });
    return res;
}

function ParseArrInArr(arr, nameElement) {
    let res = [];
    arr.forEach(element => {
        if (element[nameElement] !== undefined) {
            if (element[nameElement].length != 0) {
                res.push(element[nameElement].join(";"));
            }
            else {
                res.push('');
            }
        }
    });
    return res;
}

/**
 * ClickHouse settings date_time_output_format : basic parser can parse only '2019-01-01 00:00:00' format
 * parse file names like - 2020_Dec-01_15-51-55 -----> CH '2019-01-01 00:00:00'
 */
function GetDateByFileName(fileName) {

    let dt;
    dt = fileName.replace(/-/g, ':');
    dt = dt.replace('_', '-');
    dt = dt.replace('_', ' ');
    dt = dt.replace(':', '-');

    let unix_timestamp = Date.parse(dt);

    let fullDt = new Date(unix_timestamp);

    let dtYear = fullDt.getFullYear();
    let dtMounts = fullDt.getUTCMonth() + 1;
    let dtDay = fullDt.getUTCDate();
    let dtHour = fullDt.getHours();
    let dtMin = fullDt.getMinutes();
    let dtSec = fullDt.getSeconds();

    let dtForCH = dtYear + "-" + ("0" + dtMounts).slice(-2) + "-" + ("0" + dtDay).slice(-2) + " " +
        ("0" + dtHour).slice(-2) + ":" + ("0" + dtMin).slice(-2) + ":" + ("0" + dtSec).slice(-2);

    return dtForCH;
}
