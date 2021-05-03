/* 
 Files batch loader to ClickHouse
*/

const fs = require('fs');
const path = require('path');
const ClickHouse = require('@apla/clickhouse');
const chLoader = require('./UploaderToCH.js');
const { resolve } = require('path');

//#region user settings
let pthForLoadFiles = '/home/isiv/Desktop/Projects/ClickHouse_NodeJS/Load';
const CH = new ClickHouse({ host: 'localhost', port: '8123', user: 'default', password: '' });
const maxArrLen = 10000; // объем пачки даннных для вставки в CH
//#endregion user settings

let flsForLoad = [];

fs.readdirSync(pthForLoadFiles).forEach(file => {
	flsForLoad.push(path.join(pthForLoadFiles, file));
});

processArray(flsForLoad);


function processArray(flsForLoad) {
	for (const oneFile of flsForLoad) {
		const p = new Promise(function (resolve, reject) {
			chLoader.UploadToCh(CH,oneFile,maxArrLen);
		})
		resolve();
		p.then(console.log('File - ' + oneFile + ' has been loaded!'));
	}
	console.log('Done All!');
  }

