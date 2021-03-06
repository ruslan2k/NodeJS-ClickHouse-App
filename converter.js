const fs = require('fs');
const readline = require('readline');

let readPth = 'C:/Users/mne21/Desktop/array.json';
let writePth = 'C:/Users/mne21/Desktop/test_write.json';
let writeStream = new fs.WriteStream(writePth,{encoding: 'utf8'},{flags: 'a'})

let lineReader = readline.createInterface({
    input: fs.createReadStream(readPth)
});

let firstPos;
let title = "\"Title\":";
let snipet = "\"Snippet\":";
let url = "\"Url\":";
let badWord;

let pattern = /\"|\\/;
re = new RegExp(pattern, "g");
/**
 * парсинг файл по строчно и для определенных строк
 * убираем символы которые мешают валидации json объектов
 * из которых состоит массив, это необходимо для использования 
 * по объектного чтения в дальнейшем, для загрузки в ClickHouse
 */

let startTime = new Date().toLocaleTimeString();
console.log('Start time__' + startTime);

lineReader.on('line', function (line) {

    if (line.indexOf(title) !== -1) {
        badWord = title;
        firstPos = line.indexOf(title);
    }

    if (line.indexOf(snipet) !== -1) {
        badWord = snipet;
        firstPos = line.indexOf(snipet);
    }

    if (line.indexOf(url) !== -1) {
        badWord = url;
        firstPos = line.indexOf(url);
    }

    if ( firstPos !== undefined ) {
        
        if (line.charAt(line.length - 2) === ",") {
            isCommaEnd = true;
        }
        else{
            isCommaEnd = false;
        }

        line = line.slice(firstPos + badWord.length + 1,line.length).trim();
        if (isCommaEnd) {
            line = line.slice(0, -2);
        } else {
            line = line.slice(0, -1);
        }
        line = line.slice(1);
        line = line.replace(re, ' ');
        if(isCommaEnd){
            line = "    " + badWord + "\"" + line + "\"" + ",\r\n";
        }
        else{
            line = "    " + badWord + "\"" + line + "\"" + "\r\n";
        }
    }
    else{
        line = line + "\r\n";
    }

    isCommaEnd = undefined;
    firstPos = undefined;
    badWord = undefined;

    writeStream.write(line);
    //console.log(line);
});

writeStream.on('close',function() {
    let endTime = new Date().toLocaleTimeString();
    console.log('Start time__' + startTime + '__End Time__' + endTime);
});