const fs = require('fs');

let readPth = 'C:/Users/mne21/Desktop/test.json';
let writePth = 'C:/Users/mne21/Desktop/array_wr.json';
let readStream = new fs.ReadStream(readPth,{encoding: 'utf8'});
let writeStream = new fs.WriteStream(writePth,{encoding: 'utf8'})

//readStream.pipe(writeStream);
let arr = [];

readStream.on('readable',() => {
    let data = readStream.read().toString();
    console.log(data);
})

readStream.on('end',() => {
    console.log('All done!');
})