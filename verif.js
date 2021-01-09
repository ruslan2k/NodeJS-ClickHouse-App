const fs = require('fs');
const Verifier = require('stream-json/utils/Verifier');

var path = 'C:/Users/mne21/Desktop/small.json';
const verifier = new Verifier();
verifier.on('error', error => console.log(error));

fs.createReadStream(path).pipe(verifier);