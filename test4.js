
const fs = require('fs');
var path = 'C:/Users/mne21/Desktop/array.json';
var importStream = fs.createReadStream(path, {flags: 'r', encoding: 'utf-8'});
importStream.on('data', function(chunk) {

    var pleaseBeAJSObject = JSON.parse(chunk);           
    // insert pleaseBeAJSObject in a database
});
importStream.on('end', function(item) {
   console.log("Woot, imported objects into the database!");
});