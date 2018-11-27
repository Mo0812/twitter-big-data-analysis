var fs = require("fs");
var eol = require("os").EOL;

if(process.argv[2] === "help") {
    console.log("This program helps to convert ES dumps in JSON to the CSV format");
    console.log("Syntax: node json-converter.js JSON_PATH_IN CSV_PATH_OUT");
    process.exit();
} else {
    convertJsonToCsv(process.argv[2], process.argv[3], () => {
        process.exit();
    });
}

function convertJsonToCsv(inputPath, outputPath, callback) {
    fs.readFile(inputPath, (err, data) => {
        let json = JSON.parse(data);
        var output = "";
        for(let line of json) {
            let source = line._source;
            var outputLine = "";
            var firstAttribute = true;
            for(let key in source) {
                let attribute = source[key];
                if(attribute instanceof Object) {
                    for(let subKey in attribute) {
                        firstAttribute ? firstAttribute = false : outputLine += ","; 
                        outputLine += attribute[subKey];
                    }
                } else {
                    firstAttribute ? firstAttribute = false : outputLine += ","; 
                    outputLine += attribute;
                }
            }
            output += outputLine + eol;
        }
    
        fs.writeFileSync(outputPath, output);
        callback();
    });
}