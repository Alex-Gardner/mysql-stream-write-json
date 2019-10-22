const fs = require("fs");
const { Transform, pipeline } = require("stream");
var util = require("util");
var split = require("split");
var mysql = require("mysql");
var JSONStream = require("JSONStream");

// Insert the json file you wish to use here: 
const largeTable = 'large-ledger.json';

const bigFileStream = fs.createReadStream(largeTable);

// Apply DB connection options here
const targetTable = "DATA_TEST";

var pool = mysql.createPool({
  connectionLimit: 1000, // update sql settings to 5k max connections
  connectTimeout: 60 * 60 * 1000,
  acquireTimeout: 60 * 60 * 1000,
  timeout: 60 * 60 * 1000,   
  host: "localhost",
  user: "mock_user",
  database: "my_db"
});

function parenthesize(targetString) {
  return `(${targetString})`;
}


let transformChunk, columnArray, columnListSQL, arrayValues, valueListSQL;
let queryNumber = 0;
let chunkNumber = 1;

// ---------- Stream Implementation ----------------------------
class DataStreamTransform extends Transform {
  constructor() {
    super({ writableObjectMode: true });
  }
  _transform(chunk, encoding, callback) {
    //all variables are created on every callback call (on every line of the dataset)
    transformChunk = chunk; //SON.parse(chunk.toString());
    columnArray = Object.keys(transformChunk);
    columnListSQL = parenthesize(
      columnArray.reduce(function(accu, current) {
        return `${accu}, ${current}`;
      })
    );

    arrayValues = Object.values(transformChunk).map(item => `"${item}"`);

    valueListSQL = parenthesize(
      arrayValues.reduce(function(accu, current) {
        return `${accu}, ${current}`;
      })
    );
    // On the first line of data, we need to create the table fields & make the table in SQL

    if (chunkNumber === 1) {
      chunkNumber++;
      var columnOptions = columnArray.reduce(function(
        accu,
        current,
        currentIndex
      ) {
        function firstVar() {
          if (currentIndex === 1) {
            return accu + " TEXT";
          } else {
            return accu;
          }
        }
        return `${firstVar()}, ${current} TEXT`;
      });
      pool.getConnection(function(err, connection) {
        if (err) {
          throw err;
        }
        // create table

        connection.query(
          `CREATE TABLE ${targetTable} (id INT AUTO_INCREMENT PRIMARY KEY, ${columnOptions})`,
          function(error, results, fields) {
            if (error) {
              throw error;
            }
            connection.query(
              `INSERT INTO ${targetTable} ${columnListSQL} VALUES ?`,
              [[arrayValues]],
              function(error, results, fields) {
                callback(null, null); // Stream callback here to block updates before table is created

                queryNumber++;
                console.log(
                  `Queries Run: ${queryNumber}, Data ID: ${arrayValues[1]}`
                );
                //   When done with the insert connection, relase it
                connection.release();

                // Handle error after release
                if (error) {
                  throw error;
                }
              }
            );
            connection.release();
            console.log("Columns Added");
          }
        );
      });
    } else {
      // Table already created (i.e first line parsed)
      pool.getConnection(function(err, connection) {
        callback(null, null);
        if (err) {
          throw err;
        }
        // Use the connection
        connection.query(
          `INSERT INTO ${targetTable} ${columnListSQL} VALUES ?`,
          [[arrayValues]],
          function(error, results, fields) {
            queryNumber++;
            console.log(
              `Queries Run: ${queryNumber}, Data ID: ${arrayValues[1]}`
            );
            //   When done with the connection, relase it
            connection.release();

            // Handle error after release
            if (error) {
              throw error;
            }
          }
        );
      });
    }

    // callback(null, null);
  }
  _flush(callback) {
    this.push("completed data");
    callback();
  }
}



var transformingStream = new DataStreamTransform();

// ----------------------
// Creating Pipe Connections:
pipeline(
  bigFileStream,
  split(),
  JSONStream.parse(),
  transformingStream,
  function(err) {
    if (err) {
      console.error("Pipeline failed", err);
    } else {
      console.log("Pipeline Succeeded");
    }
  }
);

// Debugging Memory Leaks:
const numeral = require("numeral");
setInterval(() => {
  const { rss, heapTotal } = process.memoryUsage();
  console.log("rss", numeral(heapTotal).format("0.0, ib"));
}, 500);

