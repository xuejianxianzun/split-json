const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamValues } = require('stream-json/streamers/StreamValues');
const fs = require('fs');

// test small file
// const file = 'test-233.json'

// big file
const file = 'test-573002.json'
const batch = 100000

const pipeline = chain([
  fs.createReadStream(file),
  parser(),
  streamValues(),
  data => {
    splitData(data.value)
    return data
  }
]);

async function splitData (dataArray) {
  const total = dataArray.length
  let index = 0

  let temp = []
  while (index < total) {
    temp.push(dataArray[index])
    ++index
    if (index % batch === 0 || index === total) {
      console.log(index, temp.length)
      await saveFile(`split - ${index}.json`, JSON.stringify(temp))
      temp = []
    }
  }
}

async function saveFile (name, data) {
  return new Promise(resolve => {
    var writerStream = fs.createWriteStream(name);
    writerStream.write(data, 'UTF8');
    writerStream.end();

    writerStream.on('finish', function () {
      return resolve()
    });
  })
}