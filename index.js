const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

const uri = 'mongodb://localhost:27021/test?directConnection=true';
const output_path = './output';
const max_records = 100;

const client = new MongoClient(uri);

let current = 0;
let total = 0;

function log(recordId, message) {
  console.log(`[${current}/${total}]Record ${recordId}: ${message}`);
}

function validateUrl(urlString) {
  try {
    new URL(urlString);
    return true;
  } catch {
    return false;
  }
}

function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

async function run() {
  try {
    // подключаемся к БД
    await client.connect();
    const db = client.db();

    total = await db.collection('records').countDocuments();

    for (let count = 0; ; count++) {
      // получаем таблицу Record'ов
      const recordsCollection = await db
        .collection('records')
        .find(
          {},
          {
            projection: { thumbnail: 1 },
            limit: max_records,
            skip: max_records * count,
          }
        )
        .toArray();

      // если record'ы закончились
      if (recordsCollection.length === 0) break;

      // создаём папку для вывода
      if (!fs.existsSync(output_path)) {
        fs.mkdirSync(output_path);
      }

      for (let i = 0; i < recordsCollection.length; i++, current++) {
        // если thumbnail у recond'а отсутствует, то пропускаем
        if (!recordsCollection[i].thumbnail?.publicUrl) {
          log(recordsCollection[i]._id, 'No thumbnail or public link');
          continue;
        }

        // проверяем ссылку
        const link = recordsCollection[i].thumbnail.publicUrl;
        if (!validateUrl(link)) {
          log(recordsCollection[i]._id, `Wrong link format: "${link}"`);
          continue;
        }

        // выбираем нужный протокол
        const request = link.trimStart().startsWith('https') ? https : http;
        recordsCollection[i]._id = recordsCollection[i]._id.toString();

        // получаем данные
        await delay(30);
        const req = request.get(link, (response) => {
          if (response.statusCode !== 200) {
            log(recordsCollection[i]._id, 'File is unavaliable');
            return;
          }

          // создаём поток вывода в файл
          const file = fs.createWriteStream(`${output_path}/${recordsCollection[i]._id}${path.extname(link)}`);

          // записываем их в файл
          response.pipe(file);

          file.on('finish', () => {
            file.close();
            log(recordsCollection[i]._id, 'Download Complete');
          });
        });

        // обработчики ошибок
        req.on('error', (err) => {
          console.error(err);
          log(recordsCollection[i]._id, 'Site is unavaliable');
        });
      }
    }
  } finally {
    await client.close();
  }
}

run();
