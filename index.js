const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

const uri = 'mongodb://localhost:27021/test?directConnection=true';
const output_path = './output';
const max_records = 500;

const client = new MongoClient(uri);

function log(recordId, message) {
  console.log(`Record ${recordId}: ${message}`);
}

function validateUrl(urlString) {
  try {
    new URL(urlString);
    return true;
  } catch {
    return false;
  }
}

async function run() {
  try {
    // подключаемся к БД
    await client.connect();
    const db = client.db();

    for (let count = 0;; count++) {
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
      if (!recordsCollection.length) break;

      // создаём папку для вывода
      if (!fs.existsSync(output_path)) {
        fs.mkdirSync(output_path);
      }

      recordsCollection.forEach((recordCollection) => {
        // если thumbnail у recond'а отсутствует, то пропускаем
        if (!recordCollection.thumbnail?.publicUrl) {
          log(recordCollection._id, 'No thumbnail or public link');
          return;
        }

        // проверяем ссылку
        const link = recordCollection.thumbnail.publicUrl;
        if (!validateUrl(link)) {
          log(recordCollection._id, `Wrong link format: "${link}"`);
          return;
        }

        // выбираем нужный протокол
        const request = link.trimStart().startsWith('https') ? https : http;
        recordCollection._id = recordCollection._id.toString();

        // получаем данные
        const req = request.get(link, (response) => {
          if (response.statusCode !== 200) {
            log(recordCollection._id, 'File is unavaliable');
            return;
          }

          // создаём поток вывода в файл
          const file = fs.createWriteStream(`${output_path}/${recordCollection._id}${path.extname(link)}`);

          // записываем их в файл
          response.pipe(file);

          file.on('finish', () => {
            file.close();
            log(recordCollection._id, 'Download Complete');
          });
        });

        // обработчики ошибок
        req.on('error', (err) => {
          console.error(err);
          log(recordCollection._id, 'Site is unavaliable');
        });
      });
    }
  } finally {
    await client.close();
  }
}

run();
