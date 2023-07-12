const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

const uri = 'mongodb://localhost:27021/test?directConnection=true';
const output_path = './output';

const client = new MongoClient(uri);

function log(recordId, message) {
  console.log(`Record ${recordId}: ${message}`);
}

async function run() {
  try {
    // подключаемся к БД
    await client.connect();
    const db = client.db();

    // получаем таблицу Record'ов
    const recordsCollection = await db
      .collection('records')
      .find({}, { projection: { thumbnail: 1 } })
      .toArray();

    // создаём папку для вывода
    if (!fs.existsSync(output_path)) {
      fs.mkdirSync(output_path);
    }

    recordsCollection.forEach((recordCollection) => {
      // если thumbnail у recond'а отсутствует, то пропускаем
      if (recordCollection.thumbnail === null) {
        log(recordCollection._id, 'No thumbnail');
        return;
      }
      
      // выбираем нужный протокол
      const request = recordCollection.thumbnail.trimStart().startsWith('https') ? https : http;
      recordCollection._id = recordCollection._id.toString();

      // получаем данные
      const req = request.get(recordCollection.thumbnail, (response) => {
        if (response.statusCode !== 200) {
          log(recordCollection._id, 'File is unavaliable');
          return;
        }

        // создаём поток вывода в файл
        const file = fs.createWriteStream(
          `${output_path}/${recordCollection._id}${path.extname(recordCollection.thumbnail)}`
        );

        // записываем их в файл
        response.pipe(file);

        file.on('finish', () => {
          file.close();
          log(recordCollection._id, 'Download Complete');
          return;
        });
      });

      // обработчик ошибки когда сайт недоступен
      req.on('error', (_) => {
        log(recordCollection._id, 'Site is unavaliable');
      });
    });
  } finally {
    await client.close();
  }
}

run();
