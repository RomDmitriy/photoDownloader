const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

const uri = 'mongodb://localhost:27021/test?directConnection=true';
const output_path = './output';
const requestMaxRecords = 100;

const userIdFilter = '64a80e6874d8c8a21068e103';
const folderIdFilter = '64afd2a705156c28d5922d4b';

const client = new MongoClient(uri);

function makeLog(max) {
  let current = 0;
  let total = max;

  function log(recordId, message) {
    console.log(`[${++current}/${total}]Record ${recordId}: ${message}`);
  }

  return log;
}

function validateUrl(urlString) {
  try {
    new URL(urlString);
    return true;
  } catch {
    return false;
  }
}

// фильр выборки из БД
function createFilter() {
  const filter = {};

  if (userIdFilter && userIdFilter !== '') {
    filter.createdBy = new ObjectId(userIdFilter);
  }

  if (folderIdFilter && folderIdFilter !== '') {
    filter.folder = folderIdFilter;
  }

  return filter;
}

async function run() {
  try {
    // подключаемся к БД
    await client.connect();
    const db = client.db();

    const filter = createFilter();

    const log = makeLog(await db.collection('records').countDocuments(filter));

    for (let count = 0; ; count++) {
      // получаем таблицу Record'ов
      const recordsCollection = await db
        .collection('records')
        .find(filter, {
          projection: { thumbnail: 1 },
          limit: requestMaxRecords,
          skip: requestMaxRecords * count,
        })
        .toArray();

      // если record'ы закончились
      if (recordsCollection.length === 0) break;

      // создаём папку для вывода
      if (!fs.existsSync(output_path)) {
        fs.mkdirSync(output_path);
      }

      for (let i = 0; i < recordsCollection.length; i++) {
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
        await new Promise((resolve) => setTimeout(resolve, 30));
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
