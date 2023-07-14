import { MongoClient, ObjectId } from 'mongodb';
import fs from 'fs';
import path from 'path';
import http from 'http';
import https from 'https';
import { printTable } from 'console-table-printer';

//---------------------------------------------------------------------

const uri: string = 'mongodb://localhost:27021/test?directConnection=true';
const output_path: string = './output';
const requestMaxRecords: number = 100;

const userIdFilter: string = '';
const folderIdFilter: string = '64afd2a705156c28d5922d4a';

//---------------------------------------------------------------------

class Statistics {
  static total: number = 0;
  static success: number = 0;
  static current: number = 0;
  static failed: {
    byWrongUri: number;
    byUnavaliableSite: number;
    byUnavaliableFile: number;
  } = {
    byWrongUri: 0,
    byUnavaliableSite: 0,
    byUnavaliableFile: 0,
  };

  static log() {
    const interval = setInterval(() => {
      if (Statistics.isComplete()) {
        printTable([
          {
            category: 'Total records',
            value: this.total,
          },
          {
            category: 'Success',
            value: this.success,
          },
          {
            category: 'Failed by wrong URI',
            value: this.failed.byWrongUri,
          },
          {
            category: 'Failed by unavaliable file',
            value: this.failed.byUnavaliableFile,
          },
          {
            category: 'Failed by unavaliable site',
            value: this.failed.byUnavaliableSite,
          },
        ]);
        clearInterval(interval);
      }
    }, 1000);
  }

  static isComplete() {
    return (
      this.total ===
      this.success + this.failed.byUnavaliableFile + this.failed.byUnavaliableSite + this.failed.byWrongUri
    );
  }
}

function log(recordId: ObjectId, message: string) {
  console.log(`[${++Statistics.current}/${Statistics.total}]Record ${recordId}: ${message}`);
}

function validateUrl(url: string): boolean {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

interface IFilter {
  createdBy?: ObjectId;
  folder?: string;
}

// фильтр выборки из БД
function createFilter(): IFilter {
  const filter: IFilter = {};

  if (userIdFilter && userIdFilter !== '') {
    filter.createdBy = new ObjectId(userIdFilter);
  }

  if (folderIdFilter && folderIdFilter !== '') {
    filter.folder = folderIdFilter;
  }

  return filter;
}

async function run(): Promise<void> {
  const client = new MongoClient(uri);
  try {
    // подключаемся к БД
    await client.connect();
    const db = client.db();

    const filter = createFilter();

    Statistics.total = await db.collection('records').countDocuments(filter);

    for (let iteration = 0; ; iteration++) {
      // получаем таблицу Record'ов
      const recordsCollection = await db
        .collection('records')
        .find(filter, {
          projection: { thumbnail: 1 },
          limit: requestMaxRecords,
          skip: requestMaxRecords * iteration,
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
        const link: string = recordsCollection[i].thumbnail.publicUrl;
        if (!validateUrl(link)) {
          Statistics.failed.byWrongUri++;
          log(recordsCollection[i]._id, `Wrong link format: "${link}"`);
          continue;
        }

        // выбираем нужный протокол
        const request = link.trim().startsWith('https') ? https : http;
        recordsCollection[i]._id = new ObjectId(recordsCollection[i]._id.toString());

        // получаем данные
        await new Promise((resolve) => setTimeout(resolve, 30));
        request
          .get(link, (response) => {
            if (response.statusCode !== 200) {
              Statistics.failed.byUnavaliableFile++;
              log(recordsCollection[i]._id, 'File is unavaliable');
              return;
            }

            // создаём поток вывода в файл
            const file = fs.createWriteStream(`${output_path}/${recordsCollection[i]._id}${path.extname(link)}`);

            // записываем их в файл
            response.pipe(file);

            file.on('finish', () => {
              file.close();
              Statistics.success++;
              log(recordsCollection[i]._id, 'Download Complete');
            });
          })
          .on('error', (_) => {
            Statistics.failed.byUnavaliableSite++;
            log(recordsCollection[i]._id, 'Site is unavaliable');
          });
      }
    }
  } finally {
    await client.close();
  }
}

run();
Statistics.log();