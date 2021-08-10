import * as fs from 'fs';
import * as csv from 'fast-csv';
const LineByLineReader = require('line-by-line');
import events = require('events');

interface PointFeature {
  attributes: { [key: string]: string | number | Date },
  geometry: {
    x: number,
    y: number;
  }
}

export class AyyekaService extends events.EventEmitter {
  headers: string[];
  lr: typeof LineByLineReader;
  ended: boolean = false;
  ready: boolean = false;
  constructor() {
    super();
  }
  start(csvFile: string) {
    const stream = csv.parse<any, PointFeature>({
      headers: headers => headers.map(h => h?.trim().replace(' ', '_').toUpperCase()),
      objectMode: true,
      delimiter: ',',
      escape: '"',
      ignoreEmpty: true,
      trim: true,
    })
    .transform( (data: any): PointFeature => {
      const feature: PointFeature = {
        attributes: data,
        geometry: {
          x: 0,
          y: 0
        }
      };
      feature.attributes["OBJECTID"] = Number(feature.attributes["ID"]);
      feature.attributes["TRACKID"] = Number(feature.attributes["SITEID"]);
      feature.attributes["VALUE"] = parseFloat(feature.attributes["VALUE"] as string);
      feature.attributes["TIMESTAMP"] = new Date(feature.attributes["TIMESTAMP"] as string);
      delete feature.attributes["ID"];
      return feature;
    })
    .on('headers', headers => {
      console.log(headers);
      this.headers = headers;
      this.emit('ready');
    })
    .on('error', error => {
      console.error(error);
      this.emit( 'error', error);
    })
    .on('data', row => {
      // console.log(row);
      this.emit('ready', row);
    })
    .on('data-invalid', row => {
      console.error(row);
      this.emit('error', row);
    })
    .on('end', (rowCount: number) => {
      console.log(`Parsed ${rowCount} rows`);
      this.ended = true;
      this.emit('end');
    });

    this.lr = new LineByLineReader(csvFile);
    this.lr.on( 'error', ( error: any) => {
      console.log(error);
      this.ended = true;
      this.emit('error', error);
      stream.end();
    });
    this.lr.on('line', (line: string) => {
      this.lr.pause();
      console.log(line);
      const lineCRLF = line + '\r\n';
      stream.write(lineCRLF);
    });
    this.lr.on('end', () => {
      console.log('LR Done');
      this.emit('end');
      stream.end();
    });
  }
  next() {
    console.log('Next');
    this.lr.resume();
  }
  run(csvFile: string) {
    try {
      fs.createReadStream(csvFile)
        .pipe(csv.parse({
          headers: headers => headers.map(h => h?.toUpperCase()),
          objectMode: true,
          delimiter: ',',
          escape: '"',
          ignoreEmpty: true,
          trim: true
        }))
        .on('headers', headers => { 
          console.log(headers);
          this.headers = headers;
        })
        .on('error', error => console.error(error))
        .on('data', row => { 
          console.log(row);
        })
        .on('data-invalid', row => {
          console.error(row);
        })
        .on('end', (rowCount: number) => console.log(`Parsed ${rowCount} rows`));
    } catch(e) {
      console.debug("Got error on parsing csv", e);
    }
  }
}

