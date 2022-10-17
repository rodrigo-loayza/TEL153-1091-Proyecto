const env = require('./secrets.js');
const EventHubReader = require('./utils/event-hub-reader.js'); // Contiene código de https://github.com/Azure-Samples/web-apps-node-iot-hub-data-visualization
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { performance } = require('perf_hooks');

// Parámetros de conexión al IoT Hub y base de datos
const iotHubConnectionString = env.keys.iotHubConnectionString;
const eventHubConsumerGroup = env.keys.eventHubConsumerGroup;
const databaseIP = '34.193.169.168';
const token = env.keys.influxDBToken;
const org = 'iot-project';
const bucket = 'iot-bucket';
const client = new InfluxDB({ url: 'http://' + databaseIP + ':8086', token: token });

const { MongoClient } = require('mongodb');
const uri = "mongodb://" + databaseIP + ":27017/?maxPoolSize=20";
const mongoClient = new MongoClient(uri);

let database = mongoClient.db("iot-project")
let collectionSensorDataTs = database.collection("temperature_data");

// Instanciación del eventHubReader para extraer los mensajes del IoT Hub
const eventHubReader = new EventHubReader(iotHubConnectionString, eventHubConsumerGroup);
console.log('Conexión exitosa al IoT Hub');

// Lectura de datos
(async () => {
    await eventHubReader.startReadMessage((message, date, deviceId) => {
        try {
            const payload = {
                IotData: message,
                MessageDate: date || Date.now().toISOString(),
                DeviceId: deviceId,
            };

            // Procesamiento extra (ejemplo)
            procesarData(payload);

            // Monitoreo de consola
            console.log('\n----------------------------------------------------------------');
            console.log(JSON.stringify(payload));

            // Carga de datos a la colección time series de Mongo
            saveDataTsMongoDB(payload.DeviceId, payload.IotData.temperature);

            // Carga de datos a InfluxDB
            saveDataInflux(payload.DeviceId, payload.IotData.temperature);

        } catch (err) {
            console.error(
                `Error al almacenar datos: [${err}] from [${message}].`
            );
        }
    });
})().catch();

function procesarData(payload) {
    payload.IotData.temperature += 1;
};

function saveDataInflux(device, temperatura) {
    let t0 = performance.now();
    let writeApi = client.getWriteApi(org, bucket);
    writeApi.useDefaultTags({ host: 'Node.js-server' });

    let point = new Point('temperature')
        .tag('device', device)
        .floatField('value', temperatura);
    writeApi.writePoint(point);

    writeApi
        .close()
        .then(() => {
            let t1 = performance.now();
            console.log(
                `\nDatos almacenados en InfluxDB en ${(t1 - t0).toFixed(3)} ms`
            );
        })
        .catch(err => {
            console.error(
                `Error al almacenar datos: [${err}]`
            );
        });
};



function saveDataTsMongoDB(device, temperatura) {
    let t0 = performance.now();
    let dataToBeSaved = {
        timestamp: new Date(),
        device: device,
        temperatura: temperatura
    };

    mongoClient
        .connect()
        .then(async function (conn) {
            let result = await collectionSensorDataTs.insertOne(dataToBeSaved);
            let t1 = performance.now();
            console.log(
                `\nDatos almacenados en MongoDB en  ${(t1 - t0).toFixed(3)} ms`
            );
        })
        .catch(err => {
            console.error(
                `Error al almacenar datos: [${err}]`
            );
        });
};