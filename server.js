const env = require('./secrets.js');
const EventHubReader = require('./utils/event-hub-reader.js');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// Parámetros de conexión al IoT Hub y base de datos
const iotHubConnectionString = env.keys.iotHubConnectionString;
const eventHubConsumerGroup = env.keys.eventHubConsumerGroup;
const databaseIP = '34.193.169.168';
const token = env.keys.influxDBToken;
const org = 'iot-project';
const bucket = 'iot-bucket';
const client = new InfluxDB({ url: 'http://' + databaseIP + ':8086', token: token });

// Instanciación del eventHubReader para extraer los mensajes del IoT Hub
const eventHubReader = new EventHubReader(iotHubConnectionString, eventHubConsumerGroup);
console.log('Conexión exitosa al IoT Hub');

(async () => {
    // Extracción de datos
    await eventHubReader.startReadMessage((message, date, deviceId) => {
        try {
            const payload = {
                IotData: message,
                MessageDate: date || Date.now().toISOString(),
                DeviceId: deviceId,
            };

            // Procesamiento extra
            procesarData(payload);

            // Monitoreo de consola
            console.log(JSON.stringify(payload));

            // Carga de datos a la base de InfluxDB
            saveDataInflux(date,
                payload.DeviceId,
                payload.IotData.temperature);

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

function saveDataInflux(date, device, temperatura) {
    let writeApi = client.getWriteApi(org, bucket);
    writeApi.useDefaultTags({ host: 'Node.js-server' });

    let point = new Point('temperature')
        .tag('device', device)
        .floatField('value', temperatura)
        .timestamp(date);
    writeApi.writePoint(point);

    writeApi
        .close()
        .then(() => {
            console.log(
                `Nuevos datos fueron almacenados: ${point}`
            );
        })
        .catch(err => {
            console.error(
                `Error al almacenar datos: [${err}]`
            );
        });
};