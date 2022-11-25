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
                MessageProcessDate: date || Date.now(),
                DeviceId: deviceId,
            };

            // Procesamiento extra
            procesarData(payload);

            // Monitoreo de consola
            console.log('Dato a cargar a base: ' + JSON.stringify(payload));

            // Carga de datos a la base de InfluxDB
            saveDataInflux(payload.IotData.timestamp,
                payload.DeviceId,
                payload.IotData.temperature,
                payload.IotData.presence);

        } catch (err) {
            console.error(
                `Error al almacenar datos: [${err}] from [${message}].`
            );
        }
    });
})().catch();


function procesarData(payload) {
    let str = payload.IotData.timestamp;
    let [dateValues, timeValues] = str.split(' ');
    let [year, month, day] = dateValues.split('-');
    let [hours, minutes, seconds] = timeValues.split(':');
    let date = new Date(year, month - 1, day, hours, minutes, seconds);
    payload.IotData.timestamp = date;
};

function saveDataInflux(date, device, temperatura, presencia) {
    date.setHours(date.getHours() + 5);

    let writeApi = client.getWriteApi(org, bucket);
    
    let tempPoint = new Point('temperatura')
        .tag('device', device)
        .floatField('value', temperatura)
        .timestamp(date);
    writeApi.writePoint(tempPoint);

    let presencePoint = new Point('presencia')
        .tag('device', device)
        .booleanField('value', presencia)
        .timestamp(date);
    writeApi.writePoint(presencePoint);

    writeApi
        .close()
        .then(() => {
            console.log(
                `Nuevos datos fueron almacenados:\n\t${tempPoint} \n\t${presencePoint}\n`
            );
        })
        .catch(err => {
            console.error(
                `Error al almacenar datos: [${err}]`
            );
        });
};