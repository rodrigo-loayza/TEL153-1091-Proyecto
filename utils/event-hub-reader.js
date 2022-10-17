/*
  Contiene código de https://github.com/Azure-Samples/web-apps-node-iot-hub-data-visualization
*/

const { EventHubConsumerClient } = require('@azure/event-hubs');
const { convertIotHubToEventHubsConnectionString } = require('./iot-hub-connection-string.js');

class EventHubReader {
  constructor(iotHubConnectionString, consumerGroup) {
    this.iotHubConnectionString = iotHubConnectionString;
    this.consumerGroup = consumerGroup;
  }

  async startReadMessage(startReadMessageCallback) {
    try {
      const eventHubConnectionString = await convertIotHubToEventHubsConnectionString(this.iotHubConnectionString);
      const consumerClient = new EventHubConsumerClient(this.consumerGroup, eventHubConnectionString);
      console.log('Successfully created the EventHubConsumerClient from IoT Hub event hub-compatible connection string.');

      const partitionIds = await consumerClient.getPartitionIds();
      console.log('The partition ids are: ', partitionIds);

      consumerClient.subscribe({
        processEvents: (events, context) => {
          for (let i = 0; i < events.length; ++i) {
            startReadMessageCallback(
              events[i].body,
              events[i].enqueuedTimeUtc,
              events[i].systemProperties["iothub-connection-device-id"]);
          }
        },
        processError: (err, context) => {
          console.error(err.message || err);
        }
      });
    } catch (ex) {
      console.error(ex.message || ex);
    }
  }

  async stopReadMessage() {
    const disposeHandlers = [];
    this.receiveHandlers.forEach((receiveHandler) => {
      disposeHandlers.push(receiveHandler.stop());
    });
    await Promise.all(disposeHandlers);

    this.consumerClient.close();
  }
}

module.exports = EventHubReader;
