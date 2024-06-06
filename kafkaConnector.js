const { Kafka, logLevel, Partitioners } = require('kafkajs');

class KafkaConnector {
    constructor(brokers, clientId) {
        this.kafka = new Kafka({
            clientId: clientId,
            brokers: brokers,
            logLevel: logLevel.WARN
        });
        this.producer = this.kafka.producer({
            createPartitioner: Partitioners.LegacyPartitioner
        });
    }

    async connect() {
        await this.producer.connect();
    }

    async sendMessage(topic, message) {
        try {
            await this.producer.send({
                topic: topic,
                messages: [{ value: message }]
            });
            return { status: 'success', message: `Message sent to topic ${topic}` };
        } catch (error) {
            return { status: 'error', message: error.message };
        }
    }

    async disconnect() {
        await this.producer.disconnect();
    }
}

module.exports = KafkaConnector;

