const express = require('express');
const bodyParser = require('body-parser');
const KafkaConnector = require('./kafkaConnector');

const app = express();
const port = process.env.PORT || 3000;

app.use(bodyParser.json());

let kafkaConnector;

app.post('/connect', async (req, res) => {
    const { brokers, clientId } = req.body;
    if (!brokers || brokers.length === 0) {
        return res.status(400).send({ status: 'error', message: 'Brokers should not be null or empty' });
    }
    if (!clientId) {
        return res.status(400).send({ status: 'error', message: 'Client ID should not be null or empty' });
    }
    kafkaConnector = new KafkaConnector(brokers, clientId);
    await kafkaConnector.connect();
    res.send({ status: 'connected' });
});

app.post('/send', async (req, res) => {
    const { topic, message } = req.body;
    const response = await kafkaConnector.sendMessage(topic, message);
    res.send(response);
});

app.post('/disconnect', async (req, res) => {
    await kafkaConnector.disconnect();
    res.send({ status: 'disconnected' });
});

app.listen(port, () => {
    console.log(`Kafka Connector API listening at http://localhost:${port}`);
});

