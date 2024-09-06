require('dotenv').config();
const express = require('express');
const { Sequelize } = require('sequelize');
const axios = require('axios');
const fs = require('fs');
const csvParser = require('csv-parser');
const path = require('path');

// Initialize
const app = express();
app.use(express.json());

// Database configuration
const sequelize = new Sequelize(process.env.DB_NAME, process.env.DB_USER, process.env.DB_PASSWORD, {
    host: process.env.DB_HOST,
    dialect: 'mysql',
    pool: {
        // acquire added due to timeout ad here we can implement bg process like child process for big file
        acquire: 24 * 60 * 60 * 1000,  
        idle:10000,  
        max: 50, 
        min: 0  
    }
});
const Comment = require('./models/comment')(sequelize, Sequelize.DataTypes);
const CsvData = require('./models/csvData')(sequelize, Sequelize.DataTypes);
sequelize.sync();

sequelize.authenticate().then(() => {
    console.log('Connection has been created successfully');
}).catch(err => {
    console.log('Unable to connect to the database', err);
});

// API to populate data
app.get('/populate', async (req, res) => {
    try {
        await Promise.all([
            processJsonData(),
            processCsvData(),
            processLargeCsvFile()
        ]);
        res.status(200).send('Data population complete.');
    } catch (error) {
        console.error('Error in populate route:', error);
        res.status(500).send('Error occurred during data population.');
    }
});

// Search data
app.post('/search', async (req, res) => {
    const { name, email, body, limit = 10, sort = 'id', page = 1 } = req.body;
    const whereClause = {};

    if (name) whereClause.name = name;
    if (email) whereClause.email = email;
    if (body) whereClause.body = body;

    const options = {
        where: whereClause,
        limit: parseInt(limit, 10),
        offset: (parseInt(page, 10) - 1) * parseInt(limit, 10),
        order: [[sort, 'ASC']],
    };

    try {
        const { count, rows } = await CsvData.findAndCountAll(options);
        res.json({
            results: rows,
            total: count,
            totalPages: Math.ceil(count / limit),
            currentPage: page
        });
    } catch (error) {
        console.error('Error fetching data:', error);
        res.status(500).send('Error fetching data');
    }
});

// Async function to process JSON data
async function processJsonData() {
    try {
        const jsonData = await axios.get(process.env.PROCESS_JSON);
        const existingRecords = await Comment.findAll({ attributes: ['id'] });
        const existingIds = new Set(existingRecords.map(record => record.id));
        const newRecords = jsonData.data.filter(record => !existingIds.has(record.id));

        if (newRecords.length > 0) {
            await Comment.bulkCreate(newRecords);
            console.log('JSON data processed successfully.');
        } else {
            console.log('No new records to insert.');
        }
    } catch (error) {
        console.error('Error processing JSON data:', error);
        throw error;
    }
}

// Async function to process large CSV file
async function processLargeCsvFile() {
    const bigCsvUrl = process.env.PROCESS_BIG_CSV;
    const bigCsvFilePath = path.join(__dirname, 'bigcsvdata.csv');
    const CHUNK_SIZE = 1000;

    try {
        // Stream download the large CSV file
        const bigCsvResponse = await axios.get(bigCsvUrl, { responseType: 'stream' });
        const bigCsvWriter = fs.createWriteStream(bigCsvFilePath);
        bigCsvResponse.data.pipe(bigCsvWriter);

        bigCsvWriter.on('finish', async () => {
            console.log('Big CSV file downloaded successfully.');

            let recordsBatch = [];
            let recordsCount = 0;

            const readStream = fs.createReadStream(bigCsvFilePath)
                .pipe(csvParser())
                .on('data', async (data) => {
                    try {
                        // Check if the record already exists
                        if (!await CsvData.findOne({ where: { id: data.id } })) {
                            recordsBatch.push(data);
                            recordsCount++;

                            // Process in chunks
                            if (recordsBatch.length >= CHUNK_SIZE) {
                                readStream.pause();
                                await CsvData.bulkCreate(recordsBatch, { updateOnDuplicate: ['name', 'email', 'body'] });
                                console.log(`Inserted ${recordsCount} records so far...`);
                                recordsBatch = [];
                                readStream.resume();
                            }
                        }
                    } catch (error) {
                        console.error('Error processing record:', error);
                    }
                })
                .on('end', async () => {
                    if (recordsBatch.length > 0) {
                        try {
                            await CsvData.bulkCreate(recordsBatch, { updateOnDuplicate: ['name', 'email', 'body'] });
                            console.log(`Final batch inserted, total records: ${recordsCount}`);
                        } catch (error) {
                            console.error('Error inserting final batch:', error);
                        }
                    }
                    console.log('Big CSV file processed successfully.');
                })
                .on('error', (error) => {
                    console.error('Error processing CSV file:', error);
                });
        });

    } catch (error) {
        console.error('Error processing large CSV file:', error);
    }
}


// Async function to process CSV data
async function processCsvData() {
    const csvUrl = process.env.PROCESS_CSV;
    const csvPath = path.join(__dirname, 'csvdata.csv');

    try {
        const response = await axios.get(csvUrl, { responseType: 'stream' });
        const writer = fs.createWriteStream(csvPath);
        response.data.pipe(writer);

        return new Promise((resolve, reject) => {
            writer.on('finish', async () => {
                const result = [];
                fs.createReadStream(csvPath)
                    .pipe(csvParser())
                    .on('data', (data) => result.push(data))
                    .on('end', async () => {
                        try {
                            await CsvData.bulkCreate(result, { updateOnDuplicate: ['name', 'email', 'body'] });
                            console.log('CSV data processed successfully.');
                            resolve();
                        } catch (error) {
                            console.error('Error inserting CSV data:', error);
                            reject(error);
                        }
                    })
                    .on('error', (error) => {
                        console.error('Error processing CSV file:', error);
                        reject(error);
                    });
            });
        });

    } catch (error) {
        console.error('Error processing CSV data:', error);
        throw error;
    }
}

// Start server
const PORT = 4000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
