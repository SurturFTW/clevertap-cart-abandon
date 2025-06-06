const { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const csv = require('csv-parser');
const { Readable } = require('stream');
const zlib = require('zlib');
const logger = require('../config/logger');
const config = require('../config/config');
const { createObjectCsvStringifier } = require('csv-writer');

// Validate AWS credentials with debug logging
function validateAwsCredentials() {
    const { aws } = config;
    if (!aws.accessKeyId || !aws.secretAccessKey || !aws.region) {
        logger.error('Missing required AWS credentials');
        throw new Error('Missing AWS credentials');
    }
    logger.info('AWS credentials validation passed');
}

// Initialize S3 client with credential check
function createS3Client() {
    validateAwsCredentials();

    return new S3Client({
        region: config.aws.region,
        credentials: {
            accessKeyId: config.aws.accessKeyId,
            secretAccessKey: config.aws.secretAccessKey
        }
    });
}

const s3Client = createS3Client();

async function fetchCsvFromS3(bucket, key) {
    const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key
    });

    const response = await s3Client.send(command);
    const stream = response.Body;

    const results = [];
    return new Promise((resolve, reject) => {
        let dataStream = Readable.from(stream);

        // If file is gzipped, pipe through gunzip
        if (key.endsWith('.gz')) {
            dataStream = dataStream.pipe(zlib.createGunzip());
        }

        dataStream
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('error', (error) => {
                console.error('Error processing stream:', error);
                reject(error);
            })
            .on('end', () => {
                console.log(`Successfully processed ${results.length} rows from ${key}`);
                resolve(results);
            });
    });
}

async function uploadToDeltaBucket(data, filename) {
    const command = new PutObjectCommand({
        Bucket: process.env.S3_DELTA_EVENTS_BUCKET,
        Key: filename,
        Body: data,
        ContentType: 'text/csv'
    });

    return s3Client.send(command);
}

async function listObjects(bucket) {
    try {
        if (!bucket) {
            throw new Error('Bucket name is required');
        }

        const command = new ListObjectsV2Command({
            Bucket: bucket
        });

        const response = await s3Client.send(command);
        logger.info(`Successfully listed objects in bucket: ${bucket}`);
        return response.Contents || [];
    } catch (error) {
        logger.error(`Error listing objects in bucket ${bucket}:`, error);
        throw error;
    }
}

async function uploadCsv(bucket, key, data) {
    try {
        logger.info(`Preparing to upload CSV to ${bucket}/${key}`);

        // Create CSV stringifier
        const csvStringifier = createObjectCsvStringifier({
            header: Object.keys(data[0]).map(key => ({
                id: key,
                title: key
            }))
        });

        // Convert data to CSV format
        const csvString = csvStringifier.getHeaderString() +
            csvStringifier.stringifyRecords(data);

        // Upload to S3
        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: csvString,
            ContentType: 'text/csv'
        });

        await s3Client.send(command);
        logger.info(`Successfully uploaded CSV to ${bucket}/${key}`);

        return {
            bucket,
            key,
            recordCount: data.length
        };
    } catch (error) {
        logger.error(`Error uploading CSV to ${bucket}/${key}:`, error);
        throw error;
    }
}

async function downloadCsv(bucket, key) {
    try {
        logger.info(`Downloading CSV from ${bucket}/${key}`);

        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key
        });

        const response = await s3Client.send(command);
        const stream = response.Body;

        const results = [];

        return new Promise((resolve, reject) => {
            Readable.from(stream)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('error', (error) => {
                    logger.error(`Error parsing CSV from ${bucket}/${key}:`, error);
                    reject(error);
                })
                .on('end', () => {
                    logger.info(`Successfully downloaded and parsed ${results.length} records from ${bucket}/${key}`);
                    resolve(results);
                });
        });
    } catch (error) {
        logger.error(`Error downloading CSV from ${bucket}/${key}:`, error);
        throw error;
    }
}

// Update module exports
module.exports = {
    downloadCsv,
    fetchCsvFromS3,
    uploadCsv,
    listObjects
};