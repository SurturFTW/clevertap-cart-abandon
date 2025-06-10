const s3Service = require('../services/s3Service');
const csvProcessor = require('../services/csvProcessor');
const logger = require('../config/logger');
require('dotenv').config();

class CronJob1 {
    constructor() {
        this.days = 1; // Default to 1 day
    }

    setDays(numberOfDays) {
        if (numberOfDays < 1) {
            throw new Error('Number of days must be at least 1');
        }
        this.days = numberOfDays;
        logger.info(`Set to process last ${this.days} days of data`);
    }

    async execute() {
        const startTime = Date.now();
        logger.info('Starting Cron Job 1 - Data Processing');

        try {
            // Step 1: Fetch latest CSV files from both buckets
            const cartAbandonData = await this.fetchLatestCsvData(process.env.S3_CART_ABANDON_BUCKET);
            const chargedEventsData = await this.fetchLatestCsvData(process.env.S3_CHARGED_EVENTS_BUCKET);

            // Step 2: Process and filter data
            const deltaData = await csvProcessor.processCartAbandonData(cartAbandonData, chargedEventsData);

            // Step 3: Generate CSV content and upload to delta bucket
            if (deltaData.length > 0) {
                const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
                const fileName = `delta_${timestamp}.csv`;

                await s3Service.uploadCsv(
                    process.env.S3_DELTA_EVENTS_BUCKET,
                    fileName,
                    deltaData
                );

                logger.info(`Delta data uploaded successfully: ${fileName}`);
            } else {
                logger.info('No delta data to process. Cron Job 1 completed.');
            }

            logger.info(`Cron Job 1 completed successfully. Processed ${deltaData.length} records in ${Date.now() - startTime}ms`);
        } catch (error) {
            logger.error('Cron Job 1 failed:', error);
            throw error;
        }
    }

    async fetchLatestCsvData(bucketName) {
        try {
            const objects = await s3Service.listObjects(bucketName);

            if (!objects || objects.length === 0) {
                logger.warn(`No files found in bucket: ${bucketName}`);
                return [];
            }

            // Generate array of dates to process
            const dates = [];
            for (let i = 0; i < this.days; i++) {
                const date = new Date();
                date.setDate(date.getDate() - i);
                const formattedDate = date.toISOString().split('T')[0].replace(/-/g, '');
                dates.push(formattedDate);
            }

            logger.info(`Searching for files with dates: ${dates.join(', ')}`);

            // Filter files by dates and .csv.gz extension
            const matchingFiles = objects.filter(obj => {
                const fileName = obj.Key;
                return dates.some(date => {
                    const datePattern = new RegExp(`-${date}-.*\\.csv\\.gz$`);
                    return datePattern.test(fileName);
                });
            });

            if (matchingFiles.length === 0) {
                logger.warn(`No .csv.gz files found for dates (${dates.join(', ')}) in bucket: ${bucketName}`);
                return [];
            }

            logger.info(`Found ${matchingFiles.length} files in ${bucketName}`);

            // Process all matching files
            const allData = [];
            for (const file of matchingFiles) {
                logger.info(`Processing file: ${file.Key}`);
                const fileData = await s3Service.fetchCsvFromS3(bucketName, file.Key);
                allData.push(...fileData);
            }

            logger.info(`Total records processed from ${bucketName}: ${allData.length}`);
            return allData;
        } catch (error) {
            logger.error(`Error fetching CSV data from ${bucketName}:`, error);
            throw error;
        }
    }
}

module.exports = new CronJob1();