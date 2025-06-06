const s3Service = require('../services/s3Service');
const csvProcessor = require('../services/csvProcessor');
const logger = require('../config/logger');
require('dotenv').config();

class CronJob1 {
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

            // Get today's date in yyyymmdd format
            const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
            logger.info(`Searching for files with date: ${today}`);

            // Filter files by today's date and .csv.gz extension
            const todaysFiles = objects.filter(obj => {
                const fileName = obj.Key;
                // Match pattern: *-yyyymmdd-*-.csv.gz
                const datePattern = new RegExp(`-${today}-.*\\.csv\\.gz$`);
                return datePattern.test(fileName);
            });

            if (todaysFiles.length === 0) {
                logger.warn(`No .csv.gz files found for today (${today}) in bucket: ${bucketName}`);
                return [];
            }

            logger.info(`Found ${todaysFiles.length} files for today in ${bucketName}`);

            // Process all files for today
            const allData = [];
            for (const file of todaysFiles) {
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