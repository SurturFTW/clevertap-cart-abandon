const s3Service = require('../services/s3Service');
const csvProcessor = require('../services/csvProcessor');
const logger = require('../config/logger');
require('dotenv').config();

class CronJob3 {
    constructor() {
        this.days = 1; // Default to 7 days
        this.MIN_VIEW_COUNT = 5; // Minimum views required
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
        logger.info('Starting Cron Job 3 - Product View Processing');

        try {
            // Step 1: Fetch latest CSV files from both buckets
            const productViewData = await this.fetchLatestCsvData(process.env.S3_PRODUCT_VIEW_BUCKET);
            const chargedEventsData = await this.fetchLatestCsvData(process.env.S3_CHARGED_EVENTS_BUCKET);

            // Validate data
            if (!productViewData || !Array.isArray(productViewData)) {
                logger.warn('No product view data found or invalid format');
                return;
            }

            if (!chargedEventsData || !Array.isArray(chargedEventsData)) {
                logger.warn('No charged events data found or invalid format');
                return;
            }

            logger.info(`Processing ${productViewData.length} product views and ${chargedEventsData.length} charged events`);

            // Step 2: Process and filter data
            const deltaData = await this.processProductViewData(productViewData, chargedEventsData);

            // Step 3: Generate CSV content and upload to delta bucket
            if (deltaData && deltaData.length > 0) {
                const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
                const fileName = `most_viewed_delta_${timestamp}.csv`;

                await s3Service.uploadCsv(
                    process.env.S3_DELTA_EVENTS_BUCKET,
                    fileName,
                    deltaData
                );

                logger.info(`Most viewed delta data uploaded successfully: ${fileName}`);
            } else {
                logger.info('No most viewed delta data to process. Cron Job 3 completed.');
            }

            logger.info(`Cron Job 3 completed successfully. Processed ${deltaData?.length || 0} records in ${Date.now() - startTime}ms`);
        } catch (error) {
            logger.error('Cron Job 3 failed:', error);
            throw error;
        }
    }

    async processProductViewData(productViewData, chargedEventsData) {
        try {
            if (!productViewData?.length || !chargedEventsData?.length) {
                logger.warn('No data to process');
                return [];
            }

            // Group by identity and product_id with complete row data
            const viewCounts = {};

            productViewData.forEach(row => {
                if (!row) return;

                const identity = row['profile.identity']?.trim();
                const productId = row['eventProps.product_id']?.trim();

                if (identity && productId) {
                    const key = `${identity}_${productId}`;
                    if (!viewCounts[key]) {
                        viewCounts[key] = {
                            identity,
                            productId,
                            count: 0,
                            originalRow: row // Store complete row
                        };
                    }
                    viewCounts[key].count++;
                }
            });

            logger.info(`Processed ${Object.keys(viewCounts).length} unique user-product combinations`);

            // Create Set of charged combinations
            const chargedCombinations = new Set(
                chargedEventsData
                    .filter(row => row && row['profile.identity'] && row['eventProps.product_id'])
                    .map(row => `${row['profile.identity'].trim()}_${row['eventProps.product_id'].trim()}`)
            );

            // Filter by view count and not in charged events
            const deltaData = Object.values(viewCounts)
                .filter(item => {
                    const combination = `${item.identity}_${item.productId}`;
                    return item.count >= this.MIN_VIEW_COUNT && !chargedCombinations.has(combination);
                })
                .map(item => ({
                    ...item.originalRow,                    // Spread the complete original row
                    'eventProps.view_count': item.count     // Add the view count
                }));

            logger.info(`Found ${deltaData.length} products with ${this.MIN_VIEW_COUNT}+ views`);
            return deltaData;
        } catch (error) {
            logger.error('Error processing product view data:', error);
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

module.exports = new CronJob3();