const csvWriter = require('csv-writer');
const _ = require('lodash');
const logger = require('../config/logger');
const fs = require('fs').promises;
const path = require('path');

class CsvProcessor {
    async processCartAbandonData(cartAbandonData, chargedEventsData) {
        try {
            // Log Cart Abandon Data details
            logger.info('\n=== Cart Abandon Data Details ===');
            logger.info(`Total records received: ${cartAbandonData.length}`);

            // Show detailed data for first 2 rows
            if (cartAbandonData.length > 0) {
                logger.info('\nCart Abandon - Available fields:', Object.keys(cartAbandonData[0]).join(', '));
                logger.info('\nDetailed Cart Abandon Data (up to 2 rows):');
                cartAbandonData.slice(0, 2).forEach((row, index) => {
                    logger.info(`\nRow ${index + 1}:`);
                    Object.entries(row).forEach(([key, value]) => {
                        logger.info(`${key}: ${value}`);
                    });
                });
            }

            // Log Charged Events Data details
            logger.info('\n=== Charged Events Data Details ===');
            logger.info(`Total records received: ${chargedEventsData.length}`);

            // Show detailed data for first 2 rows
            if (chargedEventsData.length > 0) {
                logger.info('\nCharged Events - Available fields:', Object.keys(chargedEventsData[0]).join(', '));
                logger.info('\nDetailed Charged Events Data (up to 2 rows):');
                chargedEventsData.slice(0, 2).forEach((row, index) => {
                    logger.info(`\nRow ${index + 1}:`);
                    Object.entries(row).forEach(([key, value]) => {
                        logger.info(`${key}: ${value}`);
                    });
                });
            }

            // First, filter out invalid records from cart abandon data
            const validCartAbandonData = cartAbandonData.filter(row => {
                const hasValidIdentity = row['profile.identity'] && row['profile.identity'].trim() !== '';
                const hasValidProductId = row['eventProps.product_id'] && row['eventProps.product_id'].trim() !== '';

                if (!hasValidIdentity || !hasValidProductId) {
                    logger.debug('Skipping invalid cart abandon row:', {
                        identity: row['profile.identity'],
                        productId: row['eventProps.product_id']
                    });
                    return false;
                }
                return true;
            });

            logger.info(`Valid cart abandon records after null check: ${validCartAbandonData.length}`);

            // Create a Set of valid identity+product_id combinations from charged events
            const chargedCombinations = new Set(
                chargedEventsData
                    .filter(row => {
                        const hasValidIdentity = row['profile.identity'] && row['profile.identity'].trim() !== '';
                        const hasValidProductId = row['eventProps.product_id'] && row['eventProps.product_id'].trim() !== '';
                        return hasValidIdentity && hasValidProductId;
                    })
                    .map(row => `${row['profile.identity'].trim()}_${row['eventProps.product_id'].trim()}`)
            );

            logger.info(`Valid charged combinations: ${chargedCombinations.size}`);

            // Filter cart abandon data - only include entries NOT present in charged events
            const deltaData = validCartAbandonData.filter(row => {
                const identity = row['profile.identity'].trim();
                const productId = row['eventProps.product_id'].trim();
                const combination = `${identity}_${productId}`;

                const shouldInclude = !chargedCombinations.has(combination);

                if (!shouldInclude) {
                    logger.debug(`Excluding matching combination: ${combination}`);
                }

                return shouldInclude;
            });

            // Remove any duplicates
            const uniqueDeltaData = _.uniqBy(deltaData, row =>
                `${row['profile.identity'].trim()}_${row['eventProps.product_id'].trim()}`
            );

            // Logging summary
            logger.info('\n=== Processing Summary ===');
            logger.info(`Original cart abandon records: ${cartAbandonData.length}`);
            logger.info(`Valid cart abandon records: ${validCartAbandonData.length}`);
            logger.info(`Charged events records: ${chargedEventsData.length}`);
            logger.info(`Valid charged combinations: ${chargedCombinations.size}`);
            logger.info(`Delta records (before deduplication): ${deltaData.length}`);
            logger.info(`Final delta records: ${uniqueDeltaData.length}`);

            return uniqueDeltaData;
        } catch (error) {
            logger.error('Error processing cart abandon data:', error);
            throw error;
        }
    }

    async generateCsvContent(data) {
        try {
            if (!data || data.length === 0) {
                logger.warn('No data to generate CSV content');
                return '';
            }

            // Get all unique headers from the data
            const headers = Object.keys(data[0]);

            // Create CSV content manually to avoid file system operations
            let csvContent = headers.join(',') + '\n';

            data.forEach(row => {
                const values = headers.map(header => {
                    const value = row[header] || '';
                    // Escape commas and quotes in CSV values
                    if (value.toString().includes(',') || value.toString().includes('"')) {
                        return `"${value.toString().replace(/"/g, '""')}"`;
                    }
                    return value;
                });
                csvContent += values.join(',') + '\n';
            });

            return csvContent;
        } catch (error) {
            logger.error('Error generating CSV content:', error);
            throw error;
        }
    }

    extractCleverTapData(row) {
        // Verify required fields exist
        if (!row['profile.identity'] || !row['eventProps.product_id']) {
            logger.warn('Missing required fields in row:', {
                hasIdentity: !!row['profile.identity'],
                hasProductId: !!row['eventProps.product_id']
            });
            return null;
        }

        return {
            identity: row['profile.identity'],
            product_id: row['eventProps.product_id'],
            price: row['eventProps.price'] || '',
            image_url: row['eventProps.image_url'] || ''
        };
    }
}

module.exports = new CsvProcessor();