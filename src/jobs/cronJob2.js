const s3Service = require("../services/s3Service");
const csvProcessor = require("../services/csvProcessor");
const cleverTapService = require("../services/cleverTapService");
const logger = require("../config/logger");
require("dotenv").config();

// Add hard limit of 5
// Show recent add to cart - reverse list
class CronJob2 {
  constructor() {
    this.MAX_ITEMS_PER_PROFILE = 5;
    this.REVERSE_ORDER = true; // Toggle this to change order
  }

  async execute() {
    try {
      // Step 1: Fetch latest delta CSV data
      const deltaData = await this.fetchLatestDeltaData(
        process.env.S3_DELTA_EVENTS_BUCKET
      );

      if (deltaData.length === 0) {
        logger.info("No delta data found for CleverTap upload");
        return;
      }

      // Step 2: Extract and validate data
      const profiles = deltaData
        .filter(
          (row) => row["profile.identity"] && row["eventProps.Product ID"]
        )
        .map((row) => csvProcessor.extractCleverTapData(row))
        .filter((profile) => profile !== null);

      logger.info(
        `Extracted ${profiles.length} valid profiles from ${deltaData.length} records`
      );

      // Log sample data for verification
      if (profiles.length > 0) {
        logger.info(
          "Sample profile data:",
          JSON.stringify(profiles[0], null, 2)
        );
      }

      // Group profiles by identity
      const groupedProfiles = {};
      profiles.forEach((profile) => {
        if (!groupedProfiles[profile.identity]) {
          groupedProfiles[profile.identity] = [];
        }
        groupedProfiles[profile.identity].push(profile);
      });

      logger.info(
        `Grouped ${profiles.length} records into ${
          Object.keys(groupedProfiles).length
        } unique identities`
      );

      // Create consolidated profiles with limit
      const consolidatedProfiles = Object.entries(groupedProfiles).map(
        ([identity, items]) => {
          // Sort items if needed
          if (this.REVERSE_ORDER) {
            items = items.reverse();
          }

          // Take only the first 5 items (now most recent will be first if reversed)
          const limitedItems = items.slice(0, this.MAX_ITEMS_PER_PROFILE);

          const consolidated = {
            identity: identity,
            evtData: {},
          };

          limitedItems.forEach((item, index) => {
            consolidated.evtData[`product_id_${index}`] = item.product_id;
            consolidated.evtData[`price_${index}`] = item.price;
            // consolidated.evtData[`image_url_${index}`] = item.image_url;
            consolidated.evtData[`title_${index}`] = item.product_title;
          });

          // Log if items were truncated
          if (items.length > this.MAX_ITEMS_PER_PROFILE) {
            const order = this.REVERSE_ORDER
              ? "newest to oldest"
              : "oldest to newest";
            logger.info(
              `Profile ${identity} had ${items.length} items, truncated to ${this.MAX_ITEMS_PER_PROFILE} (${order})`
            );
          }

          return consolidated;
        }
      );

      // Log sample consolidated data
      if (consolidatedProfiles.length > 0) {
        logger.info(
          "Sample consolidated profile:",
          JSON.stringify(consolidatedProfiles[0], null, 2)
        );
      }

      // Step 3: Send to CleverTap
      const results = await cleverTapService.batchSendProfiles(
        consolidatedProfiles
      );

      logger.info("CleverTap upload results:", {
        uniqueIdentities: Object.keys(groupedProfiles).length,
        totalRecords: profiles.length,
        successful: results.success,
        failed: results.failed,
        errors: results.errors,
      });
    } catch (error) {
      logger.error("Cron Job 2 failed:", error);
      throw error;
    }
  }

  async fetchLatestDeltaData(bucketName) {
    try {
      const objects = await s3Service.listObjects(bucketName);

      if (!objects || objects.length === 0) {
        logger.warn(`No files found in bucket: ${bucketName}`);
        return [];
      }

      // Get today's date in YYYY-MM-DD format
      const today = new Date().toISOString().split("T")[0];

      // Filter for today's CSV files with the new naming format
      const todaysFiles = objects.filter((obj) => {
        const fileName = obj.Key;
        // Match files like delta_2025-06-04T12-44-02-619Z.csv
        return fileName.endsWith(".csv") && fileName.includes(today);
      });

      if (todaysFiles.length === 0) {
        logger.warn(
          `No CSV files found for today (${today}) in bucket: ${bucketName}`
        );
        return [];
      }

      // Get the most recent file
      const latestFile = todaysFiles.sort(
        (a, b) => new Date(b.LastModified) - new Date(a.LastModified)
      )[0];

      logger.info(`Processing latest delta file: ${latestFile.Key}`);
      return await s3Service.downloadCsv(bucketName, latestFile.Key);
    } catch (error) {
      logger.error("Error fetching delta CSV data:", error);
      throw error;
    }
  }

  // Optional: Method to change order at runtime
  setReverseOrder(reverse) {
    this.REVERSE_ORDER = reverse;
    logger.info(
      `Set item order to: ${reverse ? "newest to oldest" : "oldest to newest"}`
    );
  }
}

module.exports = new CronJob2();
