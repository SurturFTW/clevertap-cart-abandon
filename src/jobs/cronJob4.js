const s3Service = require("../services/s3Service");
const csvProcessor = require("../services/csvProcessor");
const cleverTapService = require("../services/cleverTapService");
const logger = require("../config/logger");
require("dotenv").config();

class CronJob4 {
  constructor() {
    this.MAX_ITEMS_PER_PROFILE = 5;
    this.REVERSE_ORDER = true;
  }

  async execute() {
    try {
      // Fetch latest delta data
      const deltaData = await this.fetchLatestDeltaData(
        process.env.S3_DELTA_EVENTS_BUCKET
      );

      if (deltaData.length === 0) {
        logger.info("No most viewed delta data found for CleverTap upload");
        return;
      }

      // Group by identity
      const groupedData = {};
      deltaData.forEach((row) => {
        const identity = row["profile.identity"];
        if (!groupedData[identity]) {
          groupedData[identity] = [];
        }
        groupedData[identity].push({
          product_id: row["eventProps.ID"],
          view_count: row["eventProps.view_count"],
          price: row["eventProps.Price"],
          title: row["eventProps.Title"],
          // url: row["eventProps.URL"],
        });
      });

      // Create CleverTap payloads
      const profiles = Object.entries(groupedData).map(([identity, items]) => {
        if (this.REVERSE_ORDER) {
          items.sort((a, b) => b.view_count - a.view_count);
        }

        const limitedItems = items.slice(0, this.MAX_ITEMS_PER_PROFILE);

        const profile = {
          identity,
          ts: Math.floor(Date.now() / 1000),
          type: "event",
          evtName: "MostProductViewed",
          evtData: {},
        };

        limitedItems.forEach((item, index) => {
          profile.evtData[`product_id_${index}`] = item.product_id;
          profile.evtData[`view_count_${index}`] = item.view_count;
          profile.evtData[`price_${index}`] = item.price;
          profile.evtData[`title_${index}`] = item.title;
          profile.evtData[`url_${index}`] = item.url;
        });

        return profile;
      });

      // Send to CleverTap
      const results = await cleverTapService.batchSendProfiles(
        profiles,
        "MostViewedItem"
      );

      logger.info("CleverTap upload results:", {
        totalProfiles: profiles.length,
        successful: results.success,
        failed: results.failed,
        errors: results.errors,
      });
    } catch (error) {
      logger.error("Cron Job 4 failed:", error);
      throw error;
    }
  }

  // Similar to CronJob2's fetchLatestDeltaData but looks for most_viewed_delta files
  async fetchLatestDeltaData(bucketName) {
    try {
      const objects = await s3Service.listObjects(bucketName);
      const today = new Date().toISOString().split("T")[0];

      const todaysFiles = objects.filter(
        (obj) =>
          obj.Key.startsWith("most_viewed_delta_") && obj.Key.includes(today)
      );

      if (todaysFiles.length === 0) return [];

      const latestFile = todaysFiles.sort(
        (a, b) => new Date(b.LastModified) - new Date(a.LastModified)
      )[0];

      return await s3Service.downloadCsv(bucketName, latestFile.Key);
    } catch (error) {
      logger.error("Error fetching most viewed delta data:", error);
      throw error;
    }
  }
}

module.exports = new CronJob4();
