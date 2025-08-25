const axios = require("axios");
const logger = require("../config/logger");
require("dotenv").config();

class CleverTapService {
  constructor() {
    this.baseURL = "https://api.clevertap.com/1/upload";
    this.accountId = process.env.CLEVERTAP_ACCOUNT_ID;
    this.passcode = process.env.CLEVERTAP_PASSCODE;
    this.maxRetries = 3;
    this.retryDelay = 1000; // 1 second
  }

  async sendProfileData(profileData, eventName = "TotalItemsInCart") {
    const payload = {
      d: [
        {
          identity: profileData.identity,
          type: "event",
          evtName: eventName,
          evtData: profileData.evtData,
        },
      ],
    };

    return this.makeRequest(payload);
  }
  //call user properties API to send user properties

  async makeRequest(payload, attempt = 1) {
    try {
      const response = await axios.post(this.baseURL, payload, {
        headers: {
          "X-CleverTap-Account-Id": this.accountId,
          "X-CleverTap-Passcode": this.passcode,
          "Content-Type": "application/json",
        },
        timeout: 10000, // 10 seconds timeout
      });

      const identities = payload.d.map((item) => item.identity).join(", ");
      logger.info(
        `CleverTap API success for batch with identities: ${identities}`
      );
      return response.data;
    } catch (error) {
      const identities = payload.d.map((item) => item.identity).join(", ");
      logger.error(`CleverTap API error (attempt ${attempt}):`, {
        identities: identities,
        error: error.message,
        status: error.response?.status,
      });

      if (attempt < this.maxRetries) {
        logger.info(
          `Retrying CleverTap API call for batch with identities: ${identities}`
        );
        await this.delay(this.retryDelay * attempt);
        return this.makeRequest(payload, attempt + 1);
      }

      throw error;
    }
  }

  async batchSendProfiles(
    consolidatedProfiles,
    eventName = "TotalItemsInCart"
  ) {
    const results = {
      success: 0,
      failed: 0,
      errors: [],
    };

    // Split profiles into batches of 500
    const batchSize = 500;
    const batches = [];

    for (let i = 0; i < consolidatedProfiles.length; i += batchSize) {
      batches.push(consolidatedProfiles.slice(i, i + batchSize));
    }

    logger.info(
      `Created ${batches.length} batches of profiles (max ${batchSize} per batch)`
    );

    // Process batches with concurrency limit
    const concurrencyLimit = 5;

    for (let i = 0; i < batches.length; i += concurrencyLimit) {
      const currentBatches = batches.slice(i, i + concurrencyLimit);
      const batchPromises = currentBatches.map((batch) =>
        this.processBatch(batch, eventName, results)
      );

      await Promise.all(batchPromises);
    }

    return results;
  }

  async processBatch(profiles, eventName, results) {
    try {
      const payload = {
        d: profiles.map((profile) => ({
          identity: profile.identity,
          type: "event",
          evtName: eventName,
          evtData: profile.evtData,
        })),
      };

      logger.info(
        `Sending ${eventName} batch with ${profiles.length} profiles`
      );
      await this.makeRequest(payload);
      results.success += profiles.length;
    } catch (error) {
      results.failed += profiles.length;
      results.errors.push({
        batch: `Batch of ${profiles.length} profiles`,
        error: error.message,
      });
    }
  }

  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = new CleverTapService();
