const axios = require('axios');
const logger = require('../config/logger');
require('dotenv').config();

class CleverTapService {
    constructor() {
        this.baseURL = 'https://api.clevertap.com/1/upload';
        this.accountId = process.env.CLEVERTAP_ACCOUNT_ID;
        this.passcode = process.env.CLEVERTAP_PASSCODE;
        this.maxRetries = 3;
        this.retryDelay = 1000; // 1 second
    }

    async sendProfileData(profileData) {
        const payload = {
            d: [{
                identity: profileData.identity,
                type: "event",
                evtName: "TotalItemsInCart",
                evtData: profileData.evtData
            }]
        };

        return this.makeRequest(payload);
    }
    //call user properties API to send user properties

    async makeRequest(payload, attempt = 1) {
        try {
            const response = await axios.post(this.baseURL, payload, {
                headers: {
                    'X-CleverTap-Account-Id': this.accountId,
                    'X-CleverTap-Passcode': this.passcode,
                    'Content-Type': 'application/json'
                },
                timeout: 10000 // 10 seconds timeout
            });

            logger.info(`CleverTap API success for identity: ${payload.d[0].identity}`);
            return response.data;
        } catch (error) {
            logger.error(`CleverTap API error (attempt ${attempt}):`, {
                identity: payload.d[0].identity,
                error: error.message,
                status: error.response?.status
            });

            if (attempt < this.maxRetries) {
                logger.info(`Retrying CleverTap API call for identity: ${payload.d[0].identity}`);
                await this.delay(this.retryDelay * attempt);
                return this.makeRequest(payload, attempt + 1);
            }

            throw error;
        }
    }

    async batchSendProfiles(consolidatedProfiles) {
        const results = {
            success: 0,
            failed: 0,
            errors: []
        };

        for (const profile of consolidatedProfiles) {
            try {
                logger.info(`Sending consolidated data for identity: ${profile.identity} with ${Object.keys(profile.evtData).length / 3} items`);
                await this.sendProfileData(profile);
                results.success++;

                // Small delay between requests to avoid rate limiting
                await this.delay(100);
            } catch (error) {
                results.failed++;
                results.errors.push({
                    identity: profile.identity,
                    error: error.message
                });
            }
        }

        return results;
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = new CleverTapService();