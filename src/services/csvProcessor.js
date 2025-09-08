const csvWriter = require("csv-writer");
const _ = require("lodash");
const logger = require("../config/logger");
const fs = require("fs").promises;
const path = require("path");

class CsvProcessor {
  async processCartAbandonData(cartAbandonData, chargedEventsData) {
    try {
      // Log Cart Abandon Data details
      logger.info("\n=== Cart Abandon Data Details ===");
      logger.info(`Total records received: ${cartAbandonData.length}`);

      // Show sample data for debugging
      if (cartAbandonData.length > 0) {
        logger.info("Sample Cart Abandon record:");
        logger.info(`Identity: ${cartAbandonData[0]["profile.identity"]}`);
        logger.info(
          `Product ID: ${cartAbandonData[0]["eventProps.Product ID"]}`
        );
      }

      // Log Charged Events Data details
      logger.info("\n=== Charged Events Data Details ===");
      logger.info(`Total records received: ${chargedEventsData.length}`);

      if (chargedEventsData.length > 0) {
        logger.info("Sample Charged record:");
        logger.info(`Identity: ${chargedEventsData[0]["profile.identity"]}`);
        logger.info(
          `Product ID fields available: ${
            chargedEventsData[0]["eventProps.Items|product_id"] || "N/A"
          }, ${chargedEventsData[0]["eventProps.Items|product id"] || "N/A"}, ${
            chargedEventsData[0]["eventProps.Product ID"] || "N/A"
          }`
        );
      }

      // First, filter out invalid records from cart abandon data
      const validCartAbandonData = cartAbandonData.filter((row) => {
        const hasValidIdentity =
          row["profile.identity"] && row["profile.identity"].trim() !== "";
        const hasValidProductId =
          row["eventProps.Product ID"] &&
          row["eventProps.Product ID"].trim() !== "";

        return hasValidIdentity && hasValidProductId;
      });

      logger.info(`Valid cart abandon records: ${validCartAbandonData.length}`);

      // Create a Set of valid identity+product_id combinations from charged events
      const chargedCombinations = new Set();
      const chargedDebugInfo = [];

      chargedEventsData.forEach((row) => {
        // Normalize identity (remove spaces, convert to string)
        const identity = row["profile.identity"]
          ? row["profile.identity"].toString().trim()
          : null;

        if (!identity) return;

        // Get product ID from available fields
        let productIds = [];

        // Check simple fields first
        const simpleProductId =
          (row["eventProps.Items|product_id"] &&
            row["eventProps.Items|product_id"].toString().trim()) ||
          (row["eventProps.Items|product id"] &&
            row["eventProps.Items|product id"].toString().trim()) ||
          (row["eventProps.Product ID"] &&
            row["eventProps.Product ID"].toString().trim());

        if (simpleProductId) {
          productIds.push(simpleProductId);
        }

        // Parse Items JSON array if it exists
        if (row["eventProps.Items"]) {
          try {
            const items = JSON.parse(row["eventProps.Items"]);
            if (Array.isArray(items)) {
              items.forEach((item) => {
                if (item.product_id) {
                  productIds.push(item.product_id.toString());
                }
              });
            }
          } catch (error) {
            logger.warn(
              `Failed to parse Items JSON for identity ${identity}:`,
              error.message
            );
          }
        }

        // Add all found product IDs to the charged combinations
        productIds.forEach((productId) => {
          if (productId) {
            const combination = `${identity}_${productId}`;
            chargedCombinations.add(combination);
            chargedDebugInfo.push({ identity, productId, combination });
          }
        });
      });

      logger.info(`Valid charged combinations: ${chargedCombinations.size}`);

      // Log first few charged combinations for debugging
      if (chargedDebugInfo.length > 0) {
        logger.info(
          "Sample charged combinations:",
          chargedDebugInfo.slice(0, 3)
        );
      }

      // Filter cart abandon data - only include entries NOT present in charged events
      const deltaData = [];
      const excludedData = [];

      validCartAbandonData.forEach((row) => {
        // Normalize identity (remove spaces, convert to string)
        const identity = row["profile.identity"].toString().trim();
        const productId = row["eventProps.Product ID"].toString().trim();
        const combination = `${identity}_${productId}`;

        if (chargedCombinations.has(combination)) {
          excludedData.push({ identity, productId, combination });
          logger.debug(`Excluding matching combination: ${combination}`);
        } else {
          deltaData.push(row);
          logger.debug(`Including combination: ${combination}`);
        }
      });

      logger.info(
        `\nExcluded ${excludedData.length} combinations that were charged`
      );
      if (excludedData.length > 0) {
        logger.info("Sample excluded combinations:", excludedData.slice(0, 3));
      }

      // Remove any duplicates
      const uniqueDeltaData = _.uniqBy(
        deltaData,
        (row) =>
          `${row["profile.identity"].toString().trim()}_${row[
            "eventProps.Product ID"
          ]
            .toString()
            .trim()}`
      );

      // Logging summary
      logger.info("\n=== Processing Summary ===");
      logger.info(`Original cart abandon records: ${cartAbandonData.length}`);
      logger.info(`Valid cart abandon records: ${validCartAbandonData.length}`);
      logger.info(`Charged events records: ${chargedEventsData.length}`);
      logger.info(`Valid charged combinations: ${chargedCombinations.size}`);
      logger.info(`Delta records (before deduplication): ${deltaData.length}`);
      logger.info(`Final delta records: ${uniqueDeltaData.length}`);

      // Log sample final delta data for verification
      if (uniqueDeltaData.length > 0) {
        logger.info("\nSample final delta record:");
        const sample = uniqueDeltaData[0];
        logger.info(`Identity: ${sample["profile.identity"]}`);
        logger.info(`Product ID: ${sample["eventProps.Product ID"]}`);
        logger.info(`Event: ${sample["eventName"]}`);
      }

      return uniqueDeltaData;
    } catch (error) {
      logger.error("Error processing cart abandon data:", error);
      throw error;
    }
  }

  async generateCsvContent(data) {
    try {
      if (!data || data.length === 0) {
        logger.warn("No data to generate CSV content");
        return "";
      }

      // Get all unique headers from the data
      const headers = Object.keys(data[0]);

      // Create CSV content manually to avoid file system operations
      let csvContent = headers.join(",") + "\n";

      data.forEach((row) => {
        const values = headers.map((header) => {
          const value = row[header] || "";
          // Escape commas and quotes in CSV values
          if (
            value.toString().includes(",") ||
            value.toString().includes('"')
          ) {
            return `"${value.toString().replace(/"/g, '""')}"`;
          }
          return value;
        });
        csvContent += values.join(",") + "\n";
      });

      return csvContent;
    } catch (error) {
      logger.error("Error generating CSV content:", error);
      throw error;
    }
  }

  extractCleverTapData(row) {
    logger.debug("Raw row data:", row);

    // Verify required fields exist
    if (!row["profile.identity"]) {
      logger.warn("Missing required identity field in row");
      return null;
    }

    // Get product ID from available fields, with fallbacks
    const productId =
      (row["eventProps.Product ID"] ? row["eventProps.Product ID"] : "") ||
      (row["eventProps.Items|product_id"]
        ? row["eventProps.Items|product_id"]
        : "") ||
      (row["eventProps.Items|product id"]
        ? row["eventProps.Items|product id"]
        : "");

    if (!productId) {
      logger.warn("Missing required product_id field in row");
      return null;
    }

    // Extract price from available fields
    const price =
      (row["eventProps.price"] ? row["eventProps.price"] : "") ||
      (row["eventProps.Price"] ? row["eventProps.Price"] : "") ||
      (row["eventProps.Items|price"] ? row["eventProps.Items|price"] : "") ||
      (row["eventProps.Items|unit_price"]
        ? row["eventProps.Items|unit_price"]
        : "");

    // Extract image URL from available fields
    const imageUrl =
      (row["eventProps.image_url"] ? row["eventProps.image_url"] : "") ||
      (row["eventProps.Image_url"] ? row["eventProps.Image_url"] : "") ||
      (row["eventProps.Image Url"] ? row["eventProps.Image Url"] : "") ||
      (row["eventProps.Items|image_url"]
        ? row["eventProps.Items|image_url"]
        : "") ||
      (row["eventProps.Items|img_url"] ? row["eventProps.Items|img_url"] : "");

    // Extract product title/name from available fields
    const productTitle =
      (row["eventProps.item_name"] ? row["eventProps.item_name"] : "") ||
      (row["eventProps.Items|item_name"]
        ? row["eventProps.Items|item_name"]
        : "") ||
      (row["eventProps.Items|title"] ? row["eventProps.Items|title"] : "") ||
      (row["eventProps.Items|item_title"]
        ? row["eventProps.Items|item_title"]
        : "") ||
      (row["eventProps.Title"] ? row["eventProps.Title"] : "") ||
      (row["eventProps.title"] ? row["eventProps.title"] : "");

    // Extract user email if available
    const email =
      (row["profile.email"] ? row["profile.email"] : "") ||
      (row["eventProps.email"] ? row["eventProps.email"] : "") ||
      (row["eventProps.customer email"]
        ? row["eventProps.customer email"]
        : "");

    // Extract phone if available
    const phone =
      (row["profile.phone"] ? row["profile.phone"] : "") ||
      (row["eventProps.phone"] ? row["eventProps.phone"] : "") ||
      (row["eventProps.customer phone"]
        ? row["eventProps.customer phone"]
        : "");

    return {
      identity: row["profile.identity"],
      product_id: productId,
      price: price,
      image_url: imageUrl,
      product_title: productTitle,
      email: email,
      phone: phone,
    };
  }
}

module.exports = new CsvProcessor();
