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

      // Show detailed data for first 2 rows
      if (cartAbandonData.length > 0) {
        logger.info(
          "\nCart Abandon - Available fields:",
          Object.keys(cartAbandonData[0]).join(", ")
        );
        logger.info("\nDetailed Cart Abandon Data (up to 2 rows):");
        cartAbandonData.slice(0, 2).forEach((row, index) => {
          logger.info(`\nRow ${index + 1}:`);
          Object.entries(row).forEach(([key, value]) => {
            logger.info(`${key}: ${value}`);
          });
        });
      }

      // Log Charged Events Data details
      logger.info("\n=== Charged Events Data Details ===");
      logger.info(`Total records received: ${chargedEventsData.length}`);

      // Show detailed data for first 2 rows
      if (chargedEventsData.length > 0) {
        logger.info(
          "\nCharged Events - Available fields:",
          Object.keys(chargedEventsData[0]).join(", ")
        );
        logger.info("\nDetailed Charged Events Data (up to 2 rows):");
        chargedEventsData.slice(0, 2).forEach((row, index) => {
          logger.info(`\nRow ${index + 1}:`);
          Object.entries(row).forEach(([key, value]) => {
            logger.info(`${key}: ${value}`);
          });
        });
      }

      // First, filter out invalid records from cart abandon data
      const validCartAbandonData = cartAbandonData.filter((row) => {
        const hasValidIdentity =
          row["profile.identity"] && row["profile.identity"].trim() !== "";
        const hasValidProductId =
          row["eventProps.Product ID"] &&
          row["eventProps.Product ID"].trim() !== "";

        if (!hasValidIdentity || !hasValidProductId) {
          logger.debug("Skipping invalid cart abandon row:", {
            identity: row["profile.identity"],
            productId: row["eventProps.Product ID"],
          });
          return false;
        }
        return true;
      });

      logger.info(
        `Valid cart abandon records after null check: ${validCartAbandonData.length}`
      );

      // Create a Set of valid identity+product_id combinations from charged events
      const chargedCombinations = new Set(
        chargedEventsData
          .filter((row) => {
            // Check for identity in profile.identity only
            const hasValidIdentity =
              row["profile.identity"] && row["profile.identity"].trim() !== "";

            // Check for product ID in multiple possible fields
            const hasValidProductId =
              (row["eventProps.Items|product_id"] &&
                row["eventProps.Items|product_id"].trim() !== "") ||
              (row["eventProps.Items|product id"] &&
                row["eventProps.Items|product id"].trim() !== "") ||
              (row["eventProps.Product ID"] &&
                row["eventProps.Product ID"].trim() !== "");

            return hasValidIdentity && hasValidProductId;
          })
          .map((row) => {
            // Get identity from profile.identity only
            const identity =
              row["profile.identity"] && row["profile.identity"].trim();

            // Get product ID from available fields
            const productId =
              (row["eventProps.Items|product_id"] &&
                row["eventProps.Items|product_id"].trim()) ||
              (row["eventProps.Items|product id"] &&
                row["eventProps.Items|product id"].trim()) ||
              (row["eventProps.Product ID"] &&
                row["eventProps.Product ID"].trim());

            return `${identity}_${productId}`;
          })
      );

      logger.info(`Valid charged combinations: ${chargedCombinations.size}`);

      // Filter cart abandon data - only include entries NOT present in charged events
      const deltaData = validCartAbandonData.filter((row) => {
        const identity = row["profile.identity"].trim();
        const productId = row["eventProps.Product ID"]
          ? row["eventProps.Product ID"].trim()
          : "";
        const combination = `${identity}_${productId}`;

        const shouldInclude = !chargedCombinations.has(combination);

        if (!shouldInclude) {
          logger.debug(`Excluding matching combination: ${combination}`);
        }

        return shouldInclude;
      });

      // Remove any duplicates
      const uniqueDeltaData = _.uniqBy(
        deltaData,
        (row) =>
          `${row["profile.identity"].trim()}_${row[
            "eventProps.Product ID"
          ].trim()}`
      );

      // Logging summary
      logger.info("\n=== Processing Summary ===");
      logger.info(`Original cart abandon records: ${cartAbandonData.length}`);
      logger.info(`Valid cart abandon records: ${validCartAbandonData.length}`);
      logger.info(`Charged events records: ${chargedEventsData.length}`);
      logger.info(`Valid charged combinations: ${chargedCombinations.size}`);
      logger.info(`Delta records (before deduplication): ${deltaData.length}`);
      logger.info(`Final delta records: ${uniqueDeltaData.length}`);

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
