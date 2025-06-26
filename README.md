# S3 Delta CleverTap Integration

## Overview

This Node.js application processes cart abandonment and product view data from S3 buckets, sending consolidated user activity to CleverTap. It runs four scheduled jobs daily to:

1. Track abandoned cart items
2. Monitor frequently viewed products
3. Update user profiles in CleverTap

## Features

- 🛒 Cart abandonment tracking
- 👁️ Product view frequency analysis (5+ views)
- 📊 Multi-day historical data processing
- 👤 User-based data consolidation
- 📝 Comprehensive logging
- 🔄 Automatic retry mechanism
- ⚡ Optimized API calls

## Prerequisites

- Node.js v14+
- AWS S3 bucket access
- CleverTap account

## Quick Start

### 1. Installation

```bash
# Clone repository
git clone <repository-url>
cd s3-delta-clevertap-app

# Install dependencies
npm install
```

### 2. Environment Configuration

Create `.env` file in root directory:

```properties
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=your_region_here

# S3 Buckets
S3_CART_ABANDON_BUCKET=your_cart_abandon_bucket
S3_CHARGED_EVENTS_BUCKET=your_charged_events_bucket
S3_DELTA_EVENTS_BUCKET=your_delta_events_bucket
S3_PRODUCT_VIEW_BUCKET=your_product_view_bucket

# CleverTap Configuration
CLEVERTAP_ACCOUNT_ID=your_clevertap_account_id
CLEVERTAP_PASSCODE=your_clevertap_passcode
CLEVERTAP_API_ENDPOINT=https://api.clevertap.com/1
```

### 3. Start Application

```bash
node src/index.js
```

## Jobs Overview

### Cart Abandonment Pipeline

- **CronJob1** (custom PM IST): Processes cart data vs charged events
- **CronJob2** (custom PM IST): Sends `TotalItemsInCart` events to CleverTap (event name is `customizable`)

### Product View Pipeline

- **CronJob3** (custom PM IST): Processes product views vs charged events
- **CronJob4** (custom PM IST): Sends `MostViewedItem` events to CleverTap (event name is `customizable`)

### Processing Settings

> **Note:**  
> All processing settings, event names, and the structure of `evtData` are fully customizable to fit your requirements.  
> You can adjust the number of days, item limits, sorting order, minimum view thresholds, event names, and the fields included in `evtData` by editing the respective job files.

Cart Abandonment (`cronJob1.js` & `cronJob2.js`):

```javascript
this.days = 7; // Historical data days (customizable)
this.MAX_ITEMS_PER_PROFILE = 5; // Items per user (customizable)
this.REVERSE_ORDER = true; // Newest first (customizable)
```

Product Views (`cronJob3.js` & `cronJob4.js`):

```javascript
this.days = 7; // Historical data days (customizable)
this.MIN_VIEW_COUNT = 5; // View threshold (customizable)
```

## CleverTap Events

> **Note:**  
> The event names and the structure of `evtData` shown below are examples.  
> You can customize both the event name and the fields sent in `evtData` as per your CleverTap integration needs.

### TotalItemsInCart Event (example)

```json
{
  "identity": "user123",
  "evtData": {
    "product_id_0": "item1",
    "price_0": "99.99",
    "image_url_0": "url1"
  }
}
```

### MostViewedItem Event (example)

```json
{
  "identity": "user123",
  "evtData": {
    "product_id": "item1",
    "view_count": 5,
    "price": "99.99",
    "image_url": "url1"
  }
}
```

> **Tip:**  
> To change the event name or the fields in `evtData`, simply update the relevant logic in your job files (e.g., `cronJob2.js`, `cronJob4.js`) and the payload construction before sending to CleverTap.

## Manual Testing

In `src/index.js`:

```javascript
async function startApp() {
  // Cart abandonment
  await runJob1Manually();
  await runJob2Manually();

  // Product views
  await runJob3Manually();
  await runJob4Manually();
}
```

## Monitoring

```bash
# View all logs
tail -f logs/app.log

# View errors only
grep "error" logs/app.log

# View successful uploads
grep "success" logs/app.log
```

## Troubleshooting

### Common Issues

1. **AWS Connection Failed**

   - Verify AWS credentials
   - Check bucket permissions
   - Confirm region settings

2. **CleverTap Upload Failed**

   - Validate API credentials
   - Check event format
   - Verify rate limits

3. **Missing Data**
   - Check S3 file naming
   - Verify CSV structure
   - Confirm bucket permissions

## License

MIT License - See LICENSE file for details

⚠️ **Important**: Never commit `.env` with real credentials!
