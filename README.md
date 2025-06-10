# S3 Delta CleverTap Integration

## Overview

This application automates the process of tracking cart abandonment by processing S3 bucket data and sending consolidated user activity to CleverTap. It runs two scheduled jobs daily to identify abandoned carts and update user profiles.

## Features

- 🕒 Configurable schedule for data processing
- 📊 Multi-day historical data support
- 👤 User-based data consolidation
- 🛒 Tracks up to 5 most recent cart items per user
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

### 2. Configuration

Create `.env` file in root directory:

```plaintext
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region

# S3 Buckets
S3_CART_BUCKET=your-cart-data-bucket
S3_CHARGED_BUCKET=your-charged-data-bucket
S3_DELTA_EVENTS_BUCKET=your-delta-events-bucket

# CleverTap Configuration
CLEVERTAP_ACCOUNT_ID=your_account_id
CLEVERTAP_PASSCODE=your_passcode
CLEVERTAP_API_ENDPOINT=https://api.clevertap.com/1

# Application Settings
PORT=3000
```

### 3. Start Application

```bash
node src/index.js
```

## Configuration Options

### Schedule Settings

In `src/index.js`:

```javascript
const cronConfig = {
  job1: {
    hour: 16, // Job 1 hour (24-hour format)
    minute: 15, // Job 1 minute
  },
  delayBetweenJobs: 3, // Minutes between jobs
};
```

### Data Processing Settings

In `src/jobs/cronJob2.js`:

```javascript
constructor() {
    this.MAX_ITEMS_PER_PROFILE = 5;  // Items per user
    this.REVERSE_ORDER = true;        // true = newest first
}
```

### Historical Data Settings

In `src/jobs/cronJob1.js`:

```javascript
constructor() {
    this.days = 1;  // Number of days to process
}
```

## Data Flow

1. **Job 1 (Data Processing)**

   - Fetches cart and charged events from S3
   - Identifies abandoned carts
   - Generates delta events
   - Stores results in delta bucket

2. **Job 2 (CleverTap Upload)**
   - Reads delta events
   - Consolidates by user
   - Formats data for CleverTap
   - Sends API requests

## Output Format

Data sent to CleverTap:

```json
{
  "identity": "user123",
  "evtData": {
    "product_id_0": "item5",
    "price_0": "99.99",
    "image_url_0": "url5",
    "product_id_1": "item4",
    "price_1": "149.99",
    "image_url_1": "url4"
  }
}
```

## Monitoring

View application logs:

```bash
tail -f logs/app.log
```

## Troubleshooting

### Common Issues

1. **AWS Connection Failed**

   - Verify AWS credentials in `.env`
   - Check S3 bucket permissions
   - Confirm AWS region setting

2. **CleverTap Upload Failed**

   - Validate API credentials
   - Check API endpoint URL
   - Verify data format

3. **No Data Processed**
   - Confirm S3 file naming format
   - Check file timestamps
   - Verify CSV structure

## Development

### Manual Testing

In `src/index.js`:

```javascript
async function startApp() {
  // Uncomment to test:
  // await runJob1Manually();
  // await runJob2Manually();
}
```

### Adding New Features

1. Create feature branch
2. Implement changes
3. Add logs
4. Test manually
5. Submit PR

## License

MIT License - See LICENSE file for details

## Support

- Create GitHub issue for bugs
- Submit PR for improvements
- Check logs for troubleshooting

## Environment Setup

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Update `.env` with your credentials:

```plaintext
AWS_ACCESS_KEY_ID=your_actual_access_key
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
...
```

⚠️ **Important**: Never commit the `.env` file with real credentials!
