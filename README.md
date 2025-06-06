# s3-delta-clevertap-app

## Overview
This project is designed to fetch data from an AWS S3 bucket, process it to check for matching user IDs and product IDs between AddToCart and Charged data, and update a Delta table accordingly. Additionally, it sends the Delta data to the CleverTap API at the specified endpoint.

## Project Structure
```
s3-delta-clevertap-app
├── src
│   ├── index.js                  # Entry point of the application
│   ├── config
│   │   └── aws.js               # AWS configuration settings
│   ├── cron
│   │   ├── processDataJob.js     # Cron job for processing data at 2 AM
│   │   └── sendDeltaToCleverTapJob.js # Cron job for sending Delta data to CleverTap
│   ├── services
│   │   ├── s3Service.js          # Functions for interacting with AWS S3
│   │   ├── deltaService.js       # Functions for managing the Delta table
│   │   └── cleverTapService.js   # Functions for sending data to CleverTap API
│   ├── utils
│   │   └── logger.js             # Logging utilities
│   ├── models
│   │   ├── addToCart.js          # Structure and methods for AddToCart data
│   │   ├── charged.js            # Structure and methods for Charged data
│   │   └── delta.js              # Structure and methods for the Delta table
│   └── routes
│       └── api.js                # API routes for the application
├── package.json                   # npm configuration file
├── .env                           # Environment variables
└── README.md                      # Project documentation
```

## Setup Instructions
1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd s3-delta-clevertap-app
   ```

2. **Install dependencies:**
   ```
   npm install
   ```

3. **Configure environment variables:**
   Create a `.env` file in the root directory and add your AWS credentials and CleverTap API keys:
   ```
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_REGION=your_region
   CLEVERTAP_ACCOUNT_ID=your_account_id
   CLEVERTAP_PASSCODE=your_passcode
   ```

4. **Run the application:**
   ```
   npm start
   ```

## Usage
- The application will automatically fetch data from the specified AWS S3 bucket and process it at 2 AM daily.
- It checks for matching user IDs and product IDs between AddToCart and Charged data, updating the Delta table accordingly.
- The Delta data is sent to the CleverTap API at the `/send-uncharged-events` endpoint.

## Contributing
Feel free to submit issues or pull requests for any improvements or bug fixes.

## License
This project is licensed under the MIT License.