const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const config = {
    aws: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.AWS_REGION
    },
    s3: {
        cartAbandonBucket: process.env.S3_CART_ABANDON_BUCKET,
        chargedEventsBucket: process.env.S3_CHARGED_EVENTS_BUCKET,
        deltaEventsBucket: process.env.S3_DELTA_EVENTS_BUCKET
    }
};

module.exports = config;