import AWS from 'aws-sdk';
const { S3 } = AWS;
const s3 = new S3();

import { processEvents, isS3PutEvent } from './batch-process.js';

export const handler = async (event) => {
    console.log("Running Lambda function...");
    console.log("Full Event Object: ", JSON.stringify(event, null, 2));

    if (isS3PutEvent(event)) {
        await processEvents(event);
    }
};
