const csv = require('csv-parser');
const { Parser } = require("json2csv");
const { S3Client, GetObjectCommand, PutObjectCommand, CopyObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const s3Client = new S3Client({ region: "us-west-1" });

async function processEvents(event) {
  const bucket = event.Records[0].s3.bucket.name;
  const fileName = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));

  console.log(`Processing file: ${fileName} from bucket: ${bucket}`);

  try {
    const rows = await processCSV(bucket, fileName);
    console.log("File content (rows):", rows.length);

    const result = processRows(rows);
    console.log(`Processing result: Success count: ${result.successCount}, Failure count: ${result.failureCount}`);

    await copyFileToProcessed(bucket, fileName);
    await deleteFile(bucket, fileName);

    if (result.errors && result.errors.length > 0) {
      await writeErrorFileToS3(bucket, fileName, result.errors);
    }

  } catch (error) {
    console.error("Error processing file:", error);
    throw error;
  }
}

function isS3PutEvent(event) {
  if (
    event.Records &&
    event.Records.length > 0 &&
    event.Records[0].eventSource === "aws:s3" &&
    event.Records[0].eventName === "ObjectCreated:Put"
    && event.Records[0].s3.object.key.includes('unprocessed')
  ) {
    return true;
  }
  return false;
}

async function processCSV(bucket, fileName) {
  const command = new GetObjectCommand({ Bucket: bucket, Key: fileName });
  const response = await s3Client.send(command);
  if (!response.Body) {
    throw new Error(`No body found in response for file: ${fileName}`);
  }

  return new Promise((resolve, reject) => {
    const results = [];
    response.Body.pipe(csv())
      .on('data', (data) => results.push(data))
      .on('error', reject)
      .on('end', () => resolve(results));
  });
}

function processRows(rows) {
  console.log("Processing rows...");
  let successCount = 0;
  let failureCount = 0;

  const errors = [];
  for (let i = 0; i < rows.length; i++) {
    try {
      const row = rows[i];
      // Simulate processing logic
      successCount++;
    } catch (error) {
      failureCount++;
      errors.push(rows[i]);
      console.error(`Error processing row ${i + 1}:`, error);
    }
  }

  return {
    successCount,
    failureCount,
    errors,
  };
}

async function copyFileToProcessed(bucket, fileName) {
  const sourceKey = fileName.startsWith('unprocessed/') ? fileName : `unprocessed/${fileName}`;
  const destinationKey = sourceKey.replace('unprocessed/', 'processed/').replace(/\.csv$/, '_processed.csv');

  try {
    console.log(`Copying file from ${sourceKey} to ${destinationKey} in bucket ${bucket}...`);

    const command = new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${sourceKey}`,
      Key: destinationKey,
    });
    await s3Client.send(command);

    console.log(`File successfully copied to ${destinationKey}`);
  } catch (error) {
    console.error(`Error copying file from ${sourceKey} to ${destinationKey}:`, error);
    throw error;
  }
}

async function deleteFile(bucket, fileName) {
  const key = fileName.startsWith('unprocessed/') ? fileName : `unprocessed/${fileName}`;

  try {
    console.log(`Deleting file: ${key} from bucket: ${bucket}...`);

    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    await s3Client.send(command);

    console.log(`File successfully deleted: ${key}`);
  } catch (error) {
    console.error(`Error deleting file: ${key} from bucket: ${bucket}`, error);
    throw error;
  }
}

async function writeErrorFileToS3(bucket, fileName, errorData) {
  const sourceKey = fileName.startsWith('unprocessed/') ? fileName : `unprocessed/${fileName}`;
  const destinationKey = sourceKey.replace('unprocessed/', 'errors/').replace(/\.csv$/, '_errors.csv');

  try {
    console.log(`Writing error file to ${destinationKey} in bucket ${bucket}...`);
    const json2csvParser = new Parser();
    const csvData = json2csvParser.parse(errorData);

    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: destinationKey,
      Body: csvData,
      ContentType: 'text/csv',
    });
    await s3Client.send(command);

    console.log(`Error file successfully written to ${destinationKey}`);
  } catch (error) {
    console.error(`Error writing error file to ${destinationKey} in bucket ${bucket}:`, error);
    throw error;
  }
}

module.exports = { processEvents, isS3PutEvent };