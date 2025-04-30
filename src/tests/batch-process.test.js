const fs = require('fs').promises;
const path = require('path');
const { Readable } = require('stream');
const { mockClient } = require("aws-sdk-client-mock");
const { S3Client, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { processEvents, isS3PutEvent } = require("../batch-process.js");

const s3Mock = mockClient(S3Client);

describe("Batch Process", () => {

    beforeEach(() => {
        s3Mock.reset();
    });
    it('should throw an error when the CSV file is empty or missing', async () => {

        s3Mock.on(GetObjectCommand).resolves({
            Body: null
        });

        const event = require("./data/s3-put-event.json");
        await expect(processEvents(event)).rejects.toThrow("No body found in response for file:");

        expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(1);
        expect(s3Mock.commandCalls(CopyObjectCommand)).toHaveLength(0);
        expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(0);
        expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
    });

    it('should process all events successfully', async () => {


        const csvContent = await readCsvFile("../../data/ferrari_models.csv");
        s3Mock.on(GetObjectCommand).resolves({
            Body: createMockReadableStream(csvContent)
        });
        
        const event = require("./data/s3-put-event.json");
        await processEvents(event);

        expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(1);
        expect(s3Mock.commandCalls(CopyObjectCommand)).toHaveLength(1);
        expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(1);
        expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
    });
});


async function readCsvFile(filePath) {
    const absolutePath = path.resolve(__dirname, filePath);
    return await fs.readFile(absolutePath, "utf-8");
}

function createMockReadableStream(content) {
    const readable = new Readable();
    readable.push(content);
    readable.push(null);
    return readable;
}

describe("isS3PutEvent", () => {
    it("should return true for a valid S3 Put event", () => {
        const event = {
            Records: [
                {
                    eventSource: "aws:s3",
                    eventName: "ObjectCreated:Put",
                    s3: {
                        object: {
                            key: "unprocessed/testfile.csv"
                        }
                    }
                }
            ]
        };

        const result = isS3PutEvent(event);
        expect(result).toBe(true);
    });

    it("should return false if eventSource is not 'aws:s3'", () => {
        const event = {
            Records: [
                {
                    eventSource: "aws:sns",
                    eventName: "ObjectCreated:Put",
                    s3: {
                        object: {
                            key: "unprocessed/testfile.csv"
                        }
                    }
                }
            ]
        };

        const result = isS3PutEvent(event);
        expect(result).toBe(false);
    });

    it("should return false if eventName is not 'ObjectCreated:Put'", () => {
        const event = {
            Records: [
                {
                    eventSource: "aws:s3",
                    eventName: "ObjectRemoved:Delete",
                    s3: {
                        object: {
                            key: "unprocessed/testfile.csv"
                        }
                    }
                }
            ]
        };

        const result = isS3PutEvent(event);
        expect(result).toBe(false);
    });

    it("should return false if the key does not include 'unprocessed'", () => {
        const event = {
            Records: [
                {
                    eventSource: "aws:s3",
                    eventName: "ObjectCreated:Put",
                    s3: {
                        object: {
                            key: "processed/testfile.csv"
                        }
                    }
                }
            ]
        };

        const result = isS3PutEvent(event);
        expect(result).toBe(false);
    });

    it("should return false if Records array is empty", () => {
        const event = {
            Records: []
        };

        const result = isS3PutEvent(event);
        expect(result).toBe(false);
    });

    it("should return false if Records is undefined", () => {
        const event = {};

        const result = isS3PutEvent(event);
        expect(result).toBe(false);
    });
});