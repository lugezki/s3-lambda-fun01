const { processEvents, isS3PutEvent } = require("./src/batch-process.js");

export const handler = async (event) => {
    console.log("Running Lambda function...");
    console.log("Full Event Object: ", JSON.stringify(event, null, 2));

    if (isS3PutEvent(event)) {
        await processEvents(event);
    }
};
