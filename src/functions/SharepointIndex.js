const { TokenCredentialAuthenticationProvider } = require("@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials");
const { Client } = require('@microsoft/microsoft-graph-client');
const { ClientSecretCredential } = require('@azure/identity');
const { BlobServiceClient } = require("@azure/storage-blob");
const axios = require('axios');
const { app } = require('@azure/functions');
const { TextDecoder } = require('util');
const { SearchClient, AzureKeyCredential } = require("@azure/search-documents");
const { OpenAIEmbeddings } = require("@microsoft/teams-ai");
const mammoth = require('mammoth');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const pdfParse = require('pdf-parse');
const unzipper = require('unzipper');
const xml2js = require('xml2js');
const { Readable } = require('stream');
const path = require('path');
require('isomorphic-fetch');
require('dotenv').config();

// Application Insights Setup
const appInsights = require('applicationinsights');
appInsights.setup().start();
const appInsightsClient = appInsights.defaultClient;

app.storageQueue('ProcessQueueItem', {
    queueName: 'file-processing-queue',
    connection: 'AzureWebJobsStorage',
    handler: async (queueItem, context) => {
        try {
            context.log('Queue trigger function processed work item', queueItem);

            let fileUrl;
            if (typeof queueItem === 'string') {
                fileUrl = queueItem;
            } else if (queueItem && queueItem.fileUrl) {
                fileUrl = queueItem.fileUrl;
            } else {
                throw new Error('Invalid queue item: fileUrl not found');
            }

            logMessage(context, "Received fileUrl from queue:", { fileUrl });

            const result = await processSharePointFile(context, fileUrl);
            context.log(result);
        } catch (error) {
            context.log('Error message', errorObject);
            throw error; // Rethrow to trigger Azure Functions retry policy
        }
    }
});


app.http('SharepointIndex', {
    methods: ['GET', 'POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        try {
            context.log("Received request body (raw):", request.body);

            let requestBody;

            if (typeof request.body === 'string') {
                // Directly parse if the body is a string
                requestBody = JSON.parse(request.body);
            } else if (request.body && typeof request.body.getReader === 'function') {
                // If it's a ReadableStream, convert it to a string first
                const reader = request.body.getReader();
                const decoder = new TextDecoder();
                let result = '';
                let done, value;

                while ({ done, value } = await reader.read(), !done) {
                    result += decoder.decode(value, { stream: true });
                }

                requestBody = JSON.parse(result);
            } else {
                // Assume the body is already parsed
                requestBody = request.body;
            }

            context.log("Parsed request body:", requestBody);

            logMessage(context, "Starting to process the request");
            logMessage(context, "Received request body:", { body: requestBody });

            const fileUrl = request.query.fileUrl || requestBody.fileUrl || null;
            logMessage(context, "Received fileUrl:", { fileUrl });

            const effectiveFileUrl = fileUrl || process.env.DEFAULT_SHAREPOINT_FILE_PATH;
            logMessage(context, "Effective fileUrl being used:", { effectiveFileUrl });

            if (!effectiveFileUrl) {
                throw new Error("No file URL provided and no default URL set");
            }

            const result = await processSharePointFile(context, effectiveFileUrl);
            return { body: result };
        } catch (error) {
            context.log('Error processing request:', error.message);
            return { status: 500, body: `Internal Server Error: ${error.message}` };
        }
    }
});

function logMessage(context, message, obj = null) {
    if (obj) {
        context.log(message, obj);
        appInsightsClient.trackTrace({ message: message, properties: obj });
    } else {
        context.log(message);
        appInsightsClient.trackTrace({ message: message });
    }
}

async function generateEmbedding(context, text) {
    try {
        const embeddings = new OpenAIEmbeddings({
            azureApiKey: process.env.SECRET_AZURE_OPENAI_API_KEY,
            azureEndpoint: process.env.AZURE_OPENAI_ENDPOINT,
            azureDeployment: process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME,
        });

        logMessage(context, "Generating embeddings for text:", text);

        const result = await embeddings.createEmbeddings(
            process.env.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME,
            text
        );

        if (result.status !== "success" || !result.output) {
            throw new Error(`Failed to generate embeddings for text: ${text}`);
        }

        logMessage(context, "Embeddings generated successfully.");
        return result.output[0];
    } catch (error) {
        logMessage(context, `Error generating embedding: ${error.message} for text: ${text}`);
        throw error;
    }
}

function checkRequiredEnvVars(context) {
    const requiredVars = [
        'AZURE_TENANT_ID',
        'AZURE_CLIENT_ID',
        'AZURE_CLIENT_SECRET',
        'AZURE_STORAGE_CONNECTION_STRING',
        'BLOB_CONTAINER_NAME',
        'DEFAULT_SHAREPOINT_FILE_PATH',
        'AZURE_SEARCH_ENDPOINT',
        'SECRET_AZURE_SEARCH_KEY',
        'AZURE_SEARCH_INDEX_NAME',
        'SECRET_AZURE_OPENAI_API_KEY',
        'AZURE_OPENAI_ENDPOINT',
        'AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME'
    ];

    const missingVars = requiredVars.filter(varName => !process.env[varName]);

    if (missingVars.length > 0) {
        const errorMsg = `Missing required environment variables: ${missingVars.join(', ')}`;
        logMessage(context, errorMsg);
        throw new Error(errorMsg);
    }
}

async function getSiteInfo(context, graphClient, tenantName, sitePath) {
    const siteUrl = `/sites/${tenantName}.sharepoint.com:/sites/${sitePath}`;
    logMessage(context, "Fetching site information", { siteUrl });
    const site = await graphClient.api(siteUrl).get();
    logMessage(context, "Site information fetched", {
        siteId: site.id,
        siteName: site.displayName
    });
    return site;
}

async function getDriveInfo(context, graphClient, siteId) {
    const drivesUrl = `/sites/${siteId}/drives`;
    logMessage(context, "Fetching drives", { drivesUrl });
    const drives = await graphClient.api(drivesUrl).get();
    const documentLibrary = drives.value.find(drive => drive.name === 'Documents');
    if (!documentLibrary) {
        throw new Error("Could not find the Documents drive");
    }
    logMessage(context, "Documents drive found", { driveId: documentLibrary.id });
    return documentLibrary;
}

async function getFileMetadata(context, graphClient, siteId, driveId, filePath) {
    const encodedFilePath = encodeURIComponent(filePath).replace(/%2F/g, '/');
    const fileUrl = `/sites/${siteId}/drives/${driveId}/root:/${encodedFilePath}`;

    logMessage(context, "Fetching file metadata", { fileUrl });

    try {
        const file = await graphClient.api(fileUrl).get();
        logMessage(context, "File metadata fetched", {
            fileName: file.name,
            fileSize: file.size,
            fileId: file.id,
            mimeType: file.file ? file.file.mimeType : 'Unknown'
        });
        return file;
    } catch (error) {
        logMessage(context, "Error fetching file metadata", {
            error: error.message,
            fileUrl: fileUrl
        });
        appInsightsClient.trackException({ exception: error });
        throw error;
    }
}

async function uploadToBlobStorage(context, fileContent, fileName, contentType) {
    if (!process.env.AZURE_STORAGE_CONNECTION_STRING) {
        throw new Error("AZURE_STORAGE_CONNECTION_STRING is not set in environment variables");
    }

    if (!process.env.BLOB_CONTAINER_NAME) {
        throw new Error("BLOB_CONTAINER_NAME is not set in environment variables");
    }

    const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient(process.env.BLOB_CONTAINER_NAME);
    const blockBlobClient = containerClient.getBlockBlobClient(fileName);

    logMessage(context, `Uploading file to Blob Storage: ${fileName}`);
    await blockBlobClient.upload(fileContent, fileContent.length, {
        blobHTTPHeaders: { blobContentType: contentType }
    });
    logMessage(context, `File uploaded successfully to Blob Storage with content type: ${contentType}`);
}

function chunkContent(context, content, maxChunkSize = 2000) {
    const chunks = [];
    let currentChunk = "";

    const sentences = content.match(/[^.!?]+[.!?]+|\s+/g) || [];

    for (const sentence of sentences) {
        if ((currentChunk + sentence).length > maxChunkSize && currentChunk.length > 0) {
            chunks.push(currentChunk.trim());
            currentChunk = "";
        }
        currentChunk += sentence;
    }

    if (currentChunk.trim().length > 0) {
        chunks.push(currentChunk.trim());
    }

    chunks.forEach((chunk, index) => {
        logMessage(context, `Chunk ${index + 1} size: ${Buffer.byteLength(chunk, 'utf8')} bytes`);
    });

    return chunks;
}

async function deleteExistingDocuments(context, searchClient, fileUrl) {
    logMessage(context, `Deleting existing documents for fileUrl: ${fileUrl}`);
    const results = await searchClient.search('', { 
        filter: `fileUrl eq '${fileUrl}'`,
        select: ['docId']
    });
    
    const documentsToDelete = [];
    for await (const result of results.results) {
        documentsToDelete.push({ docId: result.document.docId });
    }

    if (documentsToDelete.length > 0) {
        await searchClient.deleteDocuments(documentsToDelete);
        logMessage(context, `Deleted ${documentsToDelete.length} existing documents`);
    } else {
        logMessage(context, "No existing documents found to delete");
    }
}

async function processSharePointFile(context, fileUrl) {
    logMessage(context, "ProcessSharePointFile function started");

    try {
        checkRequiredEnvVars(context);

        fileUrl = fileUrl || process.env.DEFAULT_SHAREPOINT_FILE_PATH;

        logMessage(context, "File URL", { fileUrl });

        if (!fileUrl) {
            throw new Error("No file URL provided and no default URL set");
        }

        const credential = new ClientSecretCredential(
            process.env.AZURE_TENANT_ID,
            process.env.AZURE_CLIENT_ID,
            process.env.AZURE_CLIENT_SECRET
        );
        const authProvider = new TokenCredentialAuthenticationProvider(credential, {
            scopes: ['https://graph.microsoft.com/.default']
        });
        const graphClient = Client.initWithMiddleware({ authProvider });

        const url = new URL(fileUrl);
        const tenantName = url.hostname.split('.')[0];
        const sitePath = url.pathname.split('/sites/')[1].split('/')[0];
        const filePath = decodeURIComponent(url.pathname.split('/Shared%20Documents/')[1].split('?')[0]);

        logMessage(context, "URL Parsing results", { tenantName, sitePath, filePath });

        const site = await getSiteInfo(context, graphClient, tenantName, sitePath);
        const drive = await getDriveInfo(context, graphClient, site.id);
        const file = await getFileMetadata(context, graphClient, site.id, drive.id, filePath);

        logMessage(context, 'Downloading file content');
        const response = await axios.get(file['@microsoft.graph.downloadUrl'], { responseType: 'arraybuffer' });
        logMessage(context, `File content downloaded. Size: ${response.data.length} bytes`);

        let textContent = await extractTextContent(context, file, response.data);

        const chunks = chunkContent(context, textContent);
        const contentType = file.file ? file.file.mimeType : 'application/octet-stream';

        logMessage(context, `File chunked into ${chunks.length} parts`);

        const searchClient = new SearchClient(
            process.env.AZURE_SEARCH_ENDPOINT,
            process.env.AZURE_SEARCH_INDEX_NAME,
            new AzureKeyCredential(process.env.SECRET_AZURE_SEARCH_KEY)
        );

        // Delete existing documents
        await deleteExistingDocuments(context, searchClient, fileUrl);

        const indexDocuments = [];

        for (let i = 0; i < chunks.length; i++) {
            const chunkContent = chunks[i];
        
            const embedding = await generateEmbedding(context, chunkContent);
            if (!embedding) {
                throw new Error(`Failed to generate embedding for chunk ${i + 1}`);
            }
            logMessage(context, `Generated embedding for chunk ${i + 1}/${chunks.length}`);
        
            const document = {
                docId: `${file.id}-${i + 1}`,
                docTitle: file.name,
                description: chunkContent,
                filename: file.name,
                filetype: path.extname(file.name),
                lastmodified: file.lastModifiedDateTime,
                chunkindex: (i + 1).toString(),  // Changed to string
                totalChuncks: chunks.length.toString(),  // Changed to string
                descriptionVector: embedding,
                fileUrl: fileUrl
            };
        
            indexDocuments.push(document);
        
            logMessage(context, `Prepared metadata for chunk ${i + 1}`, { ...document, descriptionVector: 'Embedding data (not shown due to size)' });
        }
        
        await searchClient.uploadDocuments(indexDocuments);

        logMessage(context, "File processing and indexing completed", {
            fileName: file.name,
            fileType: contentType,
            lastModified: file.lastModifiedDateTime,
            totalChunks: chunks.length,
            totalSize: response.data.length
        });

        return `Successfully processed and indexed file: ${file.name}. Chunked into ${chunks.length} parts and indexed. Total Size: ${response.data.length} bytes. File Type: ${contentType}`;
    } catch (error) {
        logMessage(context, "Error processing and indexing file", {
            error: error.message,
            stack: error.stack
        });
        appInsightsClient.trackException({ exception: error });
        throw error;
    }
}

async function extractTextContent(context, file, fileContent) {
    const fileExtension = path.extname(file.name).toLowerCase();
    let textContent = '';

    switch (fileExtension) {
        case '.docx':
            const result = await mammoth.extractRawText({ buffer: fileContent });
            textContent = result.value;
            break;
        case '.xlsx':
            const workbook = xlsx.read(fileContent, {type:'buffer'});
            textContent = workbook.SheetNames.map(sheetName => {
                const sheet = workbook.Sheets[sheetName];
                return xlsx.utils.sheet_to_csv(sheet);
            }).join('\n');
            break;
        case '.csv':
            textContent = await new Promise((resolve) => {
                let result = '';
                Readable.from(fileContent)
                    .pipe(csv())
                    .on('data', (row) => { result += Object.values(row).join(' ') + '\n'; })
                    .on('end', () => { resolve(result); });
            });
            break;
        case '.txt':
            textContent = fileContent.toString('utf8');
            break;
        case '.pdf':
            const pdfData = await pdfParse(fileContent);
            textContent = pdfData.text;
            break;            
        case '.pptx':
            textContent = await extractTextFromPowerPoint(fileContent);
            break;
        default:
            throw new Error(`Unsupported file format: ${fileExtension}`);
    }

    logMessage(context, `Extracted text content from ${fileExtension} file`);
    logMessage(context, "Extracted Text Content (First 500 characters):", textContent.slice(0, 500));
    return textContent;
}

async function extractTextFromPowerPoint(buffer) {
    try {
        const zip = await unzipper.Open.buffer(buffer);
        let text = '';
        let slideCounter = 1;

        for (const file of zip.files) {
            if (file.path.startsWith('ppt/slides/slide')) {
                const content = await file.buffer();
                const parser = new xml2js.Parser();
                const result = await parser.parseStringPromise(content);

                if (result && result['p:sld'] && result['p:sld']['p:cSld']) {
                    const slideContent = extractTextFromSlide(result['p:sld']['p:cSld'][0]);
                    if (slideContent.trim()) {
                        text += `Slide ${slideCounter}:\n${slideContent}\n\n`;
                        slideCounter++;
                    }
                }
            }
        }

        return text.trim();
    } catch (error) {
        console.error('Error extracting text from PowerPoint:', error);
        throw error;
    }
}

function extractTextFromSlide(slide) {
    let text = '';
    if (slide && slide['p:spTree'] && slide['p:spTree'][0] && slide['p:spTree'][0]['p:sp']) {
        for (const shape of slide['p:spTree'][0]['p:sp']) {
            if (shape['p:txBody'] && shape['p:txBody'][0] && shape['p:txBody'][0]['a:p']) {
                for (const paragraph of shape['p:txBody'][0]['a:p']) {
                    if (paragraph['a:r'] && paragraph['a:r'][0] && paragraph['a:r'][0]['a:t']) {
                        text += paragraph['a:r'][0]['a:t'][0] + ' ';
                    }
                }
            }
        }
    }
    return text.trim();
}


async function main() {
    try {
        const result = await processSharePointFile(null, process.env.DEFAULT_SHAREPOINT_FILE_PATH);
        console.log(result);
    } catch (error) {
        console.error("Error in main function:", error.message);
        if (error.message.includes("environment variables")) {
            console.error("Please ensure all required environment variables are set in your .env.local file");
        }
    }
}

if (require.main === module) {
    main();
}

module.exports = app;
