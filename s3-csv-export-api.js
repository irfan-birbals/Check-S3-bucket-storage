// Suppress Node.js deprecation warnings for AWS SDK (Node.js v18.20.8 compatibility)
const originalEmitWarning = process.emitWarning;
process.emitWarning = function(warning, type, code, ctor) {
    if (code === 'NodeDeprecationWarning' && 
        typeof warning === 'string' && 
        (warning.includes('AWS SDK') || warning.includes('aws-sdk'))) {
        return;
    }
    return originalEmitWarning.apply(process, arguments);
};

process.on('warning', (warning) => {
    if (warning.name === 'NodeDeprecationWarning' && 
        warning.message && 
        (warning.message.includes('AWS SDK') || warning.message.includes('aws-sdk'))) {
        return;
    }
    console.warn(warning.name, warning.message);
});

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { Pool } = require('pg');

// Configuration
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const DB_HOST = process.env.DB_HOST;
const DB_PORT = process.env.DB_PORT || 5432;
const DB_NAME = process.env.DB_NAME;
const DB_USER = process.env.DB_USER;
const DB_PASSWORD = process.env.DB_PASSWORD;

// Parse command line arguments
const args = process.argv.slice(2);
let prefix = null;
let excludeDocumentSnapshots = false;
let outputFile = null;

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--prefix' && args[i + 1]) {
        prefix = args[i + 1];
        i++;
    } else if (args[i] === '--exclude-document-snapshots') {
        excludeDocumentSnapshots = true;
    } else if (args[i] === '--output' && args[i + 1]) {
        outputFile = args[i + 1];
        i++;
    } else if (args[i] === '--help' || args[i] === '-h') {
        console.log(`
Usage: node s3-csv-export-standalone.js [options]

Options:
  --prefix <prefix>              S3 prefix to filter (e.g., "CarImages/")
  --exclude-document-snapshots   Exclude document snapshot files
  --output <filename>            Output CSV filename (default: s3-media-export-<timestamp>.csv)
  --help, -h                     Show this help message

Examples:
  node s3-csv-export-standalone.js
  node s3-csv-export-standalone.js --prefix "CarImages/"
  node s3-csv-export-standalone.js --prefix "CarImages/" --exclude-document-snapshots
  node s3-csv-export-standalone.js --output my-export.csv
        `);
        process.exit(0);
    }
}

// Validate required environment variables
if (!S3_BUCKET_NAME) {
    console.error('❌ Error: S3_BUCKET_NAME environment variable is required');
    process.exit(1);
}

if (!DB_HOST || !DB_NAME || !DB_USER || !DB_PASSWORD) {
    console.error('❌ Error: Database configuration is required (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)');
    process.exit(1);
}

// Initialize S3 client
const s3Client = new S3Client({
    region: AWS_REGION
});

// Initialize PostgreSQL connection pool
const dbPool = new Pool({
    host: DB_HOST,
    port: DB_PORT,
    database: DB_NAME,
    user: DB_USER,
    password: DB_PASSWORD,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

// File type definitions
const IMAGE_EXTENSIONS = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'tiff', 'tif'];
const VIDEO_EXTENSIONS = ['mp4', 'avi', 'mov', 'wmv', 'flv', 'mkv', 'webm', 'm4v'];

/**
 * Get file extension from key
 */
function getFileExtension(key) {
    const keyWithoutQuery = key.split('?')[0];
    const filename = keyWithoutQuery.split('/').pop();
    const parts = filename.split('.');
    if (parts.length < 2) return '';
    return parts[parts.length - 1].toLowerCase();
}

/**
 * Get base name (filename without extension)
 */
function getBaseName(filename) {
    const lastDot = filename.lastIndexOf('.');
    return lastDot > 0 ? filename.substring(0, lastDot) : filename;
}

/**
 * Classify file type by folder path first, then by extension
 */
function classifyFileType(key, extension) {
    // Documents folder takes priority
    if (key.startsWith('Documents/') || key.includes('/Documents/')) {
        return 'documents';
    }
    
    const ext = extension.trim();
    if (IMAGE_EXTENSIONS.includes(ext)) return 'images';
    if (VIDEO_EXTENSIONS.includes(ext)) return 'videos';
    return 'other';
}

/**
 * Determine path type (original, thumbnail, reduced, snapshot)
 */
function getPathType(s3Key) {
    if (s3Key.includes('/thumbnail/')) {
        return 'thumbnail';
    } else if (s3Key.includes('/reduced/')) {
        return 'reduced';
    } else if (s3Key.includes('/snapshot_')) {
        return 'snapshot';
    }
    return 'original';
}

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes) {
    if (bytes < 1024) {
        return bytes + ' B';
    } else if (bytes < 1024 * 1024) {
        return (bytes / 1024.0).toFixed(2) + ' KB';
    } else if (bytes < 1024 * 1024 * 1024) {
        return (bytes / (1024.0 * 1024.0)).toFixed(2) + ' MB';
    } else {
        return (bytes / (1024.0 * 1024.0 * 1024.0)).toFixed(2) + ' GB';
    }
}

/**
 * Escape CSV field (handle commas, quotes, newlines)
 */
function escapeCsvField(field) {
    if (field == null || field === undefined) {
        return '';
    }
    const str = String(field);
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

/**
 * Query database to get all valid media filenames
 * Returns Set of base filenames (without extension)
 */
async function getValidMediaFilenamesFromDatabase() {
    const validBaseNames = new Set();
    
    try {
        console.log('Querying database for media URLs...');
        const startTime = Date.now();
        
        // Query medias table
        const mediaQuery = `
            SELECT DISTINCT url 
            FROM medias 
            WHERE url IS NOT NULL AND url != ''
        `;
        
        const mediaResult = await dbPool.query(mediaQuery);
        console.log(`Fetched ${mediaResult.rows.length} media records in ${Date.now() - startTime} ms`);
        
        // Extract base filenames from URLs
        for (const row of mediaResult.rows) {
            const url = row.url;
            if (!url) continue;
            
            // Remove query parameters if any
            const urlWithoutQuery = url.split('?')[0];
            
            // Extract filename (last segment after /)
            const filename = urlWithoutQuery.split('/').pop();
            
            // Get base name (without extension)
            const baseName = getBaseName(filename);
            if (baseName) {
                validBaseNames.add(baseName);
            }
        }
        
        console.log(`Extracted ${validBaseNames.size} valid media filenames from medias table`);
        
        // Query users table for profile pictures
        const userQuery = `
            SELECT DISTINCT picture 
            FROM users 
            WHERE picture IS NOT NULL AND picture != ''
        `;
        
        const userResult = await dbPool.query(userQuery);
        console.log(`Fetched ${userResult.rows.length} user records in ${Date.now() - startTime} ms`);
        
        // Extract base filenames from profile picture URLs
        for (const row of userResult.rows) {
            const picture = row.picture;
            if (!picture) continue;
            
            const urlWithoutQuery = picture.split('?')[0];
            const filename = urlWithoutQuery.split('/').pop();
            const baseName = getBaseName(filename);
            if (baseName) {
                validBaseNames.add(baseName);
            }
        }
        
        console.log(`Found ${validBaseNames.size} unique media files in database (total time: ${Date.now() - startTime} ms)`);
        
        return validBaseNames;
    } catch (error) {
        console.error('Error querying database for media filenames:', error);
        throw new Error('Failed to query database: ' + error.message);
    }
}

/**
 * Check if a file should be included in the CSV export
 * Returns true if:
 * 1. The base filename matches a database record (for original files)
 * 2. OR it's a thumbnail/reduced/snapshot of a file that exists in DB
 */
function shouldIncludeFile(s3Key, baseName, pathType, validBaseNames) {
    // For original files, check if base name exists in database
    if (pathType === 'original') {
        return validBaseNames.has(baseName);
    }
    
    // For derived files (thumbnail, reduced, snapshot), check if their base name exists in DB
    // This allows including thumbnails/reduced versions of files that exist in DB
    return validBaseNames.has(baseName);
}

/**
 * List all objects in S3 bucket with optional prefix
 */
async function listAllObjects(prefix) {
    const allObjects = [];
    let continuationToken = undefined;
    
    do {
        try {
            const command = new ListObjectsV2Command({
                Bucket: S3_BUCKET_NAME,
                Prefix: prefix || '',
                ContinuationToken: continuationToken,
                MaxKeys: 1000
            });
            
            const response = await s3Client.send(command);
            
            if (response.Contents) {
                allObjects.push(...response.Contents);
                process.stdout.write(`\r⏳ Fetched ${allObjects.length} objects...`);
            }
            
            continuationToken = response.NextContinuationToken;
        } catch (error) {
            console.error('\nError listing S3 objects:', error);
            throw error;
        }
    } while (continuationToken);
    
    console.log(`\n✅ Total objects found: ${allObjects.length}`);
    return allObjects;
}

/**
 * Generate CSV content from S3 objects
 * Only includes files that exist in the database
 */
async function generateCsv(prefix, excludeDocumentSnapshots) {
    const startTime = Date.now();
    console.log(`\n🚀 Starting CSV export...`);
    console.log(`   Prefix: ${prefix || '(none)'}`);
    console.log(`   Exclude Document Snapshots: ${excludeDocumentSnapshots}\n`);
    
    try {
        // Step 1: Query database to get all valid media filenames
        console.log('Step 1: Querying database...');
        const validBaseNames = await getValidMediaFilenamesFromDatabase();
        
        if (validBaseNames.size === 0) {
            console.log('⚠️  No media files found in database');
            return generateEmptyCsv();
        }
        
        // Step 2: List all objects from S3
        console.log('\nStep 2: Listing S3 objects...');
        const s3StartTime = Date.now();
        const allObjects = await listAllObjects(prefix);
        console.log(`Listed ${allObjects.length} S3 objects in ${Date.now() - s3StartTime} ms\n`);
        
        if (allObjects.length === 0) {
            console.log('⚠️  No objects found in S3 bucket');
            return generateEmptyCsv();
        }
        
        console.log(`Found ${allObjects.length} objects in S3 bucket, ${validBaseNames.size} valid media files in database\n`);
        
        // Step 3: Build CSV rows
        console.log('Step 3: Processing objects and building CSV...');
        const csvRows = [];
        csvRows.push([
            'S3_Key',
            'File_Name',
            'File_Extension',
            'Media_Type',
            'Path_Type',
            'Size_Bytes',
            'Size_Human_Readable',
            'Last_Modified',
            'Storage_Class',
            'ETag'
        ]);
        
        let processedCount = 0;
        let excludedCount = 0;
        
        for (const obj of allObjects) {
            const s3Key = obj.Key;
            const size = obj.Size || 0;
            
            // Skip S3 folder markers (0-byte objects ending with '/')
            if (size === 0 && s3Key.endsWith('/')) {
                excludedCount++;
                continue;
            }
            
            // Extract filename and base name
            const filename = s3Key.substring(s3Key.lastIndexOf('/') + 1);
            const baseName = getBaseName(filename);
            const extension = getFileExtension(s3Key);
            const mediaType = classifyFileType(s3Key, extension);
            const pathType = getPathType(s3Key);
            
            // Check if this file should be included based on database
            if (!shouldIncludeFile(s3Key, baseName, pathType, validBaseNames)) {
                excludedCount++;
                continue;
            }
            
            // Exclude thumbnails from all folders except CarImages
            if (pathType === 'thumbnail') {
                if (!s3Key.startsWith('CarImages/')) {
                    excludedCount++;
                    continue;
                }
            }
            
            // Handle document exclusions
            if (mediaType === 'documents') {
                if (pathType === 'thumbnail') {
                    excludedCount++;
                    continue;
                }
                if (pathType === 'snapshot' && excludeDocumentSnapshots) {
                    excludedCount++;
                    continue;
                }
            }
            
            // Build CSV row
            const lastModified = obj.LastModified 
                ? obj.LastModified.toISOString().replace('T', ' ').substring(0, 19)
                : '';
            const storageClass = obj.StorageClass || 'STANDARD';
            const etag = obj.ETag ? obj.ETag.replace(/"/g, '') : '';
            
            const row = [
                escapeCsvField(s3Key),
                escapeCsvField(filename),
                escapeCsvField(extension),
                escapeCsvField(mediaType),
                escapeCsvField(pathType),
                escapeCsvField(size),
                escapeCsvField(formatBytes(size)),
                escapeCsvField(lastModified),
                escapeCsvField(storageClass),
                escapeCsvField(etag)
            ];
            
            csvRows.push(row);
            processedCount++;
        }
        
        console.log(`\n✅ CSV generation complete. Processed: ${processedCount}, Excluded: ${excludedCount}`);
        
        // Step 4: Convert to CSV string
        console.log('\nStep 4: Converting to CSV format...');
        const csvStartTime = Date.now();
        const csvContent = csvRows.map(row => row.join(',')).join('\n');
        const csvBytes = Buffer.from(csvContent, 'utf-8');
        
        const totalTime = Date.now() - startTime;
        console.log(`✅ CSV export completed successfully in ${totalTime} ms`);
        console.log(`   CSV size: ${formatBytes(csvBytes.length)} (${csvBytes.length} bytes)`);
        
        return csvBytes;
    } catch (error) {
        const totalTime = Date.now() - startTime;
        console.error(`\n❌ Error during CSV export after ${totalTime} ms:`, error);
        throw error;
    }
}

/**
 * Generate empty CSV with headers only
 */
function generateEmptyCsv() {
    const headers = 'S3_Key,File_Name,File_Extension,Media_Type,Path_Type,Size_Bytes,Size_Human_Readable,Last_Modified,Storage_Class,ETag';
    return Buffer.from(headers, 'utf-8');
}

/**
 * Main execution
 */
async function main() {
    try {
        // Generate CSV
        const csvBytes = await generateCsv(prefix, excludeDocumentSnapshots);
        
        // Determine output filename
        const timestamp = Date.now();
        const defaultFilename = `s3-media-export-${timestamp}.csv`;
        const filename = outputFile || defaultFilename;
        
        // Write CSV file to current directory
        const filePath = path.join(process.cwd(), filename);
        fs.writeFileSync(filePath, csvBytes);
        
        console.log(`\n📄 CSV file saved: ${filePath}`);
        console.log(`   File size: ${formatBytes(csvBytes.length)} (${csvBytes.length} bytes)`);
        console.log(`\n✅ Export complete! You can download the file using:`);
        console.log(`   scp ubuntu@3.95.59.125:~/apps/Node/scripts/Check-S3-bucket-storage/${filename} .\n`);
        
        // Close database connection
        await dbPool.end();
        
    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
        await dbPool.end();
        process.exit(1);
    }
}

// Run the script
if (require.main === module) {
    main();
}

module.exports = { generateCsv, formatBytes, classifyFileType };

