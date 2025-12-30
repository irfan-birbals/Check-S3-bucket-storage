require('dotenv').config();

const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');

// Configuration
const AWS_REGION = process.env.AWS_REGION;
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const S3_PREFIX = process.env.S3_PREFIX || ''; // Optional prefix filter
const EXCLUDE_DOCUMENT_SNAPSHOTS = process.env.EXCLUDE_DOCUMENT_SNAPSHOTS === 'true'; // Exclude document snapshots from count

// File type definitions by extension
const IMAGE_EXTENSIONS = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'tiff', 'svg'];
const VIDEO_EXTENSIONS = ['mp4', 'avi', 'mov', 'mkv', 'webm', 'flv', 'wmv'];
const DOCUMENT_EXTENSIONS = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf'];

// Initialize S3 client with default credential provider chain
// This will automatically use IAM roles, credentials file, or environment variables
const s3Client = new S3Client({
    region: AWS_REGION
    // No credentials specified - uses default credential provider chain
});

/**
 * Get file extension from key
 * Handles query parameters and path separators
 */
function getFileExtension(key) {
    // Remove query parameters if any (e.g., ?X-Amz-Algorithm=...)
    const keyWithoutQuery = key.split('?')[0];
    
    // Get the filename part (last segment after /)
    const filename = keyWithoutQuery.split('/').pop();
    
    // Extract extension
    const parts = filename.split('.');
    if (parts.length < 2) return 'unknown';
    
    const ext = parts[parts.length - 1].toLowerCase();
    
    // Remove any trailing characters that might not be part of extension
    // (e.g., if there are query params that got through)
    return ext.replace(/[^a-z0-9]/g, '');
}

/**
 * Classify file type by folder path first, then by extension
 * Returns plural form to match stats object keys
 * 
 * IMPORTANT: Files in Documents/ folder are always classified as documents,
 * regardless of extension (e.g., scanned PDFs saved as JPG)
 */
function classifyFileType(key, extension) {
    // First, check folder path - Documents folder takes priority
    if (key.startsWith('Documents/') || key.includes('/Documents/')) {
        return 'documents';
    }
    
    // Then classify by extension
    const ext = extension.trim();
    
    if (IMAGE_EXTENSIONS.includes(ext)) return 'images';
    if (VIDEO_EXTENSIONS.includes(ext)) return 'videos';
    if (DOCUMENT_EXTENSIONS.includes(ext)) return 'documents';
    return 'other';
}

/**
 * Format bytes to human readable format
 */
function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * List all objects in S3 bucket
 */
async function listAllObjects() {
    const allObjects = [];
    let continuationToken = undefined;
    let totalObjects = 0;

    console.log(`\n📦 Scanning S3 bucket: ${S3_BUCKET_NAME}${S3_PREFIX ? ` (prefix: ${S3_PREFIX})` : ''}\n`);

    do {
        try {
            const command = new ListObjectsV2Command({
                Bucket: S3_BUCKET_NAME,
                Prefix: S3_PREFIX,
                ContinuationToken: continuationToken,
                MaxKeys: 1000
            });

            const response = await s3Client.send(command);
            
            if (response.Contents) {
                allObjects.push(...response.Contents);
                totalObjects += response.Contents.length;
                process.stdout.write(`\r⏳ Fetched ${totalObjects} objects...`);
            }

            continuationToken = response.NextContinuationToken;
        } catch (error) {
            console.error('\n❌ Error listing objects:', error.message);
            throw error;
        }
    } while (continuationToken);

    console.log(`\n✅ Total objects found: ${totalObjects}\n`);
    return allObjects;
}

/**
 * Analyze storage by file type
 */
function analyzeStorage(objects, debug = false) {
    const stats = {
        images: {
            count: 0,
            size: 0,
            byExtension: {},
            byPath: {
                original: { count: 0, size: 0 },
                thumbnail: { count: 0, size: 0 },
                reduced: { count: 0, size: 0 }
            }
        },
        videos: {
            count: 0,
            size: 0,
            byExtension: {}
        },
        documents: {
            count: 0,
            size: 0,
            byExtension: {},
            byPath: {
                original: { count: 0, size: 0 },
                snapshot: { count: 0, size: 0 },
                thumbnail: { count: 0, size: 0 }
            }
        },
        other: {
            count: 0,
            size: 0,
            byExtension: {}
        },
        total: {
            count: 0,
            size: 0
        }
    };

    // Debug: track all extensions found
    const debugExtensions = {};

    objects.forEach(obj => {
        const key = obj.Key;
        const size = obj.Size || 0;
        
        // Skip folder markers (0-byte files that end with /)
        if (size === 0 && (key.endsWith('/') || key.split('/').pop() === '')) {
            if (debug) {
                console.log(`[DEBUG] Skipping folder marker: ${key}`);
            }
            return;
        }
        
        const extension = getFileExtension(key);
        const fileType = classifyFileType(key, extension);

        // Debug tracking
        if (debug) {
            if (!debugExtensions[extension]) {
                debugExtensions[extension] = [];
            }
            debugExtensions[extension].push({ key, size, fileType });
        }

        // Update totals
        stats.total.count++;
        stats.total.size += size;

        // Update by file type
        if (debug && (extension === 'jpg' || extension === 'png' || extension === 'webp')) {
            console.log(`[DEBUG] Processing: ${key} | ext: ${extension} | type: ${fileType} | stats[${fileType}]:`, stats[fileType] ? 'exists' : 'undefined');
        }
        
        if (stats[fileType] !== undefined) {
            // For documents, check if this should be excluded BEFORE tracking
            let shouldExcludeFromCount = false;
            let shouldExcludeFromExtension = false;
            
            if (fileType === 'documents') {
                const filename = key.split('/').pop();
                if (key.includes('/thumbnail/')) {
                    shouldExcludeFromCount = true;
                    shouldExcludeFromExtension = true;
                    stats.documents.byPath.thumbnail = stats.documents.byPath.thumbnail || { count: 0, size: 0 };
                    stats.documents.byPath.thumbnail.count++;
                    stats.documents.byPath.thumbnail.size += size;
                } else if (filename.startsWith('snapshot_') && filename.endsWith('.png')) {
                    stats.documents.byPath.snapshot.count++;
                    stats.documents.byPath.snapshot.size += size;
                    // Optionally exclude snapshots from total count
                    if (EXCLUDE_DOCUMENT_SNAPSHOTS) {
                        shouldExcludeFromCount = true;
                        shouldExcludeFromExtension = true;
                    }
                } else {
                    stats.documents.byPath.original.count++;
                    stats.documents.byPath.original.size += size;
                }
            }
            
            // Track by extension (skip if excluded)
            if (!shouldExcludeFromExtension) {
                if (!stats[fileType].byExtension[extension]) {
                    stats[fileType].byExtension[extension] = { count: 0, size: 0 };
                }
                stats[fileType].byExtension[extension].count++;
                stats[fileType].byExtension[extension].size += size;
            }
            
            stats[fileType].count++;
            stats[fileType].size += size;
            
            // If excluded, decrement the counts
            if (shouldExcludeFromCount) {
                stats[fileType].count--;
                stats[fileType].size -= size;
                stats.total.count--;
                stats.total.size -= size;
            }
            
            // For images, track by path (original, thumbnail, reduced)
            if (fileType === 'images') {
                if (key.includes('/thumbnail/')) {
                    stats.images.byPath.thumbnail.count++;
                    stats.images.byPath.thumbnail.size += size;
                } else if (key.includes('/reduced/')) {
                    stats.images.byPath.reduced.count++;
                    stats.images.byPath.reduced.size += size;
                } else {
                    stats.images.byPath.original.count++;
                    stats.images.byPath.original.size += size;
                }
            }
            
            if (debug && (extension === 'jpg' || extension === 'png' || extension === 'webp')) {
                console.log(`[DEBUG] After update - stats.images.count: ${stats.images.count}, stats.images.size: ${stats.images.size}`);
            }
        } else if (debug) {
            console.error(`⚠️  Warning: Unknown fileType "${fileType}" for extension "${extension}" in key "${key}"`);
            console.error(`   Available stats keys:`, Object.keys(stats));
        }
    });

    // Debug output
    if (debug) {
        console.log('\n🔍 DEBUG: File Extensions Found\n');
        console.log('-'.repeat(80));
        Object.keys(debugExtensions)
            .sort()
            .forEach(ext => {
                const files = debugExtensions[ext];
                console.log(`Extension: .${ext} (${files.length} files, classified as: ${files[0].fileType})`);
                files.slice(0, 5).forEach(file => {
                    const isSnapshot = file.key.split('/').pop().startsWith('snapshot_');
                    console.log(`  - ${file.key} (${formatBytes(file.size)})${isSnapshot ? ' [SNAPSHOT]' : ''}`);
                });
                if (files.length > 5) {
                    console.log(`  ... and ${files.length - 5} more`);
                }
                console.log();
            });
        console.log('-'.repeat(80));
        console.log();
        
        // Show all documents with details
        if (stats.documents.count > 0 || stats.documents.byPath.original.count > 0 || stats.documents.byPath.snapshot.count > 0 || (stats.documents.byPath.thumbnail && stats.documents.byPath.thumbnail.count > 0)) {
            console.log('🔍 DEBUG: All Documents Found\n');
            console.log('-'.repeat(80));
            console.log(`Total Documents (in count): ${stats.documents.count}`);
            console.log(`  Original: ${stats.documents.byPath.original.count}`);
            if (stats.documents.byPath.snapshot.count > 0) {
                console.log(`  Snapshot: ${stats.documents.byPath.snapshot.count}${EXCLUDE_DOCUMENT_SNAPSHOTS ? ' (excluded)' : ''}`);
            }
            if (stats.documents.byPath.thumbnail && stats.documents.byPath.thumbnail.count > 0) {
                console.log(`  Thumbnail: ${stats.documents.byPath.thumbnail.count} (excluded from count)`);
            }
            console.log('-'.repeat(80));
            console.log();
        }
    }

    return stats;
}

/**
 * Print detailed report
 */
function printReport(stats) {
    // Debug: Check stats before printing
    if (process.env.DEBUG === 'true' || process.argv.includes('--debug')) {
        console.log('\n[DEBUG] Stats object before printing:');
        console.log('  stats.images.count:', stats.images.count);
        console.log('  stats.images.size:', stats.images.size);
        console.log('  stats.images.byExtension:', Object.keys(stats.images.byExtension));
        console.log();
    }
    
    console.log('='.repeat(80));
    console.log('📊 S3 STORAGE ANALYSIS REPORT');
    console.log('='.repeat(80));
    console.log(`Bucket: ${S3_BUCKET_NAME}${S3_PREFIX ? ` | Prefix: ${S3_PREFIX}` : ''}`);
    console.log(`Region: ${AWS_REGION}`);
    console.log('='.repeat(80));
    console.log();

    // Total Summary
    console.log('📈 TOTAL SUMMARY');
    console.log('-'.repeat(80));
    console.log(`Total Files:     ${stats.total.count.toLocaleString()}`);
    console.log(`Total Size:       ${formatBytes(stats.total.size)} (${stats.total.size.toLocaleString()} bytes)`);
    console.log();

    // Images Breakdown
    console.log('🖼️  IMAGES');
    console.log('-'.repeat(80));
    console.log(`Total Images:     ${stats.images.count.toLocaleString()}`);
    console.log(`Total Size:       ${formatBytes(stats.images.size)} (${stats.images.size.toLocaleString()} bytes)`);
    console.log(`Percentage:       ${((stats.images.size / stats.total.size) * 100).toFixed(2)}%`);
    console.log();
    console.log('  By Path:');
    console.log(`    Original:     ${stats.images.byPath.original.count.toLocaleString()} files | ${formatBytes(stats.images.byPath.original.size)}`);
    console.log(`    Thumbnail:    ${stats.images.byPath.thumbnail.count.toLocaleString()} files | ${formatBytes(stats.images.byPath.thumbnail.size)}`);
    console.log(`    Reduced:      ${stats.images.byPath.reduced.count.toLocaleString()} files | ${formatBytes(stats.images.byPath.reduced.size)}`);
    console.log();
    console.log('  By Extension:');
    Object.keys(stats.images.byExtension)
        .sort((a, b) => stats.images.byExtension[b].size - stats.images.byExtension[a].size)
        .forEach(ext => {
            const extStats = stats.images.byExtension[ext];
            console.log(`    .${ext.padEnd(6)} ${extStats.count.toString().padStart(8)} files | ${formatBytes(extStats.size).padStart(12)} | ${((extStats.size / stats.images.size) * 100).toFixed(2)}%`);
        });
    console.log();

    // Videos Breakdown
    console.log('🎥 VIDEOS');
    console.log('-'.repeat(80));
    console.log(`Total Videos:     ${stats.videos.count.toLocaleString()}`);
    console.log(`Total Size:       ${formatBytes(stats.videos.size)} (${stats.videos.size.toLocaleString()} bytes)`);
    console.log(`Percentage:       ${((stats.videos.size / stats.total.size) * 100).toFixed(2)}%`);
    console.log();
    console.log('  By Extension:');
    Object.keys(stats.videos.byExtension)
        .sort((a, b) => stats.videos.byExtension[b].size - stats.videos.byExtension[a].size)
        .forEach(ext => {
            const extStats = stats.videos.byExtension[ext];
            console.log(`    .${ext.padEnd(6)} ${extStats.count.toString().padStart(8)} files | ${formatBytes(extStats.size).padStart(12)} | ${((extStats.size / stats.videos.size) * 100).toFixed(2)}%`);
        });
    console.log();

    // Documents Breakdown
    console.log('📄 DOCUMENTS');
    console.log('-'.repeat(80));
    console.log(`Total Documents:  ${stats.documents.count.toLocaleString()}${EXCLUDE_DOCUMENT_SNAPSHOTS ? ' (snapshots excluded)' : ''}`);
    console.log(`Total Size:       ${formatBytes(stats.documents.size)} (${stats.documents.size.toLocaleString()} bytes)`);
    console.log(`Percentage:       ${((stats.documents.size / stats.total.size) * 100).toFixed(2)}%`);
    console.log();
    console.log('  By Path:');
    console.log(`    Original:     ${stats.documents.byPath.original.count.toLocaleString()} files | ${formatBytes(stats.documents.byPath.original.size)}`);
    if (stats.documents.byPath.snapshot.count > 0) {
        console.log(`    Snapshot:     ${stats.documents.byPath.snapshot.count.toLocaleString()} files | ${formatBytes(stats.documents.byPath.snapshot.size)}${EXCLUDE_DOCUMENT_SNAPSHOTS ? ' (excluded from total)' : ''}`);
    }
    console.log();
    console.log('  By Extension:');
    Object.keys(stats.documents.byExtension)
        .sort((a, b) => stats.documents.byExtension[b].size - stats.documents.byExtension[a].size)
        .forEach(ext => {
            const extStats = stats.documents.byExtension[ext];
            console.log(`    .${ext.padEnd(6)} ${extStats.count.toString().padStart(8)} files | ${formatBytes(extStats.size).padStart(12)} | ${((extStats.size / stats.documents.size) * 100).toFixed(2)}%`);
        });
    console.log();

    // Other Files
    if (stats.other.count > 0) {
        console.log('❓ OTHER FILES');
        console.log('-'.repeat(80));
        console.log(`Total Other:      ${stats.other.count.toLocaleString()}`);
        console.log(`Total Size:       ${formatBytes(stats.other.size)} (${stats.other.size.toLocaleString()} bytes)`);
        console.log(`Percentage:       ${((stats.other.size / stats.total.size) * 100).toFixed(2)}%`);
        console.log();
        console.log('  By Extension:');
        Object.keys(stats.other.byExtension)
            .sort((a, b) => stats.other.byExtension[b].size - stats.other.byExtension[a].size)
            .forEach(ext => {
                const extStats = stats.other.byExtension[ext];
                console.log(`    .${ext.padEnd(6)} ${extStats.count.toString().padStart(8)} files | ${formatBytes(extStats.size).padStart(12)}`);
            });
        console.log();
    }

    // Show unknown extensions if any
    if (stats.other.byExtension['unknown'] && stats.other.byExtension['unknown'].count > 0) {
        console.log('⚠️  WARNING: Found files without extensions');
        console.log(`   ${stats.other.byExtension['unknown'].count} files with unknown extension`);
        console.log('   Run with DEBUG=true to see file details');
        console.log();
    }

    // Summary by Type
    console.log('📊 SUMMARY BY TYPE');
    console.log('-'.repeat(80));
    console.log(`Images:           ${formatBytes(stats.images.size).padStart(12)} | ${((stats.images.size / stats.total.size) * 100).toFixed(2).padStart(6)}% | ${stats.images.count.toLocaleString().padStart(8)} files`);
    console.log(`Videos:           ${formatBytes(stats.videos.size).padStart(12)} | ${((stats.videos.size / stats.total.size) * 100).toFixed(2).padStart(6)}% | ${stats.videos.count.toLocaleString().padStart(8)} files`);
    console.log(`Documents:        ${formatBytes(stats.documents.size).padStart(12)} | ${((stats.documents.size / stats.total.size) * 100).toFixed(2).padStart(6)}% | ${stats.documents.count.toLocaleString().padStart(8)} files`);
    if (stats.other.count > 0) {
        console.log(`Other:            ${formatBytes(stats.other.size).padStart(12)} | ${((stats.other.size / stats.total.size) * 100).toFixed(2).padStart(6)}% | ${stats.other.count.toLocaleString().padStart(8)} files`);
    }
    console.log('-'.repeat(80));
    console.log(`TOTAL:            ${formatBytes(stats.total.size).padStart(12)} | ${'100.00'.padStart(6)}% | ${stats.total.count.toLocaleString().padStart(8)} files`);
    console.log('='.repeat(80));
}

/**
 * Main execution
 */
async function main() {
    try {
        console.log('🚀 Starting S3 Storage Analysis...\n');

        // Validate bucket name
        if (!S3_BUCKET_NAME || S3_BUCKET_NAME === 'micurato-design-1') {
            console.log('ℹ️  Using default bucket: micurato-design-1');
            console.log('   Set S3_BUCKET_NAME environment variable to use a different bucket\n');
        }

        // List all objects
        const objects = await listAllObjects();

        if (objects.length === 0) {
            console.log('⚠️  No objects found in the bucket.');
            return;
        }

        // Analyze storage (enable debug mode to see file extensions)
        const DEBUG_MODE = process.env.DEBUG === 'true' || process.argv.includes('--debug');
        console.log('📊 Analyzing storage...\n');
        const stats = analyzeStorage(objects, DEBUG_MODE);

        // Print report
        printReport(stats);

        console.log('\n✅ Analysis complete!\n');

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        
        // Provide helpful error messages for common authentication issues
        if (error.name === 'CredentialsProviderError' || error.message.includes('credentials')) {
            console.error('\n💡 Authentication Help:');
            console.error('   The script uses AWS SDK default credential provider chain.');
            console.error('   Ensure one of the following is configured:');
            console.error('   1. IAM role (if running on EC2/ECS/Lambda)');
            console.error('   2. AWS credentials file (~/.aws/credentials)');
            console.error('   3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)');
            console.error('   4. AWS SSO credentials');
        }
        
        if (error.name === 'NoSuchBucket' || error.message.includes('bucket')) {
            console.error('\n💡 Bucket Help:');
            console.error(`   Check if bucket "${S3_BUCKET_NAME}" exists and you have access to it.`);
            console.error('   Set S3_BUCKET_NAME environment variable if using a different bucket.');
        }
        
        if (error.stack && process.env.DEBUG) {
            console.error('\nStack trace:');
            console.error(error.stack);
        }
        process.exit(1);
    }
}

// Run the script
if (require.main === module) {
    main();
}


module.exports = { analyzeStorage, formatBytes, classifyFileType };
