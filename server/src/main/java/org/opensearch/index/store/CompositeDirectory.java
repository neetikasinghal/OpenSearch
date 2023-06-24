/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.index.store.remote.file.OnDemandBlockSearchIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCachedIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * needs an update
 */
public class CompositeDirectory extends FilterDirectory {

    // can be changed to Directory
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final TransferManager transferManager;
    private final FileCache fileCache;
    /**
     * Underlying lucene directory for caching
     */
    private final FSDirectory localCacheDir;
    private final FileTrackerImp fileTrackerImp;

    public CompositeDirectory(FSDirectory localDirectory, RemoteSegmentStoreDirectory remoteDirectory,
                              TransferManager transferManager, FileCache fileCache, FSDirectory localCacheDir,
                              FileTrackerImp fileTrackerImp) {
        super(localDirectory);
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.transferManager = transferManager;
        this.fileCache = fileCache;
        this.localCacheDir = localCacheDir;
        this.fileTrackerImp = fileTrackerImp;
    }

    @Override
    public String[] listAll() throws IOException {
        return localDirectory.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        localDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return localDirectory.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return localDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return localDirectory.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        localDirectory.sync(names);
    }

    public void afterUpload(Collection<String> names) throws IOException {
        // upload to remote store, add to the filetracker
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> segmentsUploadedToRemoteStore = remoteDirectory.getSegmentsUploadedToRemoteStore();
        Map<String, FileTrackingInfo> fileTrackingInfoMap = fileTrackerImp.getFileTrackingInfoMap();
        for (String file : names) {
            final IndexInput luceneIndexInput = localDirectory.openInput(file, IOContext.READ);
            Path path = localDirectory.getDirectory().resolve(file);
            FileTrackingInfo fileTrackingInfo = fileTrackingInfoMap.get(file);
            if(fileTrackingInfo != null && FileTrackingInfo.FileState.DISK.equals(fileTrackingInfo.getFileState())) {
                // since now the file has been uploaded, decrementing the ref count to make it evictable
                fileCache.decRef(path);
            } else {
                // to check - if the file is already added to the cache and there is an update to the file, do we need a new
                // object creation here? i think yes
                FileCachedIndexInput fileCachedIndexInput = new FileCachedIndexInput(fileCache, path, luceneIndexInput);
                // add to the file cache
                fileCache.put(path, fileCachedIndexInput);
                // add to the file tracker
            }
            fileTrackingInfoMap.put(file, new FileTrackingInfo(file, FileTrackingInfo.FileState.CACHE,
                FileTrackingInfo.FileType.NON_BLOCK, path, segmentsUploadedToRemoteStore.get(file)));
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        localDirectory.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        localDirectory.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        // check the filetracker
        Path key = localDirectory.getDirectory().resolve(name);
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> segmentsUploadedToRemoteStore = remoteDirectory.getSegmentsUploadedToRemoteStore();
        Map<String, FileTrackingInfo> fileTrackingInfoMap = fileTrackerImp.getFileTrackingInfoMap();
        if (!fileTrackerImp.isPresent(name)) {
            final IndexInput luceneIndexInput = localDirectory.openInput(name, context);
            FileCachedIndexInput fileCachedIndexInput = new FileCachedIndexInput(fileCache, key, luceneIndexInput);
            // add to the file cache
            fileCache.put(key, fileCachedIndexInput);

            // add to the file tracker
            FileTrackingInfo fileTrackingInfo = new FileTrackingInfo(name, FileTrackingInfo.FileState.DISK,
                FileTrackingInfo.FileType.NON_BLOCK, key, segmentsUploadedToRemoteStore.get(name));
            fileTrackingInfoMap.put(name, fileTrackingInfo);

            return fileCachedIndexInput.clone();
        }

        CachedIndexInput cachedIndexInput = fileCache.get(key);
        if (cachedIndexInput != null) {
            try {
                return cachedIndexInput.getIndexInput().clone();
            } finally {
                fileCache.decRef(key);
            }
        }

        FileTrackingInfo fileTrackingInfo = fileTrackingInfoMap.get(name);
        if(FileTrackingInfo.FileState.REMOTE_ONLY.equals(fileTrackingInfo.getFileState())) {
            // later - add IOContext decider
            fileTrackerImp.updateFileType(name, FileTrackingInfo.FileType.BLOCK);
        }
        // later - check the file tracker type
//        if(FileTrackingInfo.FileType.BLOCK.equals(fileTrackingInfo.getFileType())) {
//        }
        return new OnDemandBlockSearchIndexInput(fileTrackingInfo.getUploadedSegmentMetadata(), localCacheDir, transferManager);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return localDirectory.obtainLock(name);
    }

    @Override
    public void close() throws IOException {
        localDirectory.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return localDirectory.getPendingDeletions();
    }

    public FSDirectory localDirectory() {
        return localDirectory;
    }

    public RemoteSegmentStoreDirectory remoteDirectory() {
        return remoteDirectory;
    }
}
