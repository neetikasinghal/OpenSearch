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
        return new String[0];
    }

    @Override
    public void deleteFile(String name) throws IOException {

    }

    @Override
    public long fileLength(String name) throws IOException {
        return 0;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return localDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        localDirectory.sync(names);
    }

    public void afterUpload(Collection<String> names) throws IOException {
        // upload to remote store, add to the filetracker
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> segmentsUploadedToRemoteStore = remoteDirectory.getSegmentsUploadedToRemoteStore();
        for (String file : names) {
            final IndexInput luceneIndexInput = localDirectory.openInput(file, IOContext.READ);
            Path path = localDirectory.getDirectory().resolve(file);
            FileCachedIndexInput fileCachedIndexInput = new FileCachedIndexInput(fileCache, path, luceneIndexInput);
            // have another impl of CachedIndexInput and put that here
            // add to the file cache
            fileCache.put(path, fileCachedIndexInput);

            // add to the file tracker
            FileTrackingInfo fileTrackingInfo = new FileTrackingInfo(file, FileTrackingInfo.FileState.CACHE,
                FileTrackingInfo.FileType.NON_BLOCK, path, segmentsUploadedToRemoteStore.get(file));
            fileTrackerImp.getFileTrackingInfoMap().put(file, fileTrackingInfo);
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        localDirectory.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {

    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        // check the filetracker
        if (!fileTrackerImp.isPresent(name)) {
            return localDirectory.openInput(name, context);
        }
        FileTrackingInfo fileTrackingInfo = fileTrackerImp.getFileTrackingInfoMap().get(name);
        if(FileTrackingInfo.FileState.REMOTE_ONLY.equals(fileTrackingInfo.getFileState())) {
            // later - add IOContext decider
            fileTrackerImp.updateFileType(name, FileTrackingInfo.FileType.BLOCK);
        }

//        if(FileTrackingInfo.FileType.BLOCK.equals(fileTrackingInfo.getFileType())) {
//        }
        return new OnDemandBlockSearchIndexInput(fileTrackingInfo.getUploadedSegmentMetadata(), localCacheDir, transferManager);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return null;
    }
}
