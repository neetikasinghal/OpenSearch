/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.*;
import org.opensearch.index.store.remote.file.OnDemandBlockSearchIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.*;

/**
 * needs an update
 */
public class HybridDirectory extends Directory {
    private final TransferManager transferManager;
    /**
     * Underlying lucene directory for caching
     */
    private final FSDirectory localStoreDir;
    public HybridDirectory(TransferManager transferManager, FSDirectory localStoreDir) {
        this.transferManager = transferManager;
        this.localStoreDir = localStoreDir;
    }

    @Override
    public String[] listAll() throws IOException {
        // get all files which are not yet uploaded to the remote store - those will be present in the FSDirectory
        // get all the files which are uploaded to the remote store
        // ?? file tracker ??
        return null;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        // if the file is present in the base directory - delete from there else delete from the block directory
    }

    @Override
    public long fileLength(String name) throws IOException {
        // if its a block file, then call the block directory file length
        return 0;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        // add to the file cache
        return null;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
    }

    @Override
    public void syncMetaData() throws IOException {
    }

    @Override
    public void rename(String source, String dest) throws IOException {
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        // there needs to a decider to decide where the file is ?? file tracker can be helpful
        // ?? : how will we figure out if the file has been uploaded or no - can we use uploadedSegmentMetadataMap? if only local or if only remote
        // based on the IOContext, we can decide which directory we want to read from - idea

        // if the file tracker state is remote only, use a decider to decide the file type

        // check if the file type is BLOCK
//        return new OnDemandBlockSearchIndexInput();
//        else
//        return new OnDemandNonBlockSearchIndexInput();
        return null;
    }

    public IndexInput openInput(FileTrackingInfo fileTrackingInfo) throws IOException {
//        if(FileTrackingInfo.FileType.BLOCK.equals(fileTrackingInfo.getFileType())) {
//        }
        return new OnDemandBlockSearchIndexInput(fileTrackingInfo.getUploadedSegmentMetadata(), localStoreDir, transferManager);
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


