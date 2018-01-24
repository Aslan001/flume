package org.apache.flume.client.avro;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.*;
import org.apache.flume.source.RedisDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * @author zhaohaijun
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableRedisFileEventReader implements RedisReliableEventReader {

    protected static final Logger logger = LoggerFactory.getLogger(ReliableRedisFileEventReader.class);
    private final File metaFile;

    static final String metaFileName = ".flumespool-main.meta";
    private final String deserializerType;
    private final Context deserializerContext;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;
    private final String baseNameHeader;
    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;
    private boolean committed = true;

    private Optional<FileInfo> currentFile = Optional.absent();
    /**
     * Create a ReliableRedisFileEventReader to watch the given directory.
     */
    private ReliableRedisFileEventReader(boolean annotateFileName, String fileNameHeader,
                                            boolean annotateBaseName, String baseNameHeader,
                                            String deserializerType, Context deserializerContext,
                                            String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
                                            String trackerDirPath
                                         ) throws IOException {
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(inputCharset);
        Preconditions.checkNotNull(trackerDirPath);

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}, " +
                            "deserializer={}",
                    new Object[] {
                            ReliableRedisFileEventReader.class.getSimpleName(), deserializerType
                    });
        }

        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.annotateBaseName = annotateBaseName;
        this.baseNameHeader = baseNameHeader;
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);

        File trackerDirectory = new File(trackerDirPath);
        if (!trackerDirectory.isAbsolute()) {
            trackerDirectory = new File("/tmp/", trackerDirPath);
        }

        // ensure that meta directory exists
        if (!trackerDirectory.exists()) {
            if (!trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " + trackerDirectory);
            }
        }

        // ensure that the meta directory is a directory
        if (!trackerDirectory.isDirectory()) {
            throw new IOException("Specified meta directory is not a directory" + trackerDirectory);
        }

        this.metaFile = new File(trackerDirectory, metaFileName);

        if (metaFile.exists() && metaFile.length() == 0) {
            deleteMetaFile();
        }

    }

    private void deleteMetaFile() throws IOException {
        if (metaFile.exists() && !metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + metaFile);
        }
    }

    @Override
    public List<Event> readEvents(int numEvents, List<File> fileList) throws IOException {
        for(File file : fileList){
            logger.debug("file.fileName = {}", file.getName());
            logger.debug("file.filePath = {}", file.getAbsolutePath());
        }
        if (!committed) {
            if (!currentFile.isPresent()) {
                throw new IllegalStateException("File should not roll when " +
                        "commit is outstanding.");
            }
            logger.info("Last read was never committed - resetting mark position.");
            currentFile.get().getDeserializer().reset();
        } else {
            if (!currentFile.isPresent()) {
                currentFile = getNextFile(fileList);
            }
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
        }

        List<Event> events = readDeserializerEvents(numEvents);

        while (events.isEmpty()) {
            logger.info("Last read took us just up to a file boundary. " +
                    "Rolling to the next file, if there is one.");
            retireCurrentFile();
            currentFile = getNextFile(fileList);
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
            events = readDeserializerEvents(numEvents);
        }

        fillHeader(events);
        committed = false;
        return events;
    }

    private Optional<FileInfo> getNextFile(List<File> fileList) {
        if(fileList.size() <= 0){
            Optional.absent();
        }
        File selectedFile = fileList.get(0);
        if (!selectedFile.exists()) {
            return Optional.absent();
        }
        fileList.remove(0);
        return openFile(selectedFile);
    }

    private List<Event> readDeserializerEvents(int numEvents) throws IOException {
        EventDeserializer des = currentFile.get().getDeserializer();
        List<Event> events = des.readEvents(numEvents);
        if (events.isEmpty()) {
            events.add(EventBuilder.withBody(new byte[0]));
        }
        return events;
    }

    private void fillHeader(List<Event> events) {
        if (annotateFileName) {
            String filename = currentFile.get().getFile().getAbsolutePath();
            for (Event event : events) {
                event.getHeaders().put(fileNameHeader, filename);
            }
        }

        if (annotateBaseName) {
            String basename = currentFile.get().getFile().getName();
            for (Event event : events) {
                event.getHeaders().put(baseNameHeader, basename);
            }
        }
    }

    @Override
    public void close() throws IOException {
        currentFile.get().getDeserializer().close();
        currentFile = Optional.absent();
    }

    /**
     * Commit the last lines which were read.
     */
    @Override
    public void commit() throws IOException {
        currentFile.get().getDeserializer().mark();
        committed = true;
    }

    /**
     * Closes currentFile and attempt to rename it.
     * <p>
     * If these operations fail in a way that may cause duplicate log entries,
     * an error is logged but no exceptions are thrown. If these operations fail
     * in a way that indicates potential misuse of the Redis directory, a
     * FlumeException will be thrown.
     *
     * @throws FlumeException if files do not conform to redis assumptions
     */
    private void retireCurrentFile() throws IOException {
        File fileToRoll = new File(currentFile.get().getFile().getAbsolutePath());

        currentFile.get().getDeserializer().close();

        if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
            String message = "File has been modified since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }
        if (fileToRoll.length() != currentFile.get().getLength()) {
            String message = "File has changed size since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }
    }

    /**
     * Opens a file for consuming
     *
     * @param file
     * @return {@link FileInfo} for the file to consume or absent option if the
     * file does not exists or readable.
     */
    private Optional<FileInfo> openFile(File file) {
        try {
            String currentPath = file.getPath();
            PositionTracker tracker =
                    DurablePositionTracker.getInstance(metaFile, currentPath);
            if (!tracker.getTarget().equals(currentPath)) {
                tracker.close();
                tracker = DurablePositionTracker.getInstance(metaFile, currentPath);
            }

            // sanity check
            Preconditions.checkState(tracker.getTarget().equals(currentPath),
                    "Tracker target %s does not equal expected filename %s",
                    tracker.getTarget(), currentPath);

            ResettableInputStream in =
                    new ResettableFileInputStream(file, tracker,
                            ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
                            decodeErrorPolicy);
            EventDeserializer deserializer =
                    EventDeserializerFactory.getInstance(deserializerType, deserializerContext, in);

            return Optional.of(new FileInfo(file, deserializer));
        } catch (FileNotFoundException e) {
            logger.warn("Could not find file: " + file, e);
            return Optional.absent();
        } catch (IOException e) {
            logger.error("Exception opening file: " + file, e);
            return Optional.absent();
        }
    }

    /**
     * An immutable class with information about a file being processed.
     */
    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() {
            return length;
        }

        public long getLastModified() {
            return lastModified;
        }

        public EventDeserializer getDeserializer() {
            return deserializer;
        }

        public File getFile() {
            return file;
        }
    }

    /**
     * Special builder class for ReliableRedisFileEventReader
     */
    public static class Builder {
        private Boolean annotateFileName =
                RedisDirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader =
                RedisDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
        private Boolean annotateBaseName =
                RedisDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
        private String baseNameHeader =
                RedisDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
        private String deserializerType =
                RedisDirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String inputCharset =
                RedisDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
        private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                RedisDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY
                        .toUpperCase(Locale.ENGLISH));
        private String trackerDirPath =
                RedisDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;

        public Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public ReliableRedisFileEventReader build() throws IOException {
            return new ReliableRedisFileEventReader(annotateFileName, fileNameHeader,
                    annotateBaseName, baseNameHeader, deserializerType, deserializerContext,
                    inputCharset, decodeErrorPolicy, trackerDirPath
            );
        }
    }



}
