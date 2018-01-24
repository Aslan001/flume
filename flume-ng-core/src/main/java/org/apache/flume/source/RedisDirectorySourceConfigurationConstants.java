package org.apache.flume.source;

import org.apache.flume.serialization.DecodeErrorPolicy;

/**
 * @author zhaohaijun
 */
public class RedisDirectorySourceConfigurationConstants {
    /** Header in which to put absolute path filename. */
    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

    /** Whether to include absolute path filename in a header. */
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = false;

    /** Header in which to put the basename of file. */
    public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
    public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";

    /** Whether to include the basename of a file in a header. */
    public static final String BASENAME_HEADER = "basenameHeader";
    public static final boolean DEFAULT_BASENAME_HEADER = false;

    /** What size to batch with before sending to ChannelProcessor. */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /** Maximum number of lines to buffer between commits. */
    @Deprecated
    public static final String BUFFER_MAX_LINES = "bufferMaxLines";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINES = 100;

    /** Maximum length of line (in characters) in buffer between commits. */
    @Deprecated
    public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;

    /** Directory to store metadata about files being processed */
    public static final String TRACKER_DIR = "trackerDir";
    public static final String DEFAULT_TRACKER_DIR = ".flumespool";

    /** Deserializer to use to parse the file data into Flume Events */
    public static final String DESERIALIZER = "deserializer";
    public static final String DEFAULT_DESERIALIZER = "LINE";

    /** Character set used when reading the input. */
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    /** What to do when there is a character set decoding error. */
    public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();

    public static final String MAX_BACKOFF = "maxBackoff";

    public static final Integer DEFAULT_MAX_BACKOFF = 4000;

    public static final String REDIS_HOST = "redisHost";
    public static final String DEFAULT_REDIS_HOST = "localhost";

    public static final String REDIS_PORT = "redisPort";
    public static final Integer DEFAULT_REDIS_PORT = 6379;

    public static final String REDIS_PASSWORD = "redisPassword";
    public static final String DEFAULT_REDIS_PASSWORD = "";

    public static final String REDIS_TIMEOUT = "redisTimeout";
    public static final Integer DEFAULT_REDIS_TIMEOUT = 30;

    public static final String BDE_CHANNEL = "bdeChannel";
    public static final String DEFAULT_BDE_CHANNEL = "bde_notice";

    public static final String SANDBOX_CHANNEL = "sandboxChannel";
    public static final String DEFAULT_SANDBOX_CHANNEL = "awesome_notice";

    public static final String BDE_FIELDS_JSON_PATH = "bdeFieldsJsonPath";
    public static final String DEFAULT_BDE_FIELDS_JSON_PATH = "/home/soft/resource/atd-fields/bde_field.json";

    /** Delay(in milliseconds) used when polling for new files. The default is 500ms */
    public static final String POLL_DELAY = "pollDelay";
    public static final int DEFAULT_POLL_DELAY = 500;
}
