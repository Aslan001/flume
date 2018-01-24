package org.apache.flume.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.*;
import org.apache.flume.client.avro.ReliableRedisFileEventReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.LineDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.source.RedisDirectorySourceConfigurationConstants.*;

/**
 * @author zhaohaijun
 */
public class RedisDirectorySource extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(RedisDirectorySource.class);

    private static List<File> fileList = new ArrayList<>();
    private final Subscriber subscriber = new Subscriber();
    private Jedis jedis;

    private boolean fileHeader;
    private String fileHeaderKey;
    private boolean basenameHeader;
    private String basenameHeaderKey;
    private int batchSize;
    private String deserializerType;
    private Context deserializerContext;
    private String inputCharset;
    private DecodeErrorPolicy decodeErrorPolicy;

    private SourceCounter sourceCounter;
    ReliableRedisFileEventReader reader;
    private ScheduledExecutorService executor;
    private boolean backoff = true;
    private int maxBackoff;
    private int pollDelay;
    private String redisHost;
    private Integer redisPort;
    private String redisPassword;
    private Integer redisTimeout;
    private String bdeChannel;
    private String sandboxChannel;
    private String bdeFieldsJsonPath;
    private String trackerDirPath;

    @Override
    public synchronized void start() {
        executor = Executors.newSingleThreadScheduledExecutor();
        SubThread subThread = new SubThread();
        subThread.start();
        try {
            reader = new ReliableRedisFileEventReader.Builder()
                    .annotateFileName(fileHeader)
                    .fileNameHeader(fileHeaderKey)
                    .annotateBaseName(basenameHeader)
                    .baseNameHeader(basenameHeaderKey)
                    .deserializerType(deserializerType)
                    .deserializerContext(deserializerContext)
                    .inputCharset(inputCharset)
                    .decodeErrorPolicy(decodeErrorPolicy)
                    .build();
        } catch (Exception ioe) {
            throw new FlumeException("Error instantiating redis event parser",
                    ioe);
        }

        Runnable runner = new RedisDirectoryRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(
                runner, 0, pollDelay, TimeUnit.MILLISECONDS);

        super.start();
        logger.debug("RedisDirectorySource source started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("RedisDir source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public synchronized void configure(Context context) {
        fileHeader = context.getBoolean(FILENAME_HEADER,
                DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
                DEFAULT_FILENAME_HEADER_KEY);
        basenameHeader = context.getBoolean(BASENAME_HEADER,
                DEFAULT_BASENAME_HEADER);
        basenameHeaderKey = context.getString(BASENAME_HEADER_KEY,
                DEFAULT_BASENAME_HEADER_KEY);
        batchSize = context.getInteger(BATCH_SIZE,
                DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
                        .toUpperCase(Locale.ENGLISH));

        deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(DESERIALIZER +
                "."));

        pollDelay = context.getInteger(POLL_DELAY, DEFAULT_POLL_DELAY);

        redisHost = context.getString(REDIS_HOST, DEFAULT_REDIS_HOST);
        redisPort = context.getInteger(REDIS_PORT, DEFAULT_REDIS_PORT);
        redisPassword = context.getString(REDIS_PASSWORD, DEFAULT_REDIS_PASSWORD);
        redisTimeout = context.getInteger(REDIS_TIMEOUT, DEFAULT_REDIS_TIMEOUT);
        bdeChannel = context.getString(BDE_CHANNEL, DEFAULT_BDE_CHANNEL);
        sandboxChannel = context.getString(SANDBOX_CHANNEL, DEFAULT_SANDBOX_CHANNEL);
        bdeFieldsJsonPath = context.getString(BDE_FIELDS_JSON_PATH, DEFAULT_BDE_FIELDS_JSON_PATH);
        trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);

        Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH);
        if (bufferMaxLineLength != null && deserializerType != null &&
                deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
            deserializerContext.put(LineDeserializer.MAXLINE_KEY,
                    bufferMaxLineLength.toString());
        }

        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    private class RedisDirectoryRunnable implements Runnable {
        private ReliableRedisFileEventReader reader;
        private SourceCounter sourceCounter;

        public RedisDirectoryRunnable(ReliableRedisFileEventReader reader,
                                      SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {
                    List<Event> events = reader.readEvents(batchSize, fileList);
                    if (events.isEmpty()) {
                        break;
                    }
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelFullException ex) {
                        logger.warn("The channel is full, and cannot write data now. The " +
                                "source will try again after " + backoffInterval +
                                " milliseconds");
                        backoffInterval = waitAndGetNewBackoffInterval(backoffInterval);
                        continue;
                    } catch (ChannelException ex) {
                        logger.warn("The channel threw an exception, and cannot write data now. The " +
                                "source will try again after " + backoffInterval +
                                " milliseconds");
                        backoffInterval = waitAndGetNewBackoffInterval(backoffInterval);
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
            } catch (Throwable t) {
                logger.error("FATAL: " + RedisDirectorySource.this.toString() + ": " +
                        "Uncaught exception in RedisDirectorySource thread. " +
                        "Restart or reconfigure Flume to continue processing.", t);
                Throwables.propagate(t);
            }
        }

        private int waitAndGetNewBackoffInterval(int backoffInterval) throws InterruptedException {
            if (backoff) {
                TimeUnit.MILLISECONDS.sleep(backoffInterval);
                backoffInterval = backoffInterval << 1;
                backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                        backoffInterval;
            }
            return backoffInterval;
        }
    }

    public class SubThread extends Thread {
        @Override
        public void run() {
            String[] redisChannels = {bdeChannel, sandboxChannel};
            try {
                connect();
                jedis.subscribe(subscriber, redisChannels);
            } catch (JedisConnectionException e) {
                logger.error("Disconnected from Redis {} ...", redisChannels);
                connect();
            } catch (Exception e) {
                logger.error("Fatal error occurred when subscribe redis channel.");
                logger.error(ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private class Subscriber extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            String filePath = "";
            try {
                if(bdeChannel.equalsIgnoreCase(channel)){
                    String[] csvField = getAtdCsvFieldMap(bdeFieldsJsonPath).get("bde_notice");
                    Map<String, String> csvMap = csv2Map(csvField, message);
                    if(csvMap.containsKey("pcap_record") && StringUtils.isNotBlank(csvMap.get("pcap_record"))){
                        filePath = csvMap.get("pcap_record");
                    } else if (csvMap.containsKey("resp_data") && StringUtils.isNotBlank(csvMap.get("resp_data"))){
                        filePath = csvMap.get("resp_data");
                    }
                }else if(sandboxChannel.equalsIgnoreCase(channel)){
                    logger.debug("not handle yet...");
                }
                logger.debug("file path = {}", filePath);
                File file = new File(filePath);
                if(file.exists()){
                    fileList.add(file);
                }else {
                    logger.error("file is not exists : {}", filePath);
                }
            } catch (Exception e) {
                logger.error(ExceptionUtils.getStackTrace(e));
                logger.warn("Original data that trigger this exception is: " + message);
            }
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {}

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            logger.info("onSubscribe (Channel: " + channel + ")");
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            logger.info("onUnsubscribe (Channel: " + channel + ")");
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {}

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {}
    }

    protected void connect() {
        logger.info("Connecting...");
        while (true) {
            try {
                jedis = new Jedis(redisHost, redisPort, redisTimeout);
                if (!"".equals(redisPassword)) {
                    jedis.auth(redisPassword);
                } else {
                    // Force a connection.
                    jedis.ping();
                }
                break;
            } catch (JedisConnectionException e) {
                logger.error("Connection failed.", e);
                logger.info("Waiting for 10 seconds...");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            }
        }
        logger.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort) + ", timeout: "
                + String.valueOf(redisTimeout) + ")");
    }

    public static HashMap<String, String[]> getAtdCsvFieldMap(String path) {
        HashMap<String, String[]> fieldMap = new HashMap<>();
        String channelName;
        ArrayList<String> csvFieldList;
        JSONObject jsonObject = loadJson(path);
        Iterator<String> sIterator = jsonObject.keys();
        while(sIterator.hasNext()){
            channelName = sIterator.next();
            csvFieldList = new ArrayList<>();
            JSONArray fieldAttributes = jsonObject.getJSONArray(channelName);
            if(fieldAttributes.length() > 0){
                for(int i=0; i<fieldAttributes.length(); i++){
                    JSONObject attributes = fieldAttributes.getJSONObject(i);
                    if(attributes.has("name")){
                        csvFieldList.add(attributes.getString("name"));
                    } else {
                        csvFieldList.add("unset" + String.valueOf(i));
                    }
                }
            }
            fieldMap.put(channelName, csvFieldList.toArray(new String[0]));
        }
        return fieldMap;
    }

    public static JSONObject loadJson(String path){
        JSONObject jsonConf = null;
        try {
            jsonConf = new JSONObject(new String(Files.readAllBytes(Paths.get(path))));
            logger.warn(String.format("Load conf file of '%s' successfully.", path));
        }catch (Exception e) {
            logger.error(String.format("Load conf file of '%s' failed.", path));
            logger.error(ExceptionUtils.getStackTrace(e));
        }
        return (jsonConf != null) ? jsonConf : new JSONObject();
    }

    public static HashMap<String, String> csv2Map(String[] fields, String csvData) {
        if (fields == null || csvData == null || fields.length <= 0 || "".equals(csvData)) {
            logger.warn("Csv field list or data is null or empty! \n"
                    + String.format("fields: %s, csvData: %s", (fields == null ? "null" : Arrays.toString(fields)), csvData));
            return null;
        }

        HashMap<String, String> map = new HashMap<String, String>();
        String[] datas = csvData.split("\\^", -1);

        int fieldLength = fields.length;
        int index;

        int length = (fields.length > datas.length) ? datas.length : fields.length;
        for (index = 0; index < length; index++) {
            map.put(fields[index], datas[index]);
        }

        for (; length < fieldLength; length++) {
            map.put(fields[length], "");
        }
        return map;
    }
}
