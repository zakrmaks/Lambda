package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.thebuzzmedia.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LambdaMethodHandler implements RequestHandler<S3Event, String> {
    private static final Logger logger = LoggerFactory.getLogger(LambdaMethodHandler.class);
    private final String REGEX = ".*\\.([^\\.]*)";
    private final String JPG_TYPE = "jpg";
    private final String JPG_MIME = "image/jpeg";
    private final String PNG_TYPE = "png";
    private final String PNG_MIME = "image/png";
    private final Integer targetWidth = 250;
    private final Integer targetHeight = 250;
    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {


            S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);

            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getUrlDecodedKey();
            String srcBucket = record.getS3().getBucket().getName();

            String dstBucket = "outputmentorbucketdata";
            String dstKey = "resized-" + srcKey;

            // Infer the image type.
            Matcher matcher = Pattern.compile(REGEX).matcher(srcKey);
            if (!matcher.matches()) {
            logger.info("Unable to infer image type for key " + srcKey);
                return "";
            }
            String imageType = matcher.group(1);
            if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType))) {
            logger.info("Skipping non-image " + srcKey);
                return "";
            }
            S3Client s3Client = S3Client.builder().build();
            InputStream s3Object = getObject(s3Client, srcBucket, srcKey);
            BufferedImage srcImage = ImageIO.read(s3Object);
            BufferedImage newImage = Scalr.resize(srcImage, targetWidth, targetHeight);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(newImage, imageType, outputStream);

            putObject(s3Client, outputStream, dstBucket, dstKey, imageType);
            return "OK";
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }


    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }
    private void putObject(S3Client s3Client, ByteArrayOutputStream outputStream,
                           String bucket, String key, String imageType) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("Content-Length", Integer.toString(outputStream.size()));
        if (JPG_TYPE.equals(imageType)) {
            metadata.put("Content-Type", JPG_MIME);
        } else if (PNG_TYPE.equals(imageType)) {
            metadata.put("Content-Type", PNG_MIME);
        }

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .metadata(metadata)
                .build();

        // Uploading to S3 destination bucket
        try {
            logger.info("Writing to: " + bucket + "/" + key);
            s3Client.putObject(putObjectRequest,
                    RequestBody.fromBytes(outputStream.toByteArray()));
        }
        catch(AwsServiceException e)
        {
            logger.info(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
