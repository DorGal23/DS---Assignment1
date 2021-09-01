package Amazon;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

public class S3 {

    private Region region = Region.US_EAST_1;
    private final S3Client s3;
    private final String bucketName;

    public S3(String bucketName) {
        s3 = S3Client.builder()
                .region(region)
                .build();
        this.bucketName = bucketName;
        if (!bucketExist(bucketName)) {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println("Created Bucket: "+bucketName);
        }
    }

    private boolean bucketExist(String bucketName) {
        List<Bucket> buckets = s3.listBuckets().buckets();
        for (Bucket bucket : buckets)
            if (bucket.name().equals(bucketName)) {
                System.out.println("Bucket Connected: "+bucketName);
                return true;
            }
        return false;
    }

    public void uploadFile(String source, String key) {
        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
        } catch (NoSuchKeyException e){
            s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    RequestBody.fromFile(new File(source)));
            System.out.println("Uploaded: "+key);
        }
    }

    public void downloadFile(String source, String key) {
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(source)
                        .build(),
                ResponseTransformer.toFile(Paths.get(key)));
        System.out.println("Downloaded: "+source);
    }

    public boolean deleteFile(String file) {
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(file)
                .build());
        return true;
    }

    public void deleteBucket() {
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Iterable listRes = s3.listObjectsV2Paginator(listReq);
        for (S3Object content : listRes.contents()) {
            deleteFile(content.key());
            System.out.println("Deleted: "+content.key());
        }
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName)
                .build());
        System.out.println("Deleted: "+bucketName);
    }

    public String getBucketName() {
        return this.bucketName;
    }
}
