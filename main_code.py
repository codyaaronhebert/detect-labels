import boto3

def update_s3_metadata(bucket, key, metadata):
    s3 = boto3.client('s3')

    # Copy the object to itself with new metadata
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=key,
        Metadata=metadata,
        MetadataDirective='REPLACE'
    )

def detect_labels(bucket, image):
    client = boto3.client('rekognition')
    response = client.detect_labels(
        Image={'S3Object': {'Bucket': bucket, 'Name': image}},
        MaxLabels=10,
        MinConfidence=75
    )

    labels = [label['Name'] for label in response['Labels']]
    metadata = {'x-amz-meta-rekognition-labels': ','.join(labels)}
    update_s3_metadata(bucket, image, metadata)

    print(f'Detected labels in {image}')
    for label in response['Labels']:
        print(f"Label: {label['Name']}\nConfidence: {label['Confidence']}\n----------")

    return len(labels)

# Usage
bucket_name = "icloud-replacement-test"
image_name = "IMG_2358.jpeg"
num_labels = detect_labels(bucket_name, image_name)
print(f"Total labels detected: {num_labels}")
