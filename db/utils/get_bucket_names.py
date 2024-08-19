"""these functions will get the bucket list and return the bucket, and check whether the bucket is empty or not"""
import boto3


def get_data_bucket_name(bucket_prefix: str=""):
    """getting the data bucket name"""
    s3 = boto3.client('s3', region_name='eu-west-2')
    
    response = s3.list_buckets()
    
    try:
        if not response['Buckets']:
            raise ValueError("No buckets found in S3")
    
        for bucket in response['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
        raise ValueError(f"No bucket found with prefix: {bucket_prefix}")
    except ValueError as e:
        print(f"An error occurred while accessing S3: {e}")
        raise

def keys(bucket_name, prefix='/', delimiter='/'):

    """keys"""
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

def check_bucket_has_files(bkt_name):
    """checking whether the bucket is empty"""
    
    try:
        if next(keys(bkt_name), None):
            print("Bucket is NOT empty")
            return True
        return False
    except Exception as e:
        print(f"broken{e}")

if __name__ == '__main__':
    bn = get_data_bucket_name()
    print(check_bucket_has_files(bn))