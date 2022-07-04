import boto3

s3 = boto3.resource('s3',
                    aws_access_key_id="AKIATBBQH4E7FFCM5OWH",
                    aws_secret_access_key="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")

my_bucket = s3.Bucket('invisible-bike')

files = []

for object_summary in my_bucket.objects.filter(Prefix="precipitation_data/"):
    files.append(object_summary.key)

files.pop(0)

for file in files:
    temp = file.split("_", 3)
    filename = file.split("/", 1)[1]

    if temp[3] == 'wheather.json':
        s3.Object("invisible-bike", f"temp/{ temp[1].split('/')[1] }_{ temp[2] }_precipitation.json").copy_from(CopySource=f"invisible-bike/precipitation_data/{ filename }")
        #s3_resource.Object("invisible-bike", f"precipitation_data/{ filename }").delete()
    else:
        s3.Object("invisible-bike", f"temp/{ filename }").copy_from(CopySource=f"invisible-bike/precipitation_data/{ filename }")
        #s3_resource.Object("invisible-bike", f"precipitation_data/{ filename }").delete()