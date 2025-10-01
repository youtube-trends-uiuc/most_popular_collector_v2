# This is the code that goes into the AWS Lambda function.

import boto3

INIT_SCRIPT = """#!/bin/bash
cd /home/ubuntu
su ubuntu -c 'mkdir .aws'
su ubuntu -c 'printf "[default]\\nregion=us-west-2" > /home/ubuntu/.aws/config'
su ubuntu -c 'wget https://raw.githubusercontent.com/youtube-trends-uiuc/most_popular_collector_v2/refs/heads/main/init_script.sh'
su ubuntu -c 'chmod +x /home/ubuntu/init_script.sh'
su ubuntu -c 'sudo apt-get update -y'
su ubuntu -c 'sudo apt-get install -y screen'
su ubuntu -c "screen -dmS youtube_trends sh -c '/home/ubuntu/init_script.sh 2>&1 | tee output.txt; exec bash'"
"""

def lambda_handler(event, context):
    ec2 = boto3.resource('ec2')
    ec2.create_instances(
        ImageId='ami-0836fd4a4a0b4f6ec',
        InstanceType='t4g.nano',
        MinCount=1,
        MaxCount=1,
        KeyName='Dec9-2019-key',
        InstanceInitiatedShutdownBehavior='terminate',
        UserData=INIT_SCRIPT,
        SecurityGroupIds=['sg-0b6d936cbbaab78f2',],
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'DeleteOnTermination': True,
                    # The minimum volume size is 6Gb, approximately
                    'VolumeSize': 10
                }
            },
        ],
        TagSpecifications=[{'ResourceType': 'instance',
                            'Tags': [{"Key": "Name", "Value": 'youtube_trends'}]}],
        IamInstanceProfile={'Name': 'youtube_trends'}
    )
    return {
        "statusCode": 200,
        "message": "Instance created!"
    }


if __name__ == '__main__':
    lambda_handler(None, None)