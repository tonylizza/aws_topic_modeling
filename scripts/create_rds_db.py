import boto3
import time

# Create an RDS client
rds_client = boto3.client('rds', region_name='us-west-2')

# Parameters for the RDS instance
db_instance_identifier = 'mydbinstance'
db_instance_class = 'db.t2.micro'
engine = 'postgres'
master_username = 'mymasteruser'
master_user_password = 'mypassword'
allocated_storage = 20
db_name = 'nsf_awards_db'
vpc_security_group_ids = ['sg-xxxxxxxx']  # Replace with your security group ID

# Create the RDS instance
try:
    response = rds_client.create_db_instance(
        DBInstanceIdentifier=db_instance_identifier,
        AllocatedStorage=allocated_storage,
        DBInstanceClass=db_instance_class,
        Engine=engine,
        MasterUsername=master_username,
        MasterUserPassword=master_user_password,
        DBName=db_name,
        VpcSecurityGroupIds=vpc_security_group_ids,
        PubliclyAccessible=True,
        StorageType='gp2'
    )

    print(f"Creating RDS instance {db_instance_identifier}...")

    # Wait for the RDS instance to be available
    waiter = rds_client.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=db_instance_identifier)
    print(f"RDS instance {db_instance_identifier} is now available!")

    # Retrieve the endpoint address
    response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
    endpoint = response['DBInstances'][0]['Endpoint']['Address']
    print(f"RDS instance endpoint: {endpoint}")

except Exception as e:
    print(f"Error creating RDS instance: {e}")
