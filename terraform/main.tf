provider "aws" {
  region = "us-east-1"
}

resource "aws_db_instance" "example" {
  identifier        = "awardsdbinstance"
  instance_class    = "db.t2.micro"
  allocated_storage = 20
  engine            = "postgres"
  engine_version    = "13.3"
  name              = "nsf_awards_db"
  username          = "mymasteruser"
  password          = "mypassword"
  publicly_accessible = true
  skip_final_snapshot = true

  vpc_security_group_ids = ["sg-xxxxxxxx"]  # Replace with your security group ID

  tags = {
    Name = "AwardsDB"
  }
}

output "rds_endpoint" {
  value = aws_db_instance.example.endpoint
}
