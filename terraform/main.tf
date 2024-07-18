provider "aws" {
  region = "us-east-1"
}

resource "aws_db_instance" "example" {
  identifier        = "awardsdbinstance"
  instance_class    = "db.t2.micro"
  allocated_storage = 20
  engine            = "postgres"
  engine_version    = "16"
  db_name              = "nsf_awards_db"
  username          = "awarddbuser"
  password          = "K@mi1saac"
  publicly_accessible = true
  skip_final_snapshot = true


  tags = {
    Name = "AwardsDB"
  }
}

output "rds_endpoint" {
  value = aws_db_instance.example.endpoint
}
