resource "aws_instance" "streaming-ec2"{
    ami = "ami-0a59248a6294cece2"
    instance_type = "t2.small"
    subnet_id = "subnet-06914b9bd660f9ccd"

    tags = {
    Name = "Kafka-Streaming"
  }
}

resource "aws_key_pair" "deployer" {
  key_name   = "deployer-key"
  public_key = file("~/.ssh/id_rsa.pub") # Path to your local public key
}