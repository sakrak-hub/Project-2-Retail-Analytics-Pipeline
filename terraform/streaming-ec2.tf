resource "aws_instance" "streaming-ec2"{
    ami = "ami-0a59248a6294cece2"
    instance_type = "t3.small"
    subnet_id              = aws_subnet.retailitics-network.id
    vpc_security_group_ids = [aws_security_group.retailitics-network.id]
    key_name               = aws_key_pair.deployer.key_name
    iam_instance_profile = aws_iam_instance_profile.ec2_profile.name

    root_block_device {
      volume_size = 30        
      volume_type = "gp3"    
      encrypted   = true
    }

    tags = {
    Name = "Kafka-Streaming"
  }
}

resource "aws_key_pair" "deployer" {
  key_name   = "deployer-key"
  public_key = file("~/.ssh/kafka-stream.pub")
}