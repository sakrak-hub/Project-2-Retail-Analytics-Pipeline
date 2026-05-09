resource "aws_instance" "streaming-ec2" {
  ami                    = "ami-0a59248a6294cece2"
  instance_type          = "t3.small"
  subnet_id              = aws_subnet.retailitics-network.id
  vpc_security_group_ids = [aws_security_group.retailitics-network.id]
  key_name               = aws_key_pair.generated_key.key_name
  iam_instance_profile   = aws_iam_instance_profile.kafka_ec2_profile.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "Kafka-Streaming"
  }
}

resource "tls_private_key" "kafka_stream_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "generated_key" {
  key_name   = "my-terraform-key"
  public_key = tls_private_key.kafka_stream_key.public_key_openssh
}

resource "local_sensitive_file" "private_key" {
  content         = tls_private_key.kafka_stream_key.private_key_pem
  filename        = "/home/sakra_k/.ssh/my-terraform-key.pem"
  file_permission = "0400"
}