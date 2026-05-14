output "instance_public_ip" {
  value = aws_instance.streaming-ec2.public_ip
  description = "The public IP of the web server"
}

output "instance_id" {
  description = "The ID of the EC2 instance"
  value       = aws_instance.streaming-ec2.id
}