resource "aws_vpc" "retailitics-network" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "retailitics-network" {
  vpc_id                  = aws_vpc.retailitics-network.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "ap-southeast-2a"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "retailitics-network" {
  vpc_id = aws_vpc.retailitics-network.id
}

resource "aws_route_table" "retailitics-network" {
  vpc_id = aws_vpc.retailitics-network.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.retailitics-network.id
  }
}

resource "aws_route_table_association" "retailitics-network" {
  subnet_id      = aws_subnet.retailitics-network.id
  route_table_id = aws_route_table.retailitics-network.id
}

resource "aws_security_group" "retailitics-network" {
  name   = "retailitics-network-sg"
  vpc_id = aws_vpc.retailitics-network.id

}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

resource "aws_vpc_security_group_ingress_rule" "retailitics-network" {
  security_group_id = aws_security_group.retailitics-network.id
  cidr_ipv4         = "${chomp(data.http.my_ip.response_body)}/32"
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "retailitics-network" {
  security_group_id = aws_security_group.retailitics-network.id
  cidr_ipv4   = "0.0.0.0/0"
  ip_protocol = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "kafka" {
  security_group_id = aws_security_group.retailitics-network.id
  cidr_ipv4         = "${chomp(data.http.my_ip.response_body)}/32"
  from_port         = 9092
  to_port           = 9092
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "streamlit" {
  security_group_id = aws_security_group.retailitics-network.id
  cidr_ipv4         = "${chomp(data.http.my_ip.response_body)}/32"
  from_port         = 8501
  to_port           = 8501
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "airflow" {
  security_group_id = aws_security_group.retailitics-network.id
  cidr_ipv4         = "${chomp(data.http.my_ip.response_body)}/32"
  from_port         = 8080
  to_port           = 8080
  ip_protocol       = "tcp"
}