import subprocess

result = subprocess.run("terraform output -raw instance_public_ip", capture_output=True, text=True, shell=True)
iplist = result.stdout.split(".")

config_file_content = f"""
  Host kafka-streaming-ec2
    HostName ec2-{iplist[0]}-{iplist[1]}-{iplist[2]}-{iplist[3]}.ap-southeast-2.compute.amazonaws.com
    IdentityFile D:\\Projects\\Project-2-Retail-Analytics-Pipeline\\terraform\\.ssh\\my-terraform-key.pem
    User ubuntu
"""

print(config_file_content)