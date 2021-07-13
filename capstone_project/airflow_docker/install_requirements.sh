#!/bin/bash -xe

path=$1

echo "Installing Non-standard and non-Amazon Machine Image Python modules"
sudo python3 -m pip install configparser    


echo "Creating Directories"
mkdir -p /mnt1/CapstoneProject/tmp   


echo "Copying files from s3"
aws s3 cp s3://$path/CapstoneProject.tar.gz /mnt1


echo "Setting up the project space"
tar -xvzf /mnt1/CapstoneProject.tar.gz -C /mnt1
chmod -R 777 /mnt1/CapstoneProject/*