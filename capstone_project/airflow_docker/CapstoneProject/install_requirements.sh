#!/bin/bash -xe

echo "Installing Non-standard and non-Amazon Machine Image Python modules"
sudo python3 -m pip install configparser    


echo "Creating Directories"
mkdir -p /mnt1/CapstoneProject/tmp   


echo "Copying files from s3"
aws s3 cp s3://sparkprojectcode/scripts.zip /mnt1/CapstoneProject
aws s3 cp s3://sparkprojectcode/conf.zip /mnt1/CapstoneProject
aws s3 cp s3://sparkprojectcode/main.py /mnt1/CapstoneProject


echo "Setting up the project space"
unzip /mnt1/CapstoneProject/scripts.zip -d /mnt1/CapstoneProject/
unzip /mnt1/CapstoneProject/conf.zip -d /mnt1/CapstoneProject/
chmod -R 777 /mnt1/CapstoneProject/*


echo "Cleaning up zip files"
rm /mnt1/CapstoneProject/scripts.zip
rm /mnt1/CapstoneProject/conf.zip
