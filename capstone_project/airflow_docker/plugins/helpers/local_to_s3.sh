#!/bin/bash

Copy_to_s3(){
	echo "Creating a Zip file of source code to be uploaded to S3"
	zip -r CapstoneProject.zip ../CapstoneProject/*

	echo "Copying the zip file to s3"
	aws s3 cp ./CapstoneProject.zip s3://sparkprojectcode/

	status=$?
	
	if [ $status != 0 ]; 
	then
		echo "Unable to copy to s3"
		exit $status
	else
		echo "cleaning up local zip file "
		rm CapstoneProject.zip
	fi
}


echo "Executing bash script"
Copy_to_s3
