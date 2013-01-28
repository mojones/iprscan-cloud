iprscan-cloud
=============

tools for running distributed interproscan and blast searches on openstack-compatible cloud providers

Instructions
============

Sign up for an Amazon ec2 account. Go to "My account" -> "Security credentials" and take a note of your Access Key ID and Secre Access key. 

Using the ec2 website, create an ssh key pair and take a note of the name. Download the key file to the machine on which you're going to run the script.

Using the ec2 website, create a security group that allows ssh access.

Copy the file `credentials.example.py` to `credentials.py` and fill in the Access Key ID and Secret Access Key.

You will need to install boto : https://github.com/boto/boto

Edit the ec2_boto Python script and replace the following variables:

SIZE - the type of instance you want to run. 
SECURITY_GROUP - the name of the security group that you created
JOB - the name of the job for the analysis you want to run - this is just for accounting and lets you tag your instances
number - the number of instances you want to run. By default, new accounts are only allowed to launch 20 instances, so setting this to more than 20 will probably not work
processors - the number of processors to use on each instance. Since the whole purpose of this exercise is to speed up iprscan searches, you should probably set this to the number of processors on the instance size that you have selected.
input_file - path to the file containing your input sequences
KEY_FILE_PATH - path to the ssh key file
KEY_FILE_NAME - the name of the key file that you created. 

Run the script with `python ec2_boto`.
