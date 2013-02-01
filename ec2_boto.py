from boto.ec2.connection import EC2Connection

from threading import Thread

import time, subprocess, os, shutil

import credentials

from colorama import init
from colorama import Fore, Back, Style

init()





def destroy_worker_nodes():
    conn = EC2Connection(credentials.EC2_ACCESS_ID, credentials.EC2_SECRET_KEY)
    while True:
        instances_waiting =0
        for reservation in conn.get_all_instances():
            for instance in reservation.instances:
                #print('looking at an instance with image id :' + instance.image_id + ' with state ' + instance.state)
                if instance.image_id == credentials.WORKER_AMI and instance.state != 'terminated':
                    instances_waiting += 1
                if instance.image_id == credentials.WORKER_AMI and instance.state == 'running':
                    #print('\tdeleting!')
                    instance.terminate()
        if instances_waiting == 0:
            return 'done'
        else:
            print("waiting for " + str(instances_waiting) + " old instance to terminate")
        time.sleep(1)



def create_node(conn, my_size):
    print('creating node...')
    reservation = conn.run_instances(credentials.WORKER_AMI,key_name=KEY_NAME,instance_type=SIZE,security_groups=[SECURITY_GROUP])
    instance = reservation.instances[0]
    # first wait for the instance to start running
    while instance.state != 'running':
        time.sleep(5)
        instance.update()
        print("instance state : " + instance.state)
    # next wait for the instance to start responding to ssh connections
    while run_command_on_instance('uname -a', instance.public_dns_name)[0] == '':
        print('still waiting for ssh to come up', run_command_on_instance('uname -a', instance.public_dns_name))
        time.sleep(5)
    return instance

def run_command_on_instance(command, dns):
    ssh_args = [
    'ssh',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i', KEY_FILE_PATH,
    'ubuntu@' + dns,
    command
    ]
    # print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

def copy_file_to_instance(path, dns, destination):
    ssh_args = [
    'scp',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i',KEY_FILE_PATH,
    path,
    'ubuntu@' + dns + ':~/' + destination,
    ]
    #print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

def copy_file_from_instance(path, dns, destination):
    ssh_args = [
    'scp',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i',KEY_FILE_PATH,
    'ubuntu@' + dns + ':~/' + path,
    destination,
    ]
    # print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

class MyThread(Thread):
    def run(self):
        print(Fore.GREEN + self.name + ' : running' + Fore.RESET)
        conn = EC2Connection(credentials.EC2_ACCESS_ID, credentials.EC2_SECRET_KEY)
        node_name = 'worker_' + self.name
        instance = create_node(conn, node_name)
        instance.add_tag('job', JOB)

        print(self.name + ' : instance domain name is ' + instance.public_dns_name)
        #time.sleep(30)
        self.start_time = time.time()
        print(self.name + ':' + str(copy_file_to_instance(self.input_file_name, instance.public_dns_name, 'input.dna')))
        print(self.name + ':' + str(run_command_on_instance('cp /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties.' + str(self.processors) +  ' /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties', instance.public_dns_name)))
        print(self.name + ':' + str(run_command_on_instance('/home/ubuntu/iprscan/interproscan-5-RC1/interproscan.sh -appl ProDom-2006.1,PfamA-26.0,TIGRFAM-10.1,SMART-6.2,Gene3d-3.3.0,Coils-2.2,Phobius-1.01 -i /home/ubuntu/input.dna -t n', instance.public_dns_name)))
        #print(self.name + ':' + str(run_command_on_instance('/home/ubuntu/iprscan/interproscan-5-RC1/interproscan.sh -appl PfamA-26.0,Coils-2.2,Phobius-1.01 -i /home/ubuntu/input.dna -t n', instance.public_dns_name)))
        print(self.name + ':' + str(copy_file_from_instance('input.dna.gff3', instance.public_dns_name, self.input_file_name + '.out')))
        print(self.name + ' : destroying node')
    #    instance.terminate()
        seconds = time.time() - self.start_time
        print(Fore.MAGENTA + self.name + ' : completed in ' + str(int(seconds)) + Fore.RESET)

def split_fasta(filename, number):
    try:
        f = open(filename)
    except IOError:
        print("The file, %s, does not exist" % filename)
        return

    sequences = {}

    for line in f:
        if line.startswith('>'):
            name = line[1:].rstrip('\n')
            sequences[name] = ''
        else:
            sequences[name] += line.rstrip('\n').rstrip('*')

    print("%d sequences found" % len(sequences))

    files = []
    filenames = []
    for x in range(0,number):
        files.append(open(filename + '_' + str(x), 'wt'))
        filenames.append(filename + '_' + str(x))

    i =0
    for name, seq in sequences.items():
        i = i+1
        files[i % number].write('>' + name + '\n' + seq + '\n')

    for f in files:
        f.close()

    return filenames



SIZE = 'm1.medium'
SECURITY_GROUP = 'quick-start-1'
JOB = 'lab_meeting_demo'
number = 8
processors = 1
input_file = '500_seqs.fasta'
KEY_FILE_PATH = 'first_instance.pem'
KEY_NAME = 'first_instance'



print('destroying old nodes...')
destroy_worker_nodes()
print('done destroying old nodes')

#make a new temp directory, copy the input file into it, and chdir into it
temp_dir_name =str(int(time.time()))
os.mkdir(temp_dir_name)
shutil.copy(input_file, temp_dir_name)
shutil.copy(KEY_FILE_PATH, temp_dir_name)
os.chdir(temp_dir_name)
filenames = split_fasta(input_file, number)

for i in range(number):
    print('starting thread ' + str(i))
    time.sleep(2)
    my_thread = MyThread()
    my_thread.name = i
    my_thread.input_file_name = filenames[i]
    my_thread.jobname = JOB
    my_thread.processors = processors
    my_thread.start()

# # print_status()
# destroy_worker_nodes()


