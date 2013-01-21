from boto.ec2.connection import EC2Connection

from threading import Thread

import time, subprocess

import credentials

from colorama import init
from colorama import Fore, Back, Style

init()


def print_status(conn):
    nodes = conn.list_nodes()

    for status in ['stopped', 'running', 'pending', 'build']:
        selected_nodes = [i for i in nodes if i.extra['status'] == status]
        if len(selected_nodes) > 0:
            print(str(len(selected_nodes)) + ' ' + status + ' nodes:')
            for node in selected_nodes:
                print("\t" + str(node.extra['instanceId']))
                # print("\t" + str(node.name))


def get_live_nodes(conn):
    return [n for n in conn.list_nodes() if n.state == NodeState.RUNNING]

def destroy_nodes():
    for node in [n for n in conn.list_nodes() if n.state == NodeState.RUNNING]:
        print('destroying node ' + node.uuid)
        node.destroy()

def destroy_worker_nodes():
    conn = EC2Connection(credentials.EC2_ACCESS_ID, credentials.EC2_SECRET_KEY)
    for reservation in conn.get_all_instances():
        for instance in reservation.instances:
            print('looking at an instance with image id :' + instance.image_id + ' with state ' + instance.state)
            if instance.image_id == credentials.WORKER_AMI and instance.state == 'running':
                print('\tdeleting!')
                instance.terminate()


def get_node(conn, id):
    nodes = [i for i in conn.list_nodes() if i.uuid == id]
    if len(nodes) > 0:
        return nodes[0]
    else:
        return None

def create_node(conn, my_size):
    print('creating node...')
    reservation = conn.run_instances(credentials.WORKER_AMI,key_name='first_instance',instance_type=SIZE,security_groups=[SECURITY_GROUP])
    instance = reservation.instances[0]
    while instance.state != 'running':
        time.sleep(1)
        instance.update()
        print("instance state : " + instance.state)
    return instance

def run_command_on_ip(command, ip):
    ssh_args = [
    'ssh',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i', 'first_instance.pem',
    'ubuntu@' + ip,
    command
    ]
    # print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

def copy_file_to_ip(path, ip, destination):
    ssh_args = [
    'scp',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i', 'first_instance.pem',
    path,
    'ubuntu@' + ip + ':~/' + destination,
    ]
    #print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

def copy_file_from_ip(path, ip, destination):
    ssh_args = [
    'scp',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i', 'first_instance.pem',
    'ubuntu@' + ip + ':~/' + path,
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
        self.start_time = time.time()
        n = create_node(conn, node_name)
        conn.ex_create_tags(n, {'job' : self.jobname})
        ip = get_node(conn, n.uuid).private_ip[0]
        # print('ip is ' + ip + " for thread " + self.name)
        time.sleep(30)

        print(self.name + ':' + str(run_command_on_ip('uname -a', ip)))
        print(self.name + ':' + str(copy_file_to_ip(self.input_file_name, ip, 'input.dna')))
        print(self.name + ':' + str(run_command_on_ip('cp /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties.' + str(self.processors) +  ' /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties', ip)))
        print(self.name + ':' + str(run_command_on_ip('/home/ubuntu/iprscan/interproscan-5-RC1/interproscan.sh -appl ProDom-2006.1,PfamA-26.0,TIGRFAM-10.1,SMART-6.2,Gene3d-3.3.0,Coils-2.2,Phobius-1.01 -i /home/ubuntu/input.dna -t n', ip)))
        print(self.name + ':' + str(copy_file_from_ip('input.dna.gff3', ip, self.input_file_name + '.out')))
        print(self.name + ' : destroying node')
        get_node(conn, n.uuid).destroy()

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



SIZE = 'c1.xlarge'
SECURITY_GROUP = 'quick-start-1'
job = 'lab_meeting_demo'
number = 1
processors = 8
input_file = 'input.fasta'





print('destroying old nodes...')
destroy_worker_nodes()
print('done destroying old nodes')

#filenames = split_fasta(input_file, number)
#
#for i in range(number):
#    print('starting thread ' + str(i))
#    time.sleep(2)
#    my_thread = MyThread()
#    my_thread.name = i
#    my_thread.input_file_name = filenames[i]
#    my_thread.jobname = job
#    my_thread.processors = processors
#    my_thread.start()

# # print_status()
# destroy_worker_nodes()


