from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.types import NodeState

from threading import Thread

import time, subprocess

import credentials 

# SIZE = 'c1.xlarge'
SIZE = 't1.micro'



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

def destroy_nodes(conn):
    for node in [n for n in conn.list_nodes() if n.state == NodeState.RUNNING]:
        print('destroying node ' + node.uuid)
        node.destroy()

def destroy_worker_nodes(conn):
    for node in [n for n in conn.list_nodes() if n.state == NodeState.RUNNING and 'worker' in n.name]:
        print('destroying node ' + node.uuid)
        node.destroy()

def get_node(conn, id):
    nodes = [i for i in conn.list_nodes() if i.uuid == id]
    if len(nodes) > 0:
        return nodes[0]
    else:
        return None

def create_node(conn, new_node_name, my_image, my_size):
    print('creating node...')
    n = conn.create_node(name=str(new_node_name), image=my_image, size=my_size, ex_keyname='first_instance')
    print('...done creating node:')
    print(repr(get_node(conn, n.uuid)))
    while get_node(conn, n.uuid) == None or get_node(conn, n.uuid).state != 0:
	    print('waiting for ' + n.uuid + " to build")
	    # print(repr(get_node(n.uuid)))
	    time.sleep(5)
    return n



def run_command_on_ip(command, ip):
    ssh_args = [
    'ssh',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o','StrictHostKeyChecking=no',
    '-i', 'first_instance.pem', 
    'ubuntu@' + ip, 
    command
    ]
    print(' '.join(ssh_args))
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
    print(' '.join(ssh_args))
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
    print(' '.join(ssh_args))
    proc = subprocess.Popen(ssh_args, stdout=subprocess.PIPE)
    result = proc.communicate()
    return result

class MyThread(Thread):
    def run(self):
        Driver = get_driver(Provider.EC2)
        conn = Driver(credentials.EC2_ACCESS_ID, credentials.EC2_SECRET_KEY)

        my_image = [i for i in conn.list_images() if i.id == credentials.WORKER_AMI][0]
        my_size = [i for i in conn.list_sizes() if i.id == SIZE][0]

        node_name = 'worker_' + self.name
        n = create_node(conn, node_name, my_image, my_size)
        ip = get_node(conn, n.uuid).private_ip[0]
        print('ip is ' + ip + " for thread " + self.name)
        time.sleep(30)

        print(self.name + ':' + str(run_command_on_ip('uname -a', ip)))
        print(self.name + ':' + str(copy_file_to_ip('big.dna', ip, 'input.dna')))
        print(self.name + ':' + str(run_command_on_ip('cp /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties.8 /home/ubuntu/iprscan/interproscan-5-RC1/interproscan.properties', ip)))
        # print(self.name + ':' + str(run_command_on_ip('/home/ubuntu/iprscan/interproscan-5-RC1/interproscan.sh -i /home/ubuntu/input.dna -t n', ip)))
        # print(self.name + ':' + str(copy_file_from_ip('input.dna.gff3', ip, 'input.dna.GFF.'+self.name)))
        print(self.name + ' : destroying node')
        get_node(conn, n.uuid).destroy()


main_Driver = get_driver(Provider.EC2)
main_conn = main_Driver(credentials.EC2_ACCESS_ID, credentials.EC2_SECRET_KEY)
print('destroying old nodes...')
destroy_worker_nodes(main_conn)
print('done destroying old nodes')

for i in range(2):
    print('starting thread ' + str(i))
    my_thread = MyThread()
    my_thread.name = i
    my_thread.start()

# # print_status()
# destroy_worker_nodes()
	