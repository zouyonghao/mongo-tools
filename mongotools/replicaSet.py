import os
import sys

import paramiko
import yaml

# load configuration.
from pymongo import MongoClient

f = open('mongo_replica_set.yaml')
replica_set_configuration = yaml.safe_load(f)
f.close()
print('WARN: Your replicaSet configuration file is ', file=sys.stderr)
print(yaml.dump(replica_set_configuration))
print()
# yesOrNo = input('Are you sure about this configuration? [Y/N]').lower()
# if yesOrNo != 'y' or yesOrNo != 'yes':
#     exit()

f = open('mongo_replica_set_deploy.yaml')
deploy_map = yaml.safe_load(f)
f.close()
print('Your configured replicaSet is ')
print(yaml.dump(deploy_map['replicaSet']))


def upload(ip, port, username, password, local_path, remote_path):
    # upload mongo file to linux.
    t = paramiko.Transport((ip, port))
    print('start connect to ' + ip + ':' + str(port))
    t.connect(username=username, password=password)
    print('success')
    sftp = paramiko.SFTPClient.from_transport(t)
    print(os.path.isfile(local_path))
    print("Starting upload " + local_path)
    sftp.put(local_path, remote_path)
    print("Finished upload.")
    t.close()


def main():
    replica_set_configuration_for_init = {}
    replica_set_configuration_for_init['_id'] = replica_set_configuration['replication']['replSetName']
    replica_set_configuration_for_init['members'] = []
    firstServer = True
    first_ip = ''
    first_port = 0
    id = 0
    for server in deploy_map['replicaSet']:
        ip = server['ip']
        port = server['port']
        username = server['username']
        password = server['password']
        remote_path = deploy_map['remotePath']
        local_path = deploy_map['localPath']
        # upload
        upload(ip, port, username, password, local_path, remote_path)
        # ssh
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, port, username, password)
        exec_pwd(ssh)
        init_mongo_env(ssh, remote_path)

        # upload configuration file.
        path = exec_pwd(ssh)
        remote_mongo_conf_file = path + "/mongo_replica_set.yaml"
        upload(ip, port, username, password, './mongo_replica_set.yaml',
               remote_mongo_conf_file)

        log_path = replica_set_configuration['systemLog']['path']
        log_path = log_path[:log_path.rfind('/')]
        print('creating logPath ' + log_path)
        ssh.exec_command('mkdir -p ' + log_path)

        db_path = replica_set_configuration['storage']['dbPath']
        print('creating dbPath ' + db_path)
        ssh.exec_command('mkdir -p ' + db_path)

        mongoPort = server['mongoPort']

        print('starting mongo on ' + ip + ':' + str(mongoPort))
        stdin, stdout, stderr = ssh.exec_command("mongod --config " + remote_mongo_conf_file +
                                                 ">/dev/null 2>&1 &")
        print_stdout_stderr(stdout, stderr)

        replica_set_configuration_for_init['members'].append({
            '_id': id,
            'host': ip + ":" + str(mongoPort)
        })
        id += 1
        if firstServer:
            first_ip = ip
            first_port = mongoPort
        firstServer = False

    # init the master and add slaves to it.
    client = MongoClient(first_ip, first_port)
    client.admin.command('replSetInitiate', replica_set_configuration_for_init)
    exit()


def init_mongo_env(ssh, remote_path):
    # 下面几个并没有什么用
    # print('entering to the remote upload path...')
    # ssh.exec_command('cd ' + remote_path)
    # path = exec_pwd(ssh)
    # if not remote_path.startswith(path[:-1]):
    #     print('entering to the remote_path fail')
    #     exit()
    # print('current path is ' + path)
    # extract mongo files.
    print('try extract mongo files...')
    stdin, stdout, stderr = ssh.exec_command('tar zxvf ' + remote_path)
    for file_name in stdout.readlines():
        print(file_name[:-1])
    stdin, stdout, stderr = ssh.exec_command('ls')
    for dir in stdout.readlines():
        if dir.startswith('mongodb-linux-'):
            mongodir = dir
            break
    print()
    mongodir = mongodir[:-1]
    print('Your mongodb folder is ' + mongodir)
    path = exec_pwd(ssh)
    mongo_path = path + "/" + mongodir
    print(mongo_path)
    cmd = 'mv ' + mongo_path + ' /usr/local/'
    print(cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    print_stdout_stderr(stderr, stdout)
    ssh.exec_command('ln -s /usr/local/' + mongodir + ' /usr/local/mongo')
    print_stdout_stderr(stderr, stdout)
    ssh.exec_command('ln -s /usr/local/mongo/bin/* /bin/')
    print_stdout_stderr(stderr, stdout)


def print_stdout_stderr(stderr, stdout):
    for msg in stdout.readlines():
        print(msg)
    for msg in stderr.readlines():
        print(msg)


def exec_pwd(ssh):
    path = exec_command_and_return_first_outline(ssh, 'pwd')
    print('you are in path ' + path)
    return path


def exec_command_and_return_first_outline(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)
    print('executing ' + command + ' ...')
    out = stdout.readlines()[0]
    return out[:-1]


if __name__ == '__main__':
    main()
