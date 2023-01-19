import subprocess  
import pathlib
import sys
import paramiko
import socket
import pathlib
import subprocess
import re
from scp import SCPClient
import pexpect

def execute(command, directory):
    
    result = {}
    
    if type(command).__name__ == 'str':
        command = [command]

    for cmd in command:
        out_dict = result[cmd] = {}
        print("Job started with command:", cmd)
        try:
            job = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd= directory, shell=True)
            output = job.communicate()
        except Exception:
            raise 
        else:
            print("returncode =", job.returncode)
    
            if job.returncode != 0:
                print("Error...")
                for line in output[1].decode(encoding='utf-8').split('\n'):
                    print(line)
            else:
                print("job done..")
                if output[0]:
                    print("Output...")
                    for line in output[0].decode(encoding='utf-8').split('\n'):
                        print(line)
            out_dict['returncode'] = job.returncode
            out_dict['output'] = output[0].decode(encoding='utf-8')
            out_dict['error'] = output[1].decode(encoding='utf-8')
    return result
    
class SubmitLocal:

    def __init__(self, task, nprocessors:int) -> None:
        self.task = task
        self.task_info = task.task_info
        self.project_dir = self.task.project_dir
        self.np = nprocessors
        self.command = None                   

    def run_job(self, cmd): 
        self.task_info.state.local = True   
        result = execute(cmd, self.project_dir)
        self.task_info.local.update({'returncode': result[cmd]['returncode'],
                                        'output' : result[cmd]['output'],
                                        'error':result[cmd]['error']})

class SubmitNetwork:

    def __init__(self,task,
                    hostname: str,
                    username: str,
                    password: str,
                    port: int,
                    remote_path: str,
                    pkey_file=pathlib.Path('/home/anandsahu/.ssh/id_rsa'), # add a variable to take pkey_file
                    ls_file_mgmt_mode=True,
                    passwordless_ssh=False) -> None: # add a boolean variable for passwordless_ssh 

        self.task = task
        self.task_info = task.task_info
        self.task_info.state.network = True
        self.project_dir = self.task.project_dir.parent 
        self.username = username
        self.hostname = hostname
        self.password = password
        self.pkey_file=pkey_file
        self.port = port
        self.remote_path = remote_path
        self.ls_file_mgmt_mode=ls_file_mgmt_mode
        self.passwordless_ssh=passwordless_ssh        
        self.network_sub = NetworkJobSubmission(hostname, self.port)
        
        if passwordless_ssh==True:
            self.network_sub.ssh_connect(username,pkey_file)
        else:
            self.network_sub.ssh_connect(username, password)
        
        if self.network_sub.check_file(self.remote_path):
            self.task.add_proper_path(self.remote_path)
            self.upload_files()        
        else:
            raise FileNotFoundError(f"Remote path: {self.remote_path} not found.")
        
    def upload_files(self):
        """uploads entire project directory to remote path"""

        include = ['*/','*.xyz', '*.sh', f'{self.task.NAME}/**']
        (error, message) = rsync_upload_files(ruser=self.username, rhost=self.hostname,port=self.port, password=self.password,
                                                source_dir=str(self.project_dir), dst_dir=str(self.remote_path),
                                                include=include, exclude='*')
        #self.network_sub.upload_files(str(self.project_dir), str(self.remote_path), recursive=True)
        if error != 0:
            raise Exception(message)

    def download_output_files(self):
        """Downloads entire project directory to local project dir."""
        remote_path = pathlib.Path(self.remote_path) / self.project_dir.name
        #self.network_sub.download_files(str(remote_path),str(self.project_dir.parent),  recursive=True)
        if (self.ls_file_mgmt_mode==False):
            (error, message) = rsync_download_files(ruser=self.username, rhost=self.hostname,port=self.port, password=self.password,
                                                source_dir=str(remote_path), dst_dir=str(self.project_dir.parent))
        
        elif (self.ls_file_mgmt_mode==True):
            (error, message)=download_files_from_remote(self.hostname,self.username,self.port,self.password,remote_path,self.project_dir)

        elif error != 0:
            raise Exception(message)

    def get_output_log(self):
        """Downloads engine log file for that particular task."""
        engine_log = pathlib.Path(self.task.task_info.output['txt_out'])
        rpath = pathlib.Path(self.remote_path) / engine_log.relative_to(self.project_dir.parent)
        self.network_sub.download_files(str(rpath), str(engine_log))


    def run_job(self, cmd):
        "This method creates the job submission command and executes the command on the cluster"
        remote_path = pathlib.Path(self.remote_path) / self.task.project_dir.relative_to(self.project_dir.parent)
        self.command = f"cd {str(remote_path)} && {cmd} {self.task.BASH_filename}"
                
        exit_status, ssh_output, ssh_error = self.network_sub.execute_command(self.command)
        if exit_status != 0:
            print("Error...")
            for line in ssh_error.decode(encoding='utf-8').split('\n'):
                print(line)
        else:
            print("Job submitted successfully!...")
            for line in ssh_output.decode(encoding='utf-8').split('\n'):
                print(line)
        
        self.task_info.network.update({'sub_returncode': exit_status,
                                            'output':ssh_output.decode(encoding='utf-8'),
                                            'error':ssh_error.decode(encoding='utf-8')})
    
    def check_job_status(self) -> bool:
        """returns true if the job is completed in remote machine"""
        rpath = pathlib.Path(self.remote_path) / self.task.network_done_file.relative_to(self.project_dir.parent)
        return self.network_sub.check_file(str(rpath))

class NetworkJobSubmission:
    """This class contain methods connect to remote cluster through ssh and perform common
    uploadig and downloading of files and also to execute command on the remote cluster."""
    def __init__(self,
                host,
                port,
                ls_file_mgmt_mode=True,
                passwordless_ssh=True):
        
        self.client = None
        self.host = host
        self.port = port
        self.ls_file_mgmt_mode=ls_file_mgmt_mode
        self.passwordless_ssh=passwordless_ssh
                 
    def ssh_connect(self, username, password=None, pkey_file=None):
        "connects to the cluster through ssh."
        try:
            print("Establishing ssh connection")
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if pkey_file:
                # private_key = paramiko.RSAKey.from_private_key(pkey)
                private_key = paramiko.RSAKey.from_private_key_file(pkey_file)
                
                self.client.connect(hostname=self.host, port=self.port, username=username, pkey=private_key)
                print("connected to the server", self.host)
            else:    
                self.client.connect(hostname=self.host, port=self.port, username=username, password=password)
                print("connected to the server", self.host)

        except paramiko.AuthenticationException:
            raise Exception("Authentication failed, please verfiy your credentials")
        except paramiko.SSHException as sshException:
            raise Exception(f"Could not establish SSH connection: {sshException} ")
        except socket.timeout as e:
            raise Exception("Connection timed out")
        except Exception as e:
     
           raise Exception(f"Exception in connecting to the server {e}")
    
    @property
    def scp(self):
        self._check_connection()
        return SCPClient(self.client.get_transport(), progress= self.progress)
    
    def disconnect(self):
        "closes the ssh session with the cluster."
        if self.client:
            self.client.close()
        if self.scp:
            self.scp.close()

    def _check_connection(self):
        "This checks whether ssh connection is active or not."
        transport = self.client.get_transport()
        if not transport.is_active() and transport.is_authenticated():
            raise Exception('Not connected to a cluster')

    def upload_files(self, local_file_path, remote_path, recursive=False):
        """This method uploads the file to remote server."""
       
        self._check_connection()
        
        try:
            self.scp.put(local_file_path, remote_path=remote_path, recursive=recursive)
        except Exception as e:
            print(e)
            raise Exception(f"Unable to upload the file to the remote server {remote_path}")

    def progress(self, filename, size, sent):
        sys.stdout.write(f"{filename}'s progress: {float(sent)/float(size)*100 : .2f}   \r") 

    def download_files(self, remote_file_path, local_path, recursive=False):
        "This method downloads the file from cluster"
        
        self._check_connection()
        try:
           self.scp.get( remote_file_path, local_path, recursive=recursive)
        except Exception as e:
            raise Exception(f"Unable to download the file from the remote server {remote_file_path}")

    def check_file(self, remote_file_path):
        "checks if the file exists in the remote path"
        self._check_connection()
        sftp = self.client.open_sftp()
        try:
           sftp.stat(remote_file_path)
        except FileNotFoundError:
            return False
        return True

    def execute_command(self, command):
        """Execute a command on the remote host.
        Return a tuple containing retruncode, stdout and stderr
        from the command."""
        
        self._check_connection()
        try:
            print(f"Executing command --> {command}")
            stdin, stdout, stderr = self.client.exec_command(command)
            ssh_output = stdout.read()
            ssh_error = stderr.read()
            exit_status = stdout.channel.recv_exit_status()

            try:
                if exit_status:
                    pass
            except Exception as e:
                exit_status = -1

            if ssh_error:
                print(ssh_error)
                #raise Exception(f"Problem occurred while running command: {command} The error is {ssh_error}")
        except socket.timeout as e:
            exit_status = -1
            pass
            #raise Exception("Command timed out.", command)
        except paramiko.SSHException:
            raise Exception("Failed to execute the command!", command)

        return exit_status, ssh_output, ssh_error


def rsync_upload_files(ruser, rhost,port, password, source_dir, dst_dir, include=None, exclude= None):

    return rsync_cmd(ruser, rhost, port, password, source_dir, dst_dir, include=include, exclude=exclude)

def rsync_download_files(ruser, rhost, port, password, source_dir, dst_dir, include=None, exclude=None):
    
    return rsync_cmd(ruser, rhost, port, password, source_dir, dst_dir, include=include, exclude=exclude,upload=False)

def rsync_cmd(ruser, rhost, port, password, source_dir, dst_dir,include=None, exclude=None, upload=True):
    
    cmd = []
    cmd.append(f'''rsync -av -e "ssh -p {port}"''') 

    def append(name, option):
        if type(option) == str:
            option = [option]
        cmd.append(((f"--{name}=" + "'{}' ") * len(option)).format(*option))

    if bool(include) and bool(exclude):
        for name, option in [('include', include), ('exclude', exclude)]:
            append(name, option)
   
    if upload:
        cmd.append(f"{source_dir} {ruser}@{rhost}:{dst_dir}")   
    else:
        cmd.append(f"{ruser}@{rhost}:{source_dir} {dst_dir}")
    
    cmd = ' '.join(cmd)
    (error, message) = execute_rsync(cmd, passwd=password)
    
    return (error, message)

def execute_rsync(cmd,passwd, timeout=None):
    intitial_response = ['Are you sure', 'assword','[#\$] ', pexpect.EOF]
    
    ssh = pexpect.spawn(cmd,timeout=timeout)
    i = ssh.expect(intitial_response)
    if i == 0 :
        T = ssh.read(100)
        ssh.sendline('yes')
        ssh.expect('assword:')
        ssh.sendline(passwd)
    elif i == 1:
        ssh.sendline(passwd)
    elif i==2:
        print('Connected Successfully.')
        prompt = ssh.after
        print('Shell Command Prompt:', prompt.decode(encoding='utf-8'))    
    else:
        str1 = str(ssh.before)
        return (-3, 'Error: Unknown:'+ str1)

    possible_response = ['assword:', pexpect.EOF]
    i = ssh.expect(possible_response, timeout=5)

    if i == 0:
        return (-4, "Error: Incorrect password.")
    else:
        output = str(ssh.before)
        for text in ssh.before.decode(encoding='utf-8').split('\n'):
            print(text)
        return (0, output)


# lfm_file_info=get_from_task_info  or config_file # add a variable for file_tag_info

lfm_file_info={ 
                '.out':{'file_relevance':'very_impt','file_lifetime':'None', 'transfer_method':{'method':'compress_transfer','compress_method':'zstd','split_size':'500k'}},
                '.log':{'file_relevance':'very_impt','file_lifetime':'','transfer_method':{'method':'direct_transfer','compress_method':'zstd','split_size':''}},
                '.cube':{'file_relevance':'very_impt','file_lifetime':'','transfer_method':{'method':'compress_transfer','compress_method':'zstd','split_size':''}},
                '.ulm':{'file_relevance':'very_impt','file_lifetime':'','transfer_method':{'method':'split_transfer','compress_method':'zstd','split_size':'200M'}},

                 }
        
def download_files_from_remote(host,username,port,passwd,remote_proj_dir,local_proj_dir):   
    """
   1. get the list of files from remote directory
   2. convert the list of files into file-metadata-dictionary 
   3. filter the dictionary into sub-dictionary of priority-levels
   3. give each file priority level according to the assigned tags of files 
   4. classify the files on the basis of priority and transfer priority1 files first
   4. determine the method to transfer the files
   5. transfer the file
    """
    print("\nlitesoph file management activated !!")

    cmd_create_listOfFiles_at_remote=f'ssh -p {port} {username}@{host} "cd {remote_proj_dir}; find "$PWD"  -type f > listOfFiles.list"'     
    cmd_listOfFiles_to_local=f"rsync --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/listOfFiles.list {local_proj_dir}"
    (error, message)=execute_rsync(cmd_create_listOfFiles_at_remote, passwd)
    (error, message)=execute_rsync(cmd_listOfFiles_to_local, passwd)
    listOfFiles_path=f'{local_proj_dir}/listOfFiles.list'    
    file_info_dict=create_file_info(read_file_info_list(listOfFiles_path))
        
    priority1_files_dict=filter_dict(file_info_dict,'file_relevance','very_impt')
    priority2_files_dict=filter_dict(file_info_dict,'file_relevance','impt')
    
    for file in list(priority1_files_dict.keys()):
        (error, message)=file_transfer(file,priority1_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir)
    
    for file in list(priority2_files_dict.keys()):
        (error, message)=file_transfer(file,priority2_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir)

    return (error, message)
    
def add_element(dict, key, value):
    if key not in dict:
        dict[key] = {}
    dict[key] = value
    
def create_file_info(list_of_files_in_remote_dir):

    file_info_dict={}
    default_metadata={'file_relevance':'very_impt','file_lifetime':'',
                     'transfer_method':{'method':'direct_transfer','compress_method':'zstd','split_size':''}}
    
    for i in range(len(list_of_files_in_remote_dir)):
        file_extension = pathlib.Path(list_of_files_in_remote_dir[i]).suffix

        if file_extension in list(lfm_file_info.keys()):
            metadata=lfm_file_info[file_extension]
            add_element(file_info_dict, list_of_files_in_remote_dir[i], metadata)
        else:
            add_element(file_info_dict, list_of_files_in_remote_dir[i], default_metadata)
    
    return file_info_dict

def filter_dict(dictionary,filter_key,filter_value):
    """
    function to filter dictionary using key-value pairs
    """
    filtered_dict=[dictionary for d in dictionary.values() if d[filter_key] == filter_value]
    from collections import ChainMap
    filtered_dict = dict(ChainMap(*filtered_dict)) 
    return filtered_dict

def read_file_info_list(filepath_file_list):
    "create python list from listOfFiles.list"    
    file=open(filepath_file_list, 'r')
    data = [line.strip() for line in file]
    file.close()
    return data

def file_transfer(file,priority_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir):
    """
    function to selectively transfer files from remote to local    
    """    
    file_transfer_method=priority_files_dict[file]['transfer_method']['method']
    
    if file_transfer_method=="compress_transfer":
        
        algo_dict={'lz4':'.lz4', 'zstd':'.zst', 'lzop':'.lzo', 'gzip':'.gz', 'bzip2':'.bz2','p7zip':'.7z',
        'xz':'.xz','pigz':'.gz','plzip':'.lz','pbzip2':'.bz2','lbzip2':'.bz2'}
                    
        compression_method=priority_files_dict[file]['transfer_method']['compress_method']
        compressed_file_ext= algo_dict[compression_method]
        
        file_folder=str(pathlib.Path(file).parent)
        file_name=str(pathlib.Path(file).name)
        file = str(file).replace(str(remote_proj_dir), '')        
        cmd_compress_file_at_remote=f'ssh -p {port} {username}@{host} "cd {file_folder}; {compression_method} -f {file_name}"'
        cmd_compress_transfer=f"rsync -R --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file}{compressed_file_ext} {local_proj_dir}"                
        file_folder=str(pathlib.Path(file).parent)
        cmd_decompress_file_at_local=f'cd {local_proj_dir}{file_folder}; {compression_method} -d -f {file_name}{compressed_file_ext}; rm  {file_name}{compressed_file_ext}'
        
        (error, message)=execute_rsync(cmd_compress_file_at_remote, passwd)         
        (error, message)=execute_rsync(cmd_compress_transfer, passwd)

        result=execute(cmd_decompress_file_at_local, local_proj_dir)        
        error=result[cmd_decompress_file_at_local]['output']
        message=result[cmd_decompress_file_at_local]['error'] 
        return (error, message)
                
    elif file_transfer_method=="split_transfer":

        split_size=priority_files_dict[file]['transfer_method']['split_size']                
        file_folder=str(pathlib.Path(file).parent)
        file_name=str(pathlib.Path(file).name)
        file = str(file).replace(str(remote_proj_dir), '')        
        cmd_split_file_at_remote=f'ssh -p {port} {username}@{host} "cd {file_folder}; split -b {split_size} {file_name} {file_name}."'
        cmd_split_files_transfer=f"rsync -R --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file}.?? {local_proj_dir}"                
        file_folder=str(pathlib.Path(file).parent)
        cmd_unsplit_file_at_local=f'cd {local_proj_dir}{file_folder}; cat {file_name}.?? > {file_name}; rm -r {file_name}.*'
        
        (error, message)=execute_rsync(cmd_split_file_at_remote, passwd)  
        (error, message)=execute_rsync(cmd_split_files_transfer, passwd)    
        result=execute(cmd_unsplit_file_at_local, local_proj_dir)        
        error=result[cmd_unsplit_file_at_local]['output']
        message=result[cmd_unsplit_file_at_local]['error']    
        return (error, message)        
    else:
        file = str(file).replace(str(remote_proj_dir), '')
        cmd_direct_transfer=f"rsync -R --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file} {local_proj_dir}"
        (error, message)=execute_rsync(cmd_direct_transfer, passwd)    
        return (error, message)

