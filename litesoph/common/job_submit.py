import subprocess  
import pathlib
import sys
import paramiko
import socket
import pathlib
import subprocess
import re
from scp import SCPClient
from paramiko import SSHClient, RSAKey, AutoAddPolicy
import pexpect
from litesoph.utilities import keys_exists
import platform
if platform.system() == 'Windows':
    from pexpect import popen_spawn


def execute_cmd_local(command, directory):
    """
    Function to execute command locally

    Parameter:
    command: command to be run
    directory: directory in which command need to be run
    """
    
    result = {}
    
    if isinstance(command, str):
        command = [command]

    for cmd in command:
        out_dict = result[cmd] = {}
        try:
            job = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd= directory, shell=True)
            output = job.communicate()
        except Exception:
            raise 
        else:
            if job.returncode != 0:
                print("Error...")
            else:
                pass
            
            out_dict['returncode'] = job.returncode
            out_dict['pid'] = job.pid
            out_dict['output'] = output[0].decode(encoding='utf-8')
            out_dict['error'] = output[1].decode(encoding='utf-8')
    return result

def execute_cmd_remote(command,passwd, timeout=None):
    """
    Function to run command on remote machine through local machine, Thus function requires password of remote machine
    
    paramter:

    command: command to be run on remote machine
    password: password of remote machine
    timeout: timeperiod in seconds password prompt waits
    """
    intitial_response = ['Are you sure', 'assword:','[#\$] ', pexpect.EOF]
    
    if platform.system() == 'Windows':
        ssh = popen_spawn.PopenSpawn(command,timeout=timeout)
    else:
        ssh = pexpect.spawn(command,timeout=timeout)
    i = ssh.expect(intitial_response)
    if i == 0 :
        T = ssh.read(100)
        ssh.sendline('yes')
        ssh.expect('assword:')
        ssh.sendline(passwd)
    elif i == 1:
        ssh.sendline(passwd)
    elif i==2:
        prompt = ssh.after  
    else:
        str1 = str(ssh.before)
        return (-3, 'Error: Unknown:'+ str1)

    possible_response = ['assword:', pexpect.EOF]
    i = ssh.expect(possible_response, timeout=5)

    if i == 0:
        return (-4, "Error: Incorrect password.")
    else:
        output = ssh.before.decode('utf-8')
        return (0, output)

class SubmitLocal:

    def __init__(self, task) -> None:
        self.task = task
        self.task_info = task.task_info
        self.project_dir = self.task.project_dir
        self.command = None     
        self.job_id=None              

    def run_job(self, cmd): 
        self.task_info.job_info.submit_mode = 'local' 
        result = execute_cmd_local(cmd, self.project_dir)
        self.task_info.job_info.id = result[cmd]['pid']
        self.task_info.job_info.job_returncode = result[cmd]['returncode']
        self.task_info.job_info.output = result[cmd]['output']
        self.task_info.job_info.error = result[cmd]['error']

    def get_job_status_local(self,job_id):   
        """
        get the running status of submitted job at remote
        """
        job_id= self.job_id
        cmd_check_running_process=f"ps aux | grep {job_id}|grep -v grep; if [ $? -eq 0 ]; then echo Job is running; else echo No Job found; fi"
        result=execute_cmd_local(cmd_check_running_process,self.project_dir)
        error=result[cmd_check_running_process]['error']    
        message=result[cmd_check_running_process]['output']         
        return (error, message)
    
    def get_fileinfo_local(self):   
        """
        get the generated file information during runtime
        """
        cmd_filesize=f'find {self.project_dir}  -type f -exec du --human {{}} + | sort --human --reverse'
        result=execute_cmd_local(cmd_filesize,self.project_dir)
        error=result[cmd_filesize]['error']    
        message=result[cmd_filesize]['output']  

        cmd_project_size=f'cd {self.project_dir}; du -s'

        result= execute_cmd_local(cmd_project_size,self.project_dir)  
        error=result[cmd_project_size]['error']    
        message=result[cmd_project_size]['output']

        project_size= [int(s) for s in message.split() if s.isdigit()]
        self.project_size_GB=project_size[0]/(1024*1024)
    
        return (error, message)
    
    def generate_list_of_files_local(self):
        cmd=f'cd {self.project_dir}; find "$PWD"  -type f > listOfFiles_local.list'        
        result=execute_cmd_local(cmd,self.project_dir)
        error=result[cmd]['error']    
        message=result[cmd]['output']            
        return (error, message)

    def get_list_of_files_local(self):
        listOfFiles_path=f'{self.project_dir}/listOfFiles_local.list'   
        from litesoph.common.lfm_database import lfm_file_info_dict
        lfm_file_info=lfm_file_info_dict()
        file_info_dict=create_file_info(read_file_info_list(listOfFiles_path),lfm_file_info)        
        # files_dict=filter_dict(file_info_dict,{'file_type':['input_file','property_file']})        
        files_list=list(file_info_dict.keys())
        return files_list        

    def view_specific_file_local(self,file):
        cmd_view_local=f"cat {file}"
        result=execute_cmd_local(cmd_view_local,self.project_dir)
        error=result[cmd_view_local]['error']    
        message=result[cmd_view_local]['output']            
        return (error, message)
    
    def kill_job_local(self,job_id,scheduler,scheduler_stat_cmd,scheduler_kill_cmd):
        """
        kill the running job at local
        """
        if scheduler=='bash':
            cmd_kill=f"ps aux | grep -w {job_id}|grep -v grep; if [ $? -eq 0 ]; pkill -ecf {job_id}; then echo Job killed; else echo No Job found; fi"
        else:
            cmd_kill=f"{scheduler_stat_cmd} | grep -w {job_id}|grep -v grep; if [ $? -eq 0 ]; {scheduler_kill_cmd} {job_id}; then echo Job killed {job_id}; else echo No Job found; fi"        
        
        result=execute_cmd_local(cmd_kill,self.project_dir)
        error=result[cmd_kill]['error']    
        message=result[cmd_kill]['output']            
        return (error, message)       


class SubmitNetwork:

    def __init__(self, task, hostname : str, username : str, password : str, port : int, remote_path : str, pkey_file : str, passwordless_ssh : str, ls_file_mgmt_mode = False) -> None:
        self.task = task
        self.task.task_info.state.network = True
        self.project_dir = self.task.project_dir.parent
        self.username = username
        self.hostname = hostname
        self.password = password
        self.pkey_file=pathlib.Path(pkey_file)
        self.port = port
        self.remote_path = remote_path
        self.passwordless_ssh = passwordless_ssh
        self.ls_file_mgmt_mode=ls_file_mgmt_mode
        self.sshClient = SSHClient()
        try:
            self.sshClient.set_missing_host_key_policy(AutoAddPolicy())
            if passwordless_ssh == True:
                self.pkey = RSAKey.from_private_key_file(self.pkey_file)
                self.sshClient.connect(
                    self.hostname, self.port, self.username, pkey = self.pkey
                )
            else:
                self.sshClient.connect(
                    self.hostname, self.port, self.username, self.password
                )
            self._check_connection()
        except paramiko.AuthenticationException:
            raise Exception("Authentication failed, please verfiy your credentials")
        except paramiko.SSHException as sshException:
            raise Exception(f"Could not establish SSH connection: {sshException} ")
        except socket.timeout as e:
            raise Exception("Connection timed out")
        except Exception as e:
           raise Exception(f"Exception in connecting to the server {e}")

        self._check_file(self.remote_path)
        self.scp = SCPClient(self.sshClient.get_transport(), progress= lambda file, size, sent: sys.stdout.write(f"{file}'s progress: {float(sent)/float(size)*100 : .2f}   \r"))
        self.task.add_proper_path(self.remote_path)
        self._upload_files()
        self.task.task_info.job_info.submit_mode = 'remote'

    def _check_connection(self):
        "This checks whether ssh connection is active or not."
        transport = self.sshClient.get_transport()
        if not transport.is_active() and transport.is_authenticated():
            raise Exception('Not connected to a cluster')

    def _check_file(self, remote_file_path):
        "checks if the file exists in the remote path"
        self._check_connection()
        sftp = self.sshClient.open_sftp()
        sftp.stat(remote_file_path)

    def _upload_files(self):
        "uploads entire project directory to remote path"
        self._check_connection()
        self.scp.put(self.project_dir, remote_path= self.remote_path, recursive=True)

    def download_output_files(self):
        "Downloads entire project directory to local project dir."
        remote_path = pathlib.Path(self.remote_path) / self.project_dir.name
        if self.ls_file_mgmt_mode == False:
            self.scp.get(remote_path, self.project_dir.parent, recursive=True)
            return

        self._check_connection()
        from litesoph.common.lfm_database import lfm_file_info_dict
        lfm_file_info = lfm_file_info_dict()
        self.sshClient.exec_command(f'cd {remote_path}; find "$PWD"  -type f > listOfFiles_remote.list')
        self.scp.get(f'{remote_path}/listOfFiles_remote.list', self.project_dir)
        
        data = []
        with open(f'{self.project_dir}/listOfFiles_remote.list', 'r') as file:
            data = file.read()
        data = data.split('\n')
        data = [x for x in data if not re.search(r'listOfFiles', x)]
        file_info = create_file_info(data, lfm_file_info)

        priority1_files_dict=filter_dict(file_info,{'file_relevance':['very_impt']})
        priority2_files_dict=filter_dict(file_info,{'file_relevance':['impt']})

        (error, message)=download_files_from_remote(self.hostname,self.username,self.port,self.password,remote_path,self.project_dir)
        if error != 0: raise Exception(message)

    def download_files(self, remote_file_path, local_path):
        "This method downloads the file from cluster"
        self._check_connection()
        self.scp.get(remote_file_path, local_path, recursive=True)

    def get_output_log(self):
        "Downloads engine log file for that particular task."
        wfdir = pathlib.Path(self.task.project_dir)
        proj_name = pathlib.Path(self.project_dir).name
        wf_name = wfdir.name
        engine_log = pathlib.Path(self.task.task_info.output['txt_out'])
        rpath = pathlib.Path(self.remote_path) / proj_name / wf_name / engine_log
        lpath = wfdir / engine_log
        self._check_connection()
        self.scp.get(rpath, lpath)

    def run_job(self, cmd):
        "This method creates the job and submission command and executes the command on the cluster"
        remote_path = pathlib.Path(self.remote_path) / self.task.project_dir.relative_to(self.task.project_dir.parent.parent)
        self.command = f"cd {str(remote_path)} && {cmd} {self.task.BASH_filename}".replace("\\", "/")
        self._check_connection()
        self.task.task_info.job_info.submit_returncode = -1
        stdin, stdout, stderr = self.sshClient.exec_command(self.command)
        self.task.task_info.job_info.submit_output = stdout.read().decode()
        self.task.task_info.job_info.submit_error = stderr.read().decode()
        if not self.task.task_info.job_info.submit_error:
            self.task.task_info.job_info.submit_returncode = 0

    def check_job_status(self) -> bool:
        """returns true if the job is completed in remote machine"""
        job_id = self.task.task_info.uuid
        job_done_file = pathlib.Path(self.remote_path) / self.task.network_done_file.parent.relative_to(self.project_dir.parent)/ f"Done_{job_id}"
        job_done_status=self._check_file(str(job_done_file))
        return job_done_status
    
    def get_fileinfo_remote(self):
        """
        get the generated file information during runtime
        """
        cmd_create_listOfFiles_at_remote=f'ssh -p {self.port} {self.username}@{self.hostname} "cd {self.remote_path}; find "$PWD"  -type f > listOfFiles.list"'
        cmd_create_listOfFiles_at_remote = 'cd {self.remote_path} && find "$PWD"  -type f > listOfFiles.list'
        self._check_connection()
        self.sshClient.exec_command(cmd_create_listOfFiles_at_remote)

        self.scp.get(f'{self.remote_path}/listOfFiles.list', self.project_dir)
  
        cmd_filesize=f'"cd {self.remote_path}; find "$PWD"  -type f -exec du --human {{}} + | sort --human --reverse"'
        self.sshClient.exec_command(cmd_filesize)

        cmd_project_size=f'cd {self.remote_path}; du -s'
        stdin, stdout, stderr = self.sshClient.exec_command(cmd_project_size)
        message = stdout.read().decode()
        project_size= [int(s) for s in message.split() if s.isdigit()]
        self.project_size_GB=project_size[0]/(1024*1024)
        return (0, message)

    def get_job_status_remote(self):
        job_id=self.task.task_info.uuid
        job_start_file = pathlib.Path(self.remote_path) / self.task.network_done_file.parent.relative_to(self.project_dir.parent) / f"Start_{job_id}"
        try:
            self._check_file(str(job_start_file))
            job_start_status = True
        except:
            job_start_status = False
        job_done_file = pathlib.Path(self.remote_path) / self.task.network_done_file.parent.relative_to(self.project_dir.parent)/ f"Done_{job_id}"
        try:
            self._check_file(str(job_done_file))
            job_done_status = True
        except:
            job_done_status = False

        if job_start_status==False:
            job_status="Job Not Started Yet"
        
        elif job_start_status==True and job_done_status==False: 
            job_status="Job in Progress"
        
        elif job_start_status==True and job_done_status==True:
            job_status="Job Done"    

        else:
            job_status="SSH session not active"

        return job_status

    def kill_job_remote(self,job_id,scheduler,scheduler_stat_cmd,scheduler_kill_cmd):
        """
        kill the running job at remote
        """
        if scheduler=='bash':
            cmd_kill=f"ps aux | grep -w {job_id}|grep -v grep; if [ $? -eq 0 ]; pkill -ecf {job_id}; then echo Job killed; else echo No Job found; fi"
        else:
            cmd_kill=f"{scheduler_stat_cmd} | grep -w {job_id}|grep -v grep; if [ $? -eq 0 ]; {scheduler_kill_cmd} {job_id}; then echo Job killed {job_id}; else echo No Job found; fi"
        self._check_connection()
        stdin, stdout, stderr = self.sshClient.exec_command(cmd_kill)
        message = stdout.read().decode()
        return (0, message)

    def download_all_files_remote(self):
        """
        download all files from remote
        """
        remote_path = pathlib.Path(self.remote_path) / self.project_dir.name
        self._check_connection()
        self.scp.get(str(remote_path).replace("\\", '/'), self.project_dir.parent, recursive=True)
        return (0, "All files downloaded successfully")
    
    def get_list_of_files_remote(self):
        listOfFiles_path=f'{self.project_dir}/listOfFiles.list'
        from litesoph.common.lfm_database import lfm_file_info_dict
        lfm_file_info=lfm_file_info_dict()
        file_info_dict=create_file_info(read_file_info_list(listOfFiles_path),lfm_file_info)
        files_list=list(file_info_dict.keys())
        return files_list

    def download_specific_file_remote(self,file_path,priority1_files_dict):
        """
        download specific file(s) from remote
        """
        (error, message)=file_transfer(file_path,priority1_files_dict,self.hostname,self.username,self.port,self.password,self.remote_path,self.project_dir)
        return (error, message)

    def view_specific_file_remote(self,file):
        cmd_view_remote=f"cat {file}"
        stdin, stdout, stderr = self.sshClient.exec_command(cmd_view_remote)
        message = stdout.read().decode()
        return (0, message)

    def disconnect(self):
        "closes the ssh session with the cluster."
        if self.sshClient:
            self.sshClient.close()
        if self.scp:
            self.scp.close()

    def view_specific_file_remote(self,file):
        stdin, stdout, stderr = self.sshClient.exec_command(f"cat {file}")
        return 0, stdout.read().decode()

### SOME UTILITIES TO ABOVE CLASS

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

    from litesoph.common.lfm_database import lfm_file_info_dict
    lfm_file_info=lfm_file_info_dict()

    cmd_create_listOfFiles_at_remote=f'ssh -p {port} {username}@{host} "cd {remote_proj_dir}; find "$PWD"  -type f > listOfFiles_remote.list"'     
    cmd_listOfFiles_to_local=f"rsync --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/listOfFiles_remote.list {local_proj_dir}"
    # cmd_remove_listOfFiles_remote=f'ssh -p {port} {username}@{host} "cd {remote_proj_dir}; rm listOfFiles.list'

    (error, message)=execute_cmd_remote(cmd_create_listOfFiles_at_remote, passwd)
    (error, message)=execute_cmd_remote(cmd_listOfFiles_to_local, passwd)
    # (error, message)=execute_cmd_remote(cmd_remove_listOfFiles_remote, passwd)
    
    listOfFiles_path=f'{local_proj_dir}/listOfFiles_remote.list'
    file_info_dict=create_file_info(read_file_info_list(listOfFiles_path),lfm_file_info)

    # if keys_exists(file_info_dict,'file_relevance')==False:

    priority1_files_dict=filter_dict(file_info_dict,{'file_relevance':['very_impt']})
    priority2_files_dict=filter_dict(file_info_dict,{'file_relevance':['impt']})
    
    for file in list(priority1_files_dict.keys()):
        (error, message)=file_transfer(file,priority1_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir)
    
    for file in list(priority2_files_dict.keys()):
        (error, message)=file_transfer(file,priority2_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir)

    # cmd_remove_listOfFiles_local=f'rm {local_proj_dir}/listOfFiles.list'
    # (error, message)=execute_cmd_remote(cmd_remove_listOfFiles_local, passwd)
    
    return (error, message)
    
def add_element(dict, key, value):
    if key not in dict:
        dict[key] = {}
    dict[key] = value
    
def create_file_info(list_of_files_in_remote_dir,lfm_file_info):

    file_info_dict={}
    default_metadata={'file_relevance':None,
                    'file_lifetime':None,
                    'file_type':None,
                     'transfer_method':None}
    
    for i in range(len(list_of_files_in_remote_dir)):
        file_extension = pathlib.Path(list_of_files_in_remote_dir[i]).suffix

        if file_extension in list(lfm_file_info.keys()):
            metadata=lfm_file_info[file_extension]
            file_info_dict[list_of_files_in_remote_dir[i]] = metadata
        else:
            file_info_dict[list_of_files_in_remote_dir[i]] = default_metadata
    
    return file_info_dict

def filter_dict(dictionary,dict_filter_key_value):
    """
    function to filter dictionary using key-value pairs
    """
    filtered_dict={}
    for key in dictionary.keys():
        for filter_key in list(dict_filter_key_value.keys()):
            for subkey in dict_filter_key_value[filter_key]:
                if dictionary[key][filter_key]==subkey:
                    filtered_dict[key]=dictionary[key]
    return filtered_dict

def read_file_info_list(filepath_file_list):
    "create python list from listOfFiles.list"    
    file=open(filepath_file_list, 'r')
    data = [line.strip() for line in file]
    data = [x for x in data if not re.search(r'listOfFiles', x)]    
    file.close()
    return data

def check_available_compress_methd(local_proj_dir,host,username,port,passwd):
    """function to check the available compression method in local and remote machine"""
    from litesoph.common.lfm_database import compression_algo_dict
    
    available_methods_local=[]
    available_methods_remote=[]

    try:
        for key in compression_algo_dict.keys():
            cmd=f'which {key}'
            result=execute_cmd_local(cmd, local_proj_dir)
            error=result[cmd]['error']
            message=result[cmd]['output'] 

            if f'/bin/{key}' in message:
                available_methods_local.append(key)
            
            cmd_remote=f'ssh -p {port} {username}@{host} {cmd}'   
            (error, message)=execute_cmd_remote(cmd_remote, passwd)
            
            if f'/bin/{key}' in message:
                available_methods_remote.append(key)

        available_methods_local_remote=list(set(available_methods_local) & set(available_methods_remote))
    except:
        raise error

    return available_methods_local_remote
        
def file_transfer(file,priority_files_dict,host,username,port,passwd,remote_proj_dir,local_proj_dir):
    """
    function to selectively transfer files from remote to local    
    """    
    if keys_exists(priority_files_dict,'transfer_method')==True:
        file_transfer_method=priority_files_dict[file]['transfer_method']['method']
        
        if file_transfer_method=="compress_transfer":
            from litesoph.common.lfm_database import compression_algo_dict

            list_of_compression_methd=check_available_compress_methd(local_proj_dir,host,username,port,passwd)
            
            if keys_exists(priority_files_dict[file],'compress_method')==False:
                compression_method=list_of_compression_methd[0]
            else:
                compression_method=priority_files_dict[file]['compress_method']
                if compression_method in list_of_compression_methd:                 
                    compressed_file_ext= compression_algo_dict[compression_method]
                else:
                    compression_method=list_of_compression_methd[0]
                    compressed_file_ext= compression_algo_dict[compression_method]
            
            file_folder=str(pathlib.Path(file).parent)
            file_name=str(pathlib.Path(file).name)
            file = str(file).replace(str(remote_proj_dir), '')        
            cmd_compress_file_at_remote=f'ssh -p {port} {username}@{host} "cd {file_folder}; {compression_method} -f {file_name}"'
            cmd_compress_transfer=f"rsync -R --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file}{compressed_file_ext} {local_proj_dir}"                
            file_folder=str(pathlib.Path(file).parent)
            cmd_decompress_file_at_local=f'cd {local_proj_dir}{file_folder}; {compression_method} -d -f {file_name}{compressed_file_ext}; rm  {file_name}{compressed_file_ext}'
            
            (error, message)=execute_cmd_remote(cmd_compress_file_at_remote, passwd)         
            (error, message)=execute_cmd_remote(cmd_compress_transfer, passwd)

            result=execute_cmd_local(cmd_decompress_file_at_local, local_proj_dir)        
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
            
            (error, message)=execute_cmd_remote(cmd_split_file_at_remote, passwd)  
            (error, message)=execute_cmd_remote(cmd_split_files_transfer, passwd)    
            result=execute_cmd_local(cmd_unsplit_file_at_local, local_proj_dir)        
            error=result[cmd_unsplit_file_at_local]['output']
            message=result[cmd_unsplit_file_at_local]['error']    
            return (error, message)        
        else:
            file = str(file).replace(str(remote_proj_dir), '')
            cmd_direct_transfer=f"rsync -vR --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file} {local_proj_dir}"
            print("\nTransferring File :",file)
            (error, message)=execute_cmd_remote(cmd_direct_transfer, passwd)    
            return (error, message)    
    else:
        file = str(file).replace(str(remote_proj_dir), '')
        cmd_direct_transfer=f"rsync -vR --rsh='ssh -p{port}' {username}@{host}:{remote_proj_dir}/.{file} {local_proj_dir}"
        print("\nTransferring File :",file)
        (error, message)=execute_cmd_remote(cmd_direct_transfer, passwd)    
        return (error, message)
