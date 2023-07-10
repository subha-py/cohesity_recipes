# Copyright 2020 Cohesity Inc.
#
# Author : Pramod Mandagere (pramod@cohesity.com)
# This file simulates typical ransomware behavior.

import logging
import os
import datetime
# Create a custom logger
logFormat = ('%(asctime)s [%(filename)s:%(lineno)s.%(threadName)s] %(message)s')
formatter = logging.Formatter(logFormat)
time = datetime.datetime.now()
file_name = os.path.join(os.getcwd(), 'simulate-{}.log'.format(time.strftime("%m-%d-%Y_%H-%M-%S")))
logging.basicConfig(level=logging.INFO, format=logFormat)
logger = logging.getLogger('simulate')
fileHandler = logging.FileHandler(filename=file_name)
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

import argparse
import pickle
import random
import requests
import shutil
import string
import time
import zipfile
import threading
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from pathlib import Path
import glob

# See management sdk docs for details:
# https://github.com/cohesity/management-sdk-python
from cohesity_management_sdk.cohesity_client import CohesityClient
from cohesity_management_sdk.models.protection_job_request_body import \
    ProtectionJobRequestBody
from cohesity_management_sdk.models.indexing_policy import IndexingPolicy
from cohesity_management_sdk.exceptions.api_exception import APIException
from cohesity_management_sdk.models.run_type_enum import RunTypeEnum

TEST_KEY = 'test 12345678687'
TIMEOUT_THRESHOLD = 300
RETRY_COUNT = 3
# Artifactory path for static content.
STATIC_CONTENT_DOWNLOAD_URL = "https://artifactory.eng.cohesity.com/" \
    "artifactory/cohesity-builds-tools/qa/automation/misc/TestData.zip"

EXTERNAL_CONTENT_DOWNLOAD_URL = "https://cohesity-my.sharepoint.com/" \
    ":u:/p/jonathan_bell/EdlIkv3TvJhHlQFkXrFGslkBr33wrsexpuvvmnkRDkYLQw?e=AHv7Qh&download=1"
# Tune based on signals observation.
STATIC_CONTENT_FILES_TO_COPY = 30

# The cohesity sdk is not updated to handle 6.7 or newer releases (ENG-261762).
# Workaround is to accept the enum numeric values.
# Refer to iris-tools/sdk_gen_args/api_specs/v2_helios.yaml.
SUCCESS_STATUSES = ['kSuccess', 'Succeeded', 'SucceededWithWarning', 7, 8]
IN_PROGRESS_STATUSES = ['kRunning', 'kAccepted', 'Accepted', 'Running', 1, 2]

#home_directory = Path.home()
home_directory = '/home/cohesity/data'
static_content_directory = Path(home_directory, 'ransomware_simulation','sample_data')
default_data_directory = Path(home_directory,'ransomware_simulation','data')

class Simulation(object):
    def __init__(
            self, clusterip, username, password, vmname, jobnameprefix, policy,
            source, data_dirs, is_fast_simulation):
        self.cluster = clusterip
        self.user = username
        self.passsword = password
        self.vm_name = vmname
        self.job_name = '{0}{1}'.format(jobnameprefix, ''.join(
            random.choices(string.ascii_uppercase + string.digits, k=4)))
        self.policy_name = policy
        self.source_name = source
        self.cohesity_client = CohesityClient(
            cluster_vip=self.cluster, username=self.user,
            password=self.passsword)
        self.job_id = -1
        self.content_url = STATIC_CONTENT_DOWNLOAD_URL
        self.external_content_url = EXTERNAL_CONTENT_DOWNLOAD_URL
        if len(data_dirs) == 0:
            self.data_dirs = [default_data_directory]
        else:
            self.data_dirs = data_dirs.split(",")
        self.is_fast_simulation = is_fast_simulation

    def start(self):
        if not self.is_fast_simulation:
            self.start_regular_simulation()
        else:
            self.start_fast_simulation()

    # Starts simulation process.
    def start_regular_simulation(self):
        # Download static content for use in simulation.
        logger.info(f"{self.vm_name} - Downloading content for use in simulation")
        for data_directory in self.data_dirs:
            if Path(data_directory).exists():
                try:
                    shutil.rmtree(data_directory)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
            if not Path(data_directory).exists():
                os.makedirs(data_directory)

        download_success = download_static_content(self.content_url, self.external_content_url)
        if not download_success:
            logger.info(f"{self.vm_name} - Error downloading sample data, please ensure outbound network access is available and re-run")
            return
        copy_pii_data(self.data_dirs)
        time.sleep(60)

        if os.name != 'nt':
            # Sync the writes.
            os.sync()
        logger.info(f"{self.vm_name} - Creating first full backup")
        job = self._create_protection_job()
        if job is None:
            logger.info(f"{self.vm_name} - Error creating job...aborting")
            return
        self.job_id = job.id
        logger.info(f"{self.vm_name} - Job id is {self.job_id} ")

        # A protection job run isn't always scheduled directly after creating
        # the job. Try triggering it.
        req_body = ProtectionJobRequestBody()
        req_body.run_type = RunTypeEnum.KFULL
        retry=RETRY_COUNT
        while(retry>0):
            try:
                retry = retry - 1
                self.cohesity_client.protection_jobs.create_run_protection_job(
                    id=self.job_id,
                    body=req_body)
                break
            except Exception as e:
                logger.info(f"{self.vm_name} - Exception in calling create_run_protection_job", e)
                time.sleep(5)
                continue

        status = self._wait_for_run_completion()
        if not status:
            logger.info(f"{self.vm_name} - Job {0} failed".format(self.job_name))
            return

        num_backups = 17
        i = 0
        req_body = ProtectionJobRequestBody()
        req_body.run_type = RunTypeEnum.KREGULAR

        logger.info(f"{self.vm_name} - Starting baseline workload")
        while i < num_backups:
            simulate_regular_update(self.data_dirs)
            copy_static_content(self.data_dirs)
            time.sleep(60)
            if os.name != 'nt':
                # Sync the writes.
                os.sync()

            logger.info(f"{self.vm_name} - Creating new backup run_{0}".format(i))
            retry=RETRY_COUNT
            while(retry>0):
                try:
                    retry = retry - 1
                    self.cohesity_client.protection_jobs.\
                    create_run_protection_job(
                        id=self.job_id,
                        body=req_body)
                    break
                except Exception as e:
                    logger.info(f"{self.vm_name} - Exception in calling create_run_protection_job", e)
                    time.sleep(5)
                    continue
            status = self._wait_for_run_completion()
            if not status:
                logger.info(f"{self.vm_name} - Error with backup run, please retry")
                return
            i = i + 1

        logger.info(f"{self.vm_name} - Starting simulation workload")

        encrypt(self.data_dirs)

        if os.name != 'nt':
            # Sync the writes.
            os.sync()

        logger.info(f"{self.vm_name} - Creating new backup run post simulation")
        retry=RETRY_COUNT
        while(retry>0):
            try:
                retry = retry - 1
                self.cohesity_client.protection_jobs.create_run_protection_job(
                    id=self.job_id,
                    body=req_body)
                break
            except Exception as e:
                logger.info(f"{self.vm_name} - Exception in calling create_run_protection_job", e)
                time.sleep(5)
                continue
        status = self._wait_for_run_completion()
        if not status:
            logger.info(f"{self.vm_name} - Error with backup run, please retry")
            return
        logger.info(f"{self.vm_name} - Simulation complete")

        # Starts simulation process.
    def start_fast_simulation(self):
        logger.info(f"{self.vm_name} - Verifying cluster version... ")
        self._verify_cluster_version()
        logger.info(f"{self.vm_name} - Downloading content for use in simulation")
        for data_directory in self.data_dirs:
            if Path(data_directory).exists():
                shutil.rmtree(data_directory)
            os.makedirs(data_directory)

        download_success = download_static_content(self.content_url, self.external_content_url)
        if not download_success:
            logger.info(f"{self.vm_name} - Error downloading sample data, please ensure outbound network access is available and re-run")
            return
        copy_pii_data(self.data_dirs)
        time.sleep(60)

        if os.name != 'nt':
            # Sync the writes.
            os.sync()

        logger.info(f"{self.vm_name} - Creating first full backup")
        job = self._create_protection_job()
        if job is None:
            logger.info(f"{self.vm_name} - Error creating job...aborting")
            return
        self.job_id = job.id

        # A protection job run isn't always scheduled directly after creating
        # the job. Try triggering it.
        req_body = ProtectionJobRequestBody()
        req_body.run_type = RunTypeEnum.KFULL
        self.cohesity_client.protection_jobs.create_run_protection_job(
                        id=self.job_id,
                        body=req_body)

        status = self._wait_for_run_completion()
        if not status:
            logger.info(f"{self.vm_name} - Job {self.job_name} failed")
            return

        req_body = ProtectionJobRequestBody()
        req_body.run_type = RunTypeEnum.KREGULAR

        logger.info(f"{self.vm_name} - Starting baseline workload")
        simulate_regular_update(self.data_dirs)
        copy_static_content(self.data_dirs)
        time.sleep(60)

        if os.name != 'nt':
            # Sync the writes.
            os.sync()

        logger.info(f"{self.vm_name} - Creating new backup run")
        self.cohesity_client.protection_jobs.create_run_protection_job(
            id=self.job_id,
            body=req_body)
        status = self._wait_for_run_completion()
        if not status:
            logger.info(f"{self.vm_name} - Error with backup run, please retry")
            return
        logger.info(f"{self.vm_name} - Starting simulation workload")
        encrypt(self.data_dirs)

        if os.name != 'nt':
            # Sync the writes.
            os.sync()

        logger.info(f"{self.vm_name} - Creating new backup run post simulation")
        self.cohesity_client.protection_jobs.create_run_protection_job(
            id=self.job_id,
            body=req_body)
        status = self._wait_for_run_completion()
        if not status:
            logger.info(f"{self.vm_name} - Error with backup run, please retry")
            return
        logger.info(f"{self.vm_name} - Simulation complete")

    def _verify_cluster_version(self):
        logger.info(f"{self.vm_name} - This script only works with cluster version 6.8.1 and above")
        try:
            cluster_info = self.cohesity_client.get_basic_cluster_info()
        except Exception as e:
            logger.info(f"{self.vm_name} - Exception in API request, cannot verify cluster version. "
                  "Going ahead.", e)
            return True

        logger.info(f"{self.vm_name} - The cluster version is {cluster_info.cluster_software_version}")


    def _wait_for_run_completion(self):
        count = 0
        while True:
            time.sleep(60)
            status, is_complete = self._get_latest_run_status()
            if is_complete:
                if status in SUCCESS_STATUSES:
                    return True
                return False
            count += 1
            if count >= TIMEOUT_THRESHOLD:
                print(
                    "Timed out waiting for job to complete ({0}m)".format(
                        TIMEOUT_THRESHOLD))
                return False

    def _get_latest_run_status(self):
        try:
            runs = self.cohesity_client.protection_runs.get_protection_runs(
                job_id=self.job_id, num_runs=1)
        except Exception as e:
            logger.info(f"{self.vm_name} - Exception in _get_latest_run_status...", e)
            runs=[]
        if len(runs) == 0:
            return "", False
        if runs[0].backup_run.status in IN_PROGRESS_STATUSES:
            return runs[0].backup_run.status, False
        # All other statuses are considered failures.
        return runs[0].backup_run.status, True

    def _get_viewbox(self):
        retry=RETRY_COUNT
        viewboxes = []
        while(retry>0):
            try:
                retry = retry - 1
                viewboxes = self.cohesity_client.view_boxes.get_view_boxes()
                break
            except Exception as e:
                logger.info(f"{self.vm_name} - Exception in calling get_view_boxes", e)
                time.sleep(5)
                continue
        for viewbox in viewboxes:
            return viewbox
        return None

    def _get_policy(self):
        retry=RETRY_COUNT
        policies = []
        while(retry>0):
            try:
                retry = retry - 1
                policies = self.cohesity_client.protection_policies.\
                    get_protection_policies(
                        names=self.policy_name)
                break
            except Exception as e:
                logger.info(f"{self.vm_name} - Exception in calling protection_policies", e)
                time.sleep(5)
                continue
        if policies is not None and len(policies) != 0:
            return policies[0]
        return None

    def _get_source(self):
        retry=RETRY_COUNT
        sources = []
        while(retry>0):
            try:
                retry = retry - 1
                sources = self.cohesity_client.protection_sources.\
                    list_protection_sources(
                        environments='kVMware')
                break
            except Exception as e:
                logger.info(f"{self.vm_name} - Exception in calling protection_sources", e)
                time.sleep(5)
                continue
        if sources is None:
            return None
        for source in sources:
            if source.protection_source.name == self.source_name:
                return source.protection_source
        return None

    def _get_vm(self):
        vms = self.cohesity_client.protection_sources.\
            get_protection_sources_objects()
        if vms is None:
            return None
        for vm in vms:
            if vm.name == self.vm_name:
                return vm
        return None

    def _create_protection_job(self):
        policy = self._get_policy()
        if policy is None:
            logger.info(f"{self.vm_name} - Policy {self.policy_name} not found")
            return

        source = self._get_source()
        if source is None:
            logger.info(f"{self.vm_name} - Source/Vcenter {self.source_name} not found. Please ensure source/vcenter is registered on cluster before running simulations")
            return

        vm = self._get_vm()
        if vm is None:
            logger.info(f"{self.vm_name} - VM {self.vm_name} not found. Please ensure source is registered on cluster and vm is present on vcenter")
            return

        viewbox = self._get_viewbox()
        if viewbox is None:
            logger.info(f"{self.vm_name} - Viewbox cannot be determined")
            return

        payload = ProtectionJobRequestBody()
        payload.name = self.job_name
        payload.policy_id = policy.id
        payload.environment = 'kVMware'
        payload.view_box_id = viewbox.id
        payload.parent_source_id = source.id
        payload.source_ids = [vm.id]
        payload.timezone = 'America/Los_Angeles'
        indexing_policy = IndexingPolicy(["/"], [
            "/Recovery", "/var", "/usr", "/sys", "/proc", "/lib", "/grub",
            "/grub2", "/opt/splunk", "/splunk"], False)
        payload.indexing_policy = indexing_policy
        try:
            return self.cohesity_client.protection_jobs.create_protection_job(
                payload)
        except APIException as e:
            logger.info(f"{self.vm_name} - Exception in API request, update params and retry", e)
            return None


def download_static_content(primary_url, secondary_url):
    if not Path(static_content_directory).exists():
        os.makedirs(static_content_directory)
    file_name = Path(static_content_directory,"sample_data.zip")        
    if not file_name.is_file():
        with open(file_name, 'wb') as file:
            # Try internal URL first.
            resp = requests.get(primary_url)
            if resp.status_code != 200:
                # If internal URL is not accessible, try external URL.
                resp = requests.get(secondary_url)
                if resp.status_code != 200:
                    return False
            file.write(resp.content)
            with zipfile.ZipFile(file_name.absolute(), 'r') as zip_ref:
                zip_ref.extractall(static_content_directory)
            return True
    else:
        logger.info("Skipping download as sample data already present")
        logger.info(file_name.absolute())
        with zipfile.ZipFile(file_name.absolute(), 'r') as zip_ref:
            zip_ref.extractall(static_content_directory)
        return True


def copy_static_content(data_dirs):
    logger.info("Copying over static files")
    glob_path = "{0}{1}**{1}*.*".format(Path(static_content_directory.absolute(),'TestData').absolute(), os.sep)
    available_files = glob.glob(glob_path, recursive = True)
    count = 0
    while count < STATIC_CONTENT_FILES_TO_COPY:
        data_directory = random.choice(data_dirs)
        file_to_copy = random.choice(available_files)
        head, file_name = os.path.split(file_to_copy)
        # Copy file from static directory to data directory.
        destination = Path(data_directory,file_name)
        shutil.copyfile(file_to_copy,destination)
        count += 1

def copy_pii_data(data_dirs):
    data_directory = random.choice(data_dirs)
    glob_path = "{0}{1}*.*".format(Path(static_content_directory.absolute(),'PiiData').absolute(), os.sep)
    available_files = glob.glob(glob_path, recursive = False)
    for file_to_copy in available_files:
        head, file_name = os.path.split(file_to_copy)
        # Copy file from static directory to data directory.
        destination = Path(data_directory,file_name)
        shutil.copyfile(file_to_copy,destination)

def simulate_regular_update(data_dirs):
    counter = 0
    num_files = random.randint(80, 100)
    logger.info(f"Generating {num_files} files")
    while counter < num_files:
        data_directory = random.choice(data_dirs)
        data = ''.join(['This is a test'] * 1024 * 512)
        ii = random.randint(0, 1000)
        outfile = Path(data_directory,'outfile{0}.txt'.format(ii))
        with open(outfile, 'wb') as fp:
            pickle.dump(data, fp)
        counter += 1

def encrypt(data_dirs):
    key = get_random_bytes(16)
    for data_directory in data_dirs:
        for dir_path, dirs, files in os.walk(data_directory):
            for file in files:
                absolute_path = Path(dir_path, file).absolute()
                with open(absolute_path, 'r+b') as f:
                    raw_data = f.read(2048)
                    while raw_data:
                        cipher = AES.new(key, AES.MODE_GCM)
                        ciphertext, tag = cipher.encrypt_and_digest(raw_data)
                        f.seek(-len(raw_data), 1)
                        f.write(ciphertext)
                        raw_data = f.read(2048)
                os.rename(absolute_path, "{}.enc".format(absolute_path))

def start_simulation(**kwargs):
    simulation = Simulation(**kwargs)
    simulation.start()
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", dest='cluster',
                        help="Cluster VIP", required=True)
    parser.add_argument("--user", dest='user', help="Username", required=True)
    parser.add_argument("--password", dest='password',
                        help="Password", required=True)
    parser.add_argument("--vms", dest='vms', help="VM names separated by comma (,)", required=True)
    parser.add_argument("--vcenter", dest='vcenter',
                        help="Vcenter name", required=True)
    parser.add_argument("--policy", dest='policy',
                        help="Policy name", required=True)
    parser.add_argument("--jobprefix", dest="prefix", default="testSimJob")
    parser.add_argument("--data_dirs", dest="data_dirs", default="")
    parser.add_argument("--fast_simulation", dest="is_fast_simulation", default=False)

    args = parser.parse_args()

    vms = args.vms.split(',')
    logger.info("Main    : before creating thread")
    threads = {}
    for vm in vms:
        logger.info("Main    : create and start thread - {}".format(vm))
        kwargs = dict(clusterip=args.cluster,
                                username=args.user,
                                password=args.password,
                                vmname=vm,
                                jobnameprefix = args.prefix,
                                policy=args.policy,
                                source=args.vcenter,
                                data_dirs=args.data_dirs,
                                is_fast_simulation=args.is_fast_simulation)
        x = threading.Thread(target=start_simulation, kwargs=kwargs)
        threads[vm] = x
        x.start()

    logger.info("Main    : wait for the thread to finish")
    for vm in threads:
        logger.info("Main    : before joining thread {}".format(vm))
        threads[vm].join()
        logger.info("Main    : thread {} done".format(vm))
    logger.info("Main    : all done")
