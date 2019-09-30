#!/usr/bin/env python3

# import of built-in modules
import contextlib
import logging
import os
import shutil
import stat
import subprocess
import random
import sys

# import of third-party modules

# import of local modules
import ccf.one_subject_job_submitter as one_subject_job_submitter
import ccf.processing_stage as ccf_processing_stage
import ccf.subject as ccf_subject
import utils.debug_utils as debug_utils
import utils.str_utils as str_utils
import utils.os_utils as os_utils
import utils.user_utils as user_utils
import ccf.archive as ccf_archive

# authorship information
__author__ = "Timothy B. Brown"
__copyright__ = "Copyright 2019, Connectome Coordination Facility"
__maintainer__ = "Junil Chang"

# create a module logger
module_logger = logging.getLogger(__name__)
# Note: This can be overidden by log file configuration
module_logger.setLevel(logging.WARNING)


class OneSubjectJobSubmitter(one_subject_job_submitter.OneSubjectJobSubmitter):

	@classmethod
	def MY_PIPELINE_NAME(cls):
		return 'FunctionalPreprocessing'

	def __init__(self, archive, build_home):
		super().__init__(archive, build_home)

	@property
	def PIPELINE_NAME(self):
		return OneSubjectJobSubmitter.MY_PIPELINE_NAME()

	@property
	def WORK_NODE_COUNT(self):
		return 1

	@property
	def WORK_PPN(self):
		return 1

	def create_get_data_job_script(self):
		"""Create the script to be submitted to perform the get data job"""
		module_logger.debug(debug_utils.get_name())

		script_name = self.get_data_job_script_name

		with contextlib.suppress(FileNotFoundError):
			os.remove(script_name)

		script = open(script_name, 'w')

		self._write_bash_header(script)
		script.write('#PBS -l nodes=1:ppn=1,walltime=4:00:00,mem=4gb' + os.linesep)
		script.write('#PBS -o ' + self.working_directory_name + os.linesep)
		script.write('#PBS -e ' + self.working_directory_name + os.linesep)
		script.write(os.linesep)
		script.write('source ' + self._get_xnat_pbs_setup_script_path() + ' ' + self._get_db_name() + os.linesep)
		script.write('module load ' + self._get_xnat_pbs_setup_script_singularity_version() + os.linesep)
		script.write(os.linesep)
		script.write('singularity exec -B ' + self._get_xnat_pbs_setup_script_archive_root() + ',' + self._get_xnat_pbs_setup_script_singularity_bind_path() + ' ' + self._get_xnat_pbs_setup_script_singularity_container_xnat_path() + ' ' + self.get_data_program_path  + ' \\' + os.linesep)
		script.write('  --project=' + self.project + ' \\' + os.linesep)
		script.write('  --subject=' + self.subject + ' \\' + os.linesep)
		script.write('  --classifier=' + self.classifier + ' \\' + os.linesep)

		if self.scan:
			script.write('  --scan=' + self.scan + ' \\' + os.linesep)
			
		script.write('  --working-dir=' + self.working_directory_name + ' \\' + os.linesep)

		script.write(os.linesep)
		script.write('rm -rf ' + self.working_directory_name + os.sep + self.subject + '_' + self.classifier + '/unprocessed/T1w_MPR_vNav_4e_RMS' + os.linesep)
		script.write('find ' + self.working_directory_name + os.sep + self.subject + '_' + self.classifier + '/unprocessed/ -maxdepth 1 -mindepth 1 ' )
		script.write('-type d -not -path ' + self.working_directory_name + os.sep + self.subject + '_' + self.classifier + '/unprocessed/' + self.scan )
		script.write(' -path \'' + self.working_directory_name + os.sep + self.subject + '_' + self.classifier + '/unprocessed/[rt]fMRI_*_[AP][PA]\' -exec rm -rf \'{}\' \;' + os.linesep)

		script.close()
		os.chmod(script_name, stat.S_IRWXU | stat.S_IRWXG)

	def create_clean_data_script(self):
		module_logger.debug(debug_utils.get_name())

		script_name = self.clean_data_script_name

		with contextlib.suppress(FileNotFoundError):
			os.remove(script_name)

		script = open(script_name, 'w')

		self._write_bash_header(script)
		script.write('#PBS -l nodes=1:ppn=1,walltime=4:00:00,mem=4gb' + os.linesep)
		script.write('#PBS -o ' + self.working_directory_name + os.linesep)
		script.write('#PBS -e ' + self.working_directory_name + os.linesep)
		script.write(os.linesep)
		
		script.write('find ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep)
		script.write('subjects' + os.path.sep + self.subject + '_' + self.classifier + os.path.sep  + 'hcp' + os.path.sep)
		script.write(self.subject + '_' + self.classifier + ' \! -newer ' + self.starttime_file_name + ' -delete')
		script.write(os.linesep)
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep)
		script.write('subjects' + os.path.sep + self.subject + '_' + self.classifier + os.path.sep  + 'hcp' + os.path.sep)
		script.write(self.subject + '_' + self.classifier + os.path.sep + '* ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.linesep)
					
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'subjects' + os.path.sep + 'specs ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'processing ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'info' + os.path.sep + 'hcpls ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)
		script.write('cp ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'subjects' + os.path.sep + self.subject + '_' + self.classifier + os.path.sep  + 'subject_hcp.txt ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.path.sep + 'processing' + os.linesep)
		script.write('cp ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'subjects' + os.path.sep + self.subject + '_' + self.classifier + os.path.sep  + 'hcpls' + os.path.sep  + 'hcpls2nii.log ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.path.sep + 'processing' + os.linesep)
		
		script.write('find ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier)
		script.write(' -not -path "' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'T1w/*"')
		script.write(' -not -path "' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo/*"')
		script.write(' -not -path "' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'MNINonLinear/*"')
		script.write(' -not -path "' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + self.scan + '/*"')
		script.write(' -delete')
		script.write(os.linesep)
		script.write('echo "Removing any XNAT catalog files still around."' + os.linesep)
		script.write('find ' + self.working_directory_name + ' -name "*_catalog.xml" -delete')
		script.write(os.linesep)
		script.write('echo "Remaining files:"' + os.linesep)
		script.write('find ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.linesep)

		script.close()
		os.chmod(script_name, stat.S_IRWXU | stat.S_IRWXG)


	def create_process_data_job_script(self):
		module_logger.debug(debug_utils.get_name())

		xnat_pbs_jobs_control_folder = os_utils.getenv_required('XNAT_PBS_JOBS_CONTROL')

		subject_info = ccf_subject.SubjectInfo(self.project, self.subject,
											   self.classifier, self.scan)

		script_name = self.process_data_job_script_name

		with contextlib.suppress(FileNotFoundError):
			os.remove(script_name)

		walltime_limit_str = str(self.walltime_limit_hours) + ':00:00'
		vmem_limit_str = str(self.vmem_limit_gbs) + 'gb'

		resources_line = '#PBS -l nodes=' + str(self.WORK_NODE_COUNT)
		resources_line += ':ppn=' + str(self.WORK_PPN) + ':haswell'
		resources_line += ',walltime=' + walltime_limit_str
		resources_line += ',mem=' + vmem_limit_str

		stdout_line = '#PBS -o ' + self.working_directory_name
		stderr_line = '#PBS -e ' + self.working_directory_name

		xnat_pbs_setup_line = 'source ' + self._get_xnat_pbs_setup_script_path() + ' ' + self._get_db_name()
		xnat_pbs_setup_singularity_load = 'module load ' + self._get_xnat_pbs_setup_script_singularity_version()
		xnat_pbs_setup_singularity_process = 'singularity exec -B ' + xnat_pbs_jobs_control_folder + ':/opt/xnat_pbs_jobs_control' \
											+ ',' + self._get_xnat_pbs_setup_script_archive_root() + ',' + self._get_xnat_pbs_setup_script_singularity_bind_path() \
											+ ',' + self._get_xnat_pbs_setup_script_gradient_coefficient_path() + ':/export/HCP/gradient_coefficient_files' \
											+ ' ' + self._get_xnat_pbs_setup_script_singularity_container_path() + ' ' + '/opt/xnat_pbs_jobs_control/run_qunex.sh' 
		
		studyfolder_line   = '  --studyfolder=' + self.working_directory_name + '/' + self.subject + '_' + self.classifier
		subject_line   = '  --subjects=' + self.subject+ '_' + self.classifier
		scan_line   = '  --scan=' + self.scan
		overwrite_line = '  --overwrite=yes'
		hcppipelineprocess_line = '  --hcppipelineprocess=FunctionalPreprocessing'
		
		with open(script_name, 'w') as script:
			script.write(resources_line + os.linesep)
			script.write(stdout_line + os.linesep)
			script.write(stderr_line + os.linesep)
			script.write(os.linesep)
			script.write(xnat_pbs_setup_line + os.linesep)
			script.write(xnat_pbs_setup_singularity_load + os.linesep)
			
			script.write(os.linesep)
			script.write(xnat_pbs_setup_singularity_process+ ' \\' + os.linesep)
			
			script.write(studyfolder_line + ' \\' + os.linesep)
			script.write(subject_line + ' \\' + os.linesep)
			script.write(scan_line + ' \\' + os.linesep)
			script.write(overwrite_line + ' \\' + os.linesep)
			script.write(hcppipelineprocess_line + os.linesep)
			
			os.chmod(script_name, stat.S_IRWXU | stat.S_IRWXG)
			
	def mark_running_status(self, stage):
		module_logger.debug(debug_utils.get_name())

		if stage > ccf_processing_stage.ProcessingStage.PREPARE_SCRIPTS:
			mark_cmd = self._xnat_pbs_jobs_home
			mark_cmd += os.sep + self.PIPELINE_NAME
			mark_cmd += os.sep + self.PIPELINE_NAME
			mark_cmd += '.XNAT_MARK_RUNNING_STATUS'
			mark_cmd += ' --user=' + self.username
			mark_cmd += ' --password=' + self.password
			mark_cmd += ' --server=' + str_utils.get_server_name(self.put_server)
			mark_cmd += ' --project=' + self.project
			mark_cmd += ' --subject=' + self.subject
			mark_cmd += ' --classifier=' + self.classifier
			mark_cmd += ' --scan=' + self.scan
			mark_cmd += ' --resource=RunningStatus'
			mark_cmd += ' --queued'

			completed_mark_cmd_process = subprocess.run(
				mark_cmd, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)
			print(completed_mark_cmd_process.stdout)

			return

if __name__ == "__main__":
	import ccf.functional_preprocessing.one_subject_run_status_checker as one_subject_run_status_checker
	xnat_server = os_utils.getenv_required('XNAT_PBS_JOBS_XNAT_SERVER')
	username, password = user_utils.get_credentials(xnat_server)
	archive = ccf_archive.CcfArchive()	
	subject = ccf_subject.SubjectInfo(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
	submitter = OneSubjectJobSubmitter(archive, archive.build_home)	
		
	run_status_checker = one_subject_run_status_checker.OneSubjectRunStatusChecker()
	
	if run_status_checker.get_queued_or_running(subject):
		print("-----")
		print("NOT SUBMITTING JOBS FOR")
		print("project: " + subject.project)
		print("subject: " + subject.subject_id)
		print("session classifier: " + subject.classifier)
		print("scan: " + subject.extra)
		print("JOBS ARE ALREADY QUEUED OR RUNNING")
		print ('Process terminated')
		sys.exit()	

	job_submitter=OneSubjectJobSubmitter(archive, archive.build_home)	
	put_server_name = os.environ.get("XNAT_PBS_JOBS_PUT_SERVER_LIST").split(" ")
	put_server = random.choice(put_server_name)

	clean_output_first = eval(sys.argv[5])
	processing_stage_str = sys.argv[6]
	processing_stage = submitter.processing_stage_from_string(processing_stage_str)
	walltime_limit_hrs = sys.argv[7]
	vmem_limit_gbs = sys.argv[8]
	output_resource_suffix = sys.argv[9]
	
	print("-----")
	print("\tSubmitting", submitter.PIPELINE_NAME, "jobs for:")
	print("\t			   project:", subject.project)
	print("\t			   subject:", subject.subject_id)
	print("\t				  scan:", subject.extra)
	print("\t	session classifier:", subject.classifier)
	print("\t			put_server:", put_server)
	print("\t	clean_output_first:", clean_output_first)
	print("\t	  processing_stage:", processing_stage)
	print("\t	walltime_limit_hrs:", walltime_limit_hrs)
	print("\t		vmem_limit_gbs:", vmem_limit_gbs)
	print("\toutput_resource_suffix:", output_resource_suffix)	

	
	# configure one subject submitter
			
	# user and server information
	submitter.username = username
	submitter.password = password
	submitter.server = 'http://' + os_utils.getenv_required('XNAT_PBS_JOBS_XNAT_SERVER')	
	
	# subject and project information
	submitter.project = subject.project
	submitter.subject = subject.subject_id
	submitter.classifier = subject.classifier
	submitter.session = subject.subject_id + '_' + subject.classifier
	submitter.scan = subject.extra

	# job parameters
	submitter.clean_output_resource_first = clean_output_first
	submitter.put_server = put_server
	submitter.walltime_limit_hours = walltime_limit_hrs
	submitter.vmem_limit_gbs = vmem_limit_gbs
	submitter.output_resource_suffix = output_resource_suffix

	# submit jobs
	submitted_job_list = submitter.submit_jobs(processing_stage)

	print("\tsubmitted jobs:", submitted_job_list)
	print("-----")
