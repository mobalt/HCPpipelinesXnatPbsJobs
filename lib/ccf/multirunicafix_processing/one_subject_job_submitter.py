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

# create a module logger
module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.WARNING)  # Note: This can be overidden by log file configuration

class OneSubjectJobSubmitter(one_subject_job_submitter.OneSubjectJobSubmitter):

	@classmethod
	def MY_PIPELINE_NAME(cls):
		return 'MultiRunIcaFixProcessing'
		
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
	
	@property
	def groups(self):
		subject_info = ccf_subject.SubjectInfo(self.project, self.subject, self.classifier)
		preproc_dirs = self.archive.available_functional_preproc_dir_full_paths(subject_info)
		groupsA = []
		for preproc_dir in preproc_dirs:
			groupsA.append(preproc_dir[preproc_dir.rindex(os.sep)+1:preproc_dir.index("_preproc")])
		groupsA.sort()
		return groupsA

	def _expand(self, group):
		result = ''
		for scan_name in self.groups:
			result += scan_name + '|'
		return result.strip('|')

	def _concat(self, group):
		scan_name_list = group.split(sep='@')
		core_names = []
		for scan_name in scan_name_list:
			parts = scan_name.split(sep='_')
			if parts[1] not in core_names:
				core_names.append(parts[1])

		concat_name = "_".join(core_names)

		if "REST" in concat_name:
			concat_name = "rfMRI_" + concat_name + "_RL_LR"
		else:
			concat_name = "tfMRI_" + concat_name + "_RL_LR"

		return concat_name

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
		script.write('  --working-dir=' + self.working_directory_name + os.linesep)
		script.write(os.linesep)
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
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'processing ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)		
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'subjects' + os.path.sep + 'specs ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)
		script.write('mv ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'info' + os.path.sep + 'hcpls ')
		script.write(self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo' + os.linesep)
		script.write('find ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier)
		script.write(' -maxdepth 1 -mindepth 1 \( -type d -not -path ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'ProcessingInfo')
		script.write(' -a -not -path ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + os.path.sep + 'MNINonLinear')
		script.write(' \) -exec rm -rf \'{}\' \;')
		script.write(os.linesep)
		script.write('find ' + self.working_directory_name + os.path.sep + self.subject + '_' + self.classifier + " -type d -empty -delete")
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

		subject_info = ccf_subject.SubjectInfo(self.project, self.subject, self.classifier)


		script_name = self.process_data_job_script_name

		with contextlib.suppress(FileNotFoundError):
			os.remove(script_name)

		walltime_limit_str = str(self.walltime_limit_hours) + ':00:00'
		## Using mem option instead of vmem for IcaFix	
		#vmem_limit_str = str(self.vmem_limit_gbs) + 'gb'
		mem_limit_str = str(self.mem_limit_gbs) + 'gb'
		resources_line = '#PBS -l nodes=' + str(self.WORK_NODE_COUNT)
		resources_line += ':ppn=' + str(self.WORK_PPN)
		## FIX shouldn't be limited to haswell cores
		#resources_line += ':haswell'
		resources_line += ',walltime=' + walltime_limit_str
		#resources_line += ',vmem=' + vmem_limit_str
		resources_line += ',mem=' + mem_limit_str
		stdout_line = '#PBS -o ' + self.working_directory_name
		stderr_line = '#PBS -e ' + self.working_directory_name
		scratch_tmpdir = '/scratch/hodgem/singularity/tmp/' + self.subject + "_" + self.classifier;
		xnat_pbs_setup_singularity_load = 'module load ' + self._get_xnat_pbs_setup_script_singularity_version()
		make_scratch_tmpdir = 'mkdir  -p ' + scratch_tmpdir
		xnat_pbs_setup_singularity_process = 'singularity exec -B ' + xnat_pbs_jobs_control_folder + ':/opt/xnat_pbs_jobs_control' \
										+ ',' + scratch_tmpdir + ':/tmp' \
											+ ',' + self._get_xnat_pbs_setup_script_archive_root() + ',' + self._get_xnat_pbs_setup_script_singularity_bind_path() \
										+ ',' + self._get_xnat_pbs_setup_script_gradient_coefficient_path() + ':/export/HCP/gradient_coefficient_files' \
										+ ' ' + self._get_xnat_pbs_setup_script_singularity_container_path() + ' ' + self._get_xnat_pbs_setup_script_singularity_qunexrun_path()
		#xnat_pbs_setup_singularity_process = '/opt/xnat_pbs_jobs_control/run_qunexContainer.sh' 
		#xnat_pbs_setup_singularity_process = self._get_xnat_pbs_setup_script_singularity_qunexrun_path()
		#container_line   = '  --containerpath=' + self._get_xnat_pbs_setup_script_singularity_container_path()
		## Per MH, parameterfolder is irrelevant to MR-FIX
		#parameter_line   = '  --parameterfolder=' + self._get_xnat_pbs_setup_script_singularity_qunexparameter_path()
		studyfolder_line   = '  --studyfolder=' + self.working_directory_name + '/' + self.subject + '_' + self.classifier
		subject_line   = '  --subjects=' + self.subject+ '_' + self.classifier
		overwrite_line = '  --overwrite=yes'
		hcppipelineprocess_line = '  --hcppipelineprocess=MultiRunIcaFixProcessing'

		with open(script_name, 'w') as script:
			script.write(resources_line + os.linesep)
			script.write(stdout_line + os.linesep)
			script.write(stderr_line + os.linesep)
			script.write(os.linesep)
			script.write(xnat_pbs_setup_singularity_load + os.linesep)
			script.write(make_scratch_tmpdir + os.linesep)
			script.write(os.linesep)
			script.write(xnat_pbs_setup_singularity_process+ ' \\' + os.linesep)
			## Per MH, parameterfolder is irrelevant to MR-FIX
			#script.write(parameter_line + ' \\' + os.linesep)
			script.write(studyfolder_line + ' \\' + os.linesep)
			script.write(subject_line + ' \\' + os.linesep)
			script.write(overwrite_line + ' \\' + os.linesep)
			#script.write(container_line + ' \\' + os.linesep)
			self._group_list = []
			script.write('  --boldlist="' + self._expand(self.groups) + '" \\' + os.linesep)
			script.write(hcppipelineprocess_line + os.linesep)
			script.close()
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
			mark_cmd += ' --resource=RunningStatus'
			mark_cmd += ' --queued'

			completed_mark_cmd_process = subprocess.run(
				mark_cmd, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)
			print(completed_mark_cmd_process.stdout)
			
			return

if __name__ == "__main__":
	import ccf.multirunicafix_processing.one_subject_run_status_checker as one_subject_run_status_checker
	xnat_server = os_utils.getenv_required('XNAT_PBS_JOBS_XNAT_SERVER')
	username, password = user_utils.get_credentials(xnat_server)
	archive = ccf_archive.CcfArchive()	
	subject = ccf_subject.SubjectInfo(sys.argv[1], sys.argv[2], sys.argv[3])
	submitter = OneSubjectJobSubmitter(archive, archive.build_home)
	
	run_status_checker = one_subject_run_status_checker.OneSubjectRunStatusChecker()
	if run_status_checker.get_queued_or_running(subject):
		print("-----")
		print("NOT SUBMITTING JOBS FOR")
		print("project: " + subject.project)
		print("subject: " + subject.subject_id)
		print("session classifier: " + subject.classifier)
		print("JOBS ARE ALREADY QUEUED OR RUNNING")
		print ('Process terminated')
		sys.exit()	
		
	job_submitter=OneSubjectJobSubmitter(archive, archive.build_home)	
	put_server_name = os.environ.get("XNAT_PBS_JOBS_PUT_SERVER_LIST").split(" ")
	put_server = random.choice(put_server_name)

	clean_output_first = eval(sys.argv[4])
	processing_stage_str = sys.argv[5]
	processing_stage = submitter.processing_stage_from_string(processing_stage_str)
	walltime_limit_hrs = sys.argv[6]
	#vmem_limit_gbs = sys.argv[7]
	mem_limit_gbs = sys.argv[7]
	output_resource_suffix = sys.argv[8]
	#group_list = sys.argv[10]
	
	
	print("-----")
	print("\tSubmitting", submitter.PIPELINE_NAME, "jobs for:")
	print("\t			   project:", subject.project)
	print("\t			   subject:", subject.subject_id)
	print("\t	session classifier:", subject.classifier)
	print("\t			put_server:", put_server)
	print("\t	clean_output_first:", clean_output_first)
	print("\t	  processing_stage:", processing_stage)
	print("\t	walltime_limit_hrs:", walltime_limit_hrs)
	#print("\t		vmem_limit_gbs:", vmem_limit_gbs)
	print("\t		mem_limit_gbs:", mem_limit_gbs)
	print("\toutput_resource_suffix:", output_resource_suffix)

	
	# configure one subject submitter
			
	# user and server information
	submitter.username = username
	submitter.password = password
	submitter.server = 'http://' + os_utils.getenv_required('XNAT_PBS_JOBS_XNAT_SERVER')

	# subject and project information
	submitter.project = subject.project
	submitter.subject = subject.subject_id
	submitter.session = subject.subject_id + '_' + subject.classifier
	submitter.classifier = subject.classifier

			
	# job parameters
	submitter.clean_output_resource_first = clean_output_first
	submitter.put_server = put_server
	submitter.walltime_limit_hours = walltime_limit_hrs
	#submitter.vmem_limit_gbs = vmem_limit_gbs
	submitter.mem_limit_gbs = mem_limit_gbs
	submitter.output_resource_suffix = output_resource_suffix

	# submit jobs
	submitted_job_list = submitter.submit_jobs(processing_stage)
	print("\tsubmitted jobs:", submitted_job_list)
	print("-----")
			
