use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME	=> "Schedule::Depend",

    VERSION	=> '2.5',

	ABSTRACT	=>
'Parallel process startup with dependencies and restart.
Execute, Config, Utilities modules simplify generating
#! code for running queues including logging, email
notification, file read/write, and generation of absolute
paths relative to the execution directory paths via tokens .',

	AUTHOR     	=> 'Steven Lembark <lembark@wrkhors.com>',
	PREREQ_PM	=> 
	{
		# unlikely these aren't avaialble, but can't 
		# hurt to check... Symbol is core as of 5.8.

		Carp			=> 0,
		constant		=> 0,
		strict			=> 0,
		Symbol			=> 0,

		Data::Dumper	=> 2.102,
		File::Basename	=> 2.6,

		FindBin			=> 1.44,

		# these may have to be installed,
		# used in S::D::Execute

		FindBin::libs	=> 1.0,
		Cwd				=> 2.12,
	},

	dist =>
	{
		COMPRESS	=> 'gzip',
		SUFFIX		=> 'gz'
	}
);

