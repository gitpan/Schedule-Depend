use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME	=> "Schedule::Depend",

    VERSION	=> '1.1',

	(
		$] ge '5.006001' ? 
		(
			ABSTRACT	=>
'Parallel process startup with dependencies and restart.
Execute, Config, Utilities modules simplify generating
#! code for running queues.',

			AUTHOR     	=> 'Steven Lembark <lembark@wrkhors.com>',
			PREREQ_PM	=> 
			{
				# unlikely these aren't avaialble, but can't 
				# hurt to check...

				Carp			=> 0,
				constant		=> 0,
				strict			=> 0,

				Data::Dumper	=> 2.102,
				File::Basename	=> 2.6,

				FindBin			=> 1.44,

				# these may have to be installed,
				# used in S::D::Execute

				FindBin::libs	=> 1.0,
				Cwd				=> 2.12,
			},
		)
		: ()
	),

	dist =>
	{
		COMPRESS	=> 'gzip',
		SUFFIX		=> 'gz'
	}
);

