use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME			=> "Schedule::Depend",
    VERSION_FROM	=> 'Depend.pm',
	(
		$] >= '5.006' ? 
		(
			ABSTRACT	=> 'Parallel process startup with dependencies and restart.',
			AUTHOR     	=> 'Steven Lembark (slembark@knightsbridge.com)',
			PREREQ_PM	=> 
			{
				# unlikely these aren't avaialble, but can't 
				# hurt to check...

				constant		=> 0,
				strict			=> 0,
				Carp			=> 0,

				# These are both part of the 5.6 distribution.
				# May have to upgrade versions.

				Data::Dumper	=> 2.102,
			},
		)
		: ()
	),

	dist =>
	{
		COMPRESS	=> 'gzip',
		SUFFIX		=> 'gz',
	}

);

