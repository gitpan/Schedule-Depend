use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME => "Schedule::Depend",
    'VERSION_FROM'	=> 'Depend.pm',
	(
		$] >= 5.005 ?
		(
			ABSTRACT	=> 'Parallel process startup with dependencies and restart.',
			AUTHOR     	=> 'Steven Lembark (slembark@knightsbridge.com)',
			PREREQ_PM	=> 
			{
				# unlikely these aren't avaialble, but can't 
				# hurt to check...

				Carp			=> 0,
				constant		=> 0,
				strict			=> 0,

				Data::Dumper	=> 2.102,
				File::Basename	=> 2.6,
				Text::Balanced	=> 1.89,

			},
		)
		: ()
	),
	'dist' => {COMPRESS=>'gzip',SUFFIX=>'gz'}
);

