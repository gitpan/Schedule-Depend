use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME => "Schedule::Depend",
    'VERSION_FROM'	=> 'Depend.pm',
	(
		$] >= 5.005
		? (ABSTRACT   => 'Parallel process startup with dependencies and restart.',
		AUTHOR     => 'Steven Lembark (slembark@knightsbridge.com)')
		: ()
	),
	'dist' => {COMPRESS=>'gzip',SUFFIX=>'gz'}
);

