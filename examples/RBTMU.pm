=head1 NAME

RBTMU.pm

=head2 SYNOPSIS

Extension to Schedule::Depend, overloads unalias
method to automated loading data via Red Brick TMU.
Unalias checks for gzip-ed files, deals with them
automatically.

Same calling conventions as Schedule::Depend

e.g., 

	#!/usr/bin/perl -w

	use RBTMU;

	# either find them by name, directory or 
	# have the list of files hard-coded.

	my @dimz  = <dim*>;
	my @factz = <fact*>;

 	my @sched = ();

	push @sched, join ' ', @factz, ':', @dimz;

	# add non-dependency items.

	push @sched, 'verbose	= 1';
	push @sched, 'logdir	= /var/rblogs';
	push @sched, 'rundir	= /var/rbpids';

	my $todo = join "\n", @sched;

	if( my $loader = RBTMU->prepare($todo)->debug )
	{
		eval { $loader->execute };

		die "Failed to execute: $@" if $@;
	}
	else
	{
		die "Unable to prepare schedule: $!";
	}

	0
	__END__

=AUTHOR

Steven Lembark
slembark@knightsbridge.com

=COPYRIGHT

Same terms as Perl itself.

=SEE ALSO

perl(1)

=cut

package RBTMU;

use base 'Schedule::Depend';

use File::Basename;

my $tmudir = dirname $0;

print "$$: Processing tmu files from $tmudir"; 

sub unalias
{
	my $datafile = shift;

	my ( $base, $dir, $ext ) = parsefile( $datafile, '\.*' );

	my $tmufile = "$tmudir/$base.tmu"; 

	-e $tmufile	or croak "$$: missing tmufile: $tmufile";
	-r _		or croak "$$: unreadable tmufile: $tmufile";

	my $gzip = $ext =~ /\.(gz|Z)\b/;

	# the command line begins with gzip for squished
	# files, ends with stdin redirected for non-ziped
	# files.
	#
	# either way, it has rb_tmu $RB_USER $tmufile.

	my @cmd = ();

	push @cmd, 'gzip -dc $datafile |' if $gzip;

	push @cmd, 'rb_tmu $RB_USER', $tmufile;

	push @cmd, "< $datafile" unless $gzip;

	# caller gets back the shell command required to 
	# load the file as a scalar.

	join ' ', @cmd
}

# keep the use pragma happy.

1

__END__
