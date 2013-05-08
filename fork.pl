#! perl -w

# Simple load balancer.
# The input is a list of shell commands: run them all, 10 at a time.

use strict;
my $MAX_PROCESSES = shift || 10;

use Parallel::ForkManager;
my $pm = new Parallel::ForkManager($MAX_PROCESSES);
while(<>){
  my $pid = $pm->start and next; 
  system($_);
  $pm->finish; # Terminates the child process
}

