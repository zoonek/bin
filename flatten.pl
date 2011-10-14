#!/usr/bin/perl -w
use strict;
use constant DEBUG => 0;
use File::Find;
sub wanted {
  stat($_);
  return unless -f _;
  my $name = $_;
  my $new = $name;
  $new =~ s#/#_#g;
  $new =~ s#^\._\._##g;
  $new =~ s#^\._##g;
  print STDERR "Renaming $name to $new\n" if DEBUG;
  rename $name, $new
    or warn "Cannot rename $name to $new: $!";
  $_ = $name;
}
find({wanted => \&wanted, no_chdir => 1}, '.');

