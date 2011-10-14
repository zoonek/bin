#! perl -w
use strict;
use File::Find;
use Digest::MD5 qw(md5);
undef $/;
$|++;
our %f;
sub wanted {
  stat($_);
  return unless -f _;
  open(A, '<', $_) || return;
  my $md5 = md5(<A>);
  close A;
  #  print STDERR "$md5 $_ $File::Find::name\n";
  $f{$md5} = [] unless exists $f{$md5};
  push @{ $f{$md5}  }, $File::Find::name;
}
find(\&wanted, '.');
foreach my $m (keys %f) {
  if( scalar @{ $f{$m} } > 1 ) {
    print join("\n", @{ $f{$m} });
    print "\n\n";
  }
}
