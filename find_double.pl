#! perl -w

# Look for duplicated files:
# - First check the size of all files
# - For sizes that appear several times (hopefully, this will exclude very large files),
#   compute their MD5 sum.
# - Print duplicated files

use strict;
use File::Find;
use Digest::MD5 qw(md5);
undef $/;
$|++;

our %s;
our %f;

sub size {
  stat($_);
  return unless -f _;
  my $size = -s _;
  #print STDERR "SIZE: $size $_ $File::Find::name\n";
  $s{$size} = [] unless exists $s{$size};
  push @{ $s{$size}  }, $File::Find::name;
}

sub contents {
  stat($_);
  return unless -f _;
  my $size = -s _;
  return unless scalar @{ $s{$size} } > 1;

  open A, "-|", "md5sum", "-b", "--", $_;  # Do not read the whole file in Perl: it can be large.
  chomp( my $md5 = <A> );
  $md5 =~ s/ .*//gsm;
  close(A);

  #print STDERR "MD5: $md5 *** $_ *** $File::Find::name\n";
  $f{$md5} = [] unless exists $f{$md5};
  push @{ $f{$md5}  }, $File::Find::name;
}

find(\&size,     '.');
find(\&contents, '.');

foreach my $m (keys %f) {
  if( scalar @{ $f{$m} } > 1 ) {
    my $size = -s $f{$m}->[0];
    print $size, " ", join("\n" . $size . " ", @{ $f{$m} });
    print "\n\n";
  }
}
