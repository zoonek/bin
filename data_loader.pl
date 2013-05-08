#! perl -w

##
## Load a CSV file into a database, creating or extening the
## table schema if needed.
## You should be aware that the result will not be very
## clean, in particular, the database will usually not be in
## third normal form and referencial integrity will not be
## enforced.
##
## May work with Postgres, may not work with other databases.
##

############################################################
##
## Modules
##
############################################################

use strict;
use warnings;
use Getopt::Simple qw/$switch/;
use Text::CSV_XS;
use Data::Dumper;
use IO::File;       # Needed to use Text::CSV_XS
use POSIX;          # For strtod(), to infer the type of the columns

use constant TRUE  => 0==0;
use constant FALSE => 0==1;

############################################################
##
## A few functions
##
############################################################

##
## In case two columns have the same name (this can be due
## to them genuinely having the same name, or having the
## same name up to capitalization, or the same name up to
## non-alphanumeric characters), we change the name of the
## second by adding "_X1", "_X2", etc. to it.
## This function creates those new names.
##

sub alter_duplicate_column_names {
  my %a = ();
  my @result = ();
  foreach (@_) {
    if (exists $a{$_}) {
      my $i=0;
      $i++ while exists $a{ $_ . "_X" . $i };
      $_ .= "_X" . $i;
    }
    $a{$_} = 1;
    push @result, $_;
  }
  @result;
}

##
## The values to be inserted into the database have to be
## slightly modified:
##  - They are quoted
##  - Missing values (as described by the --NA command line
##    option) are replaced by NULL
##  - To avoid other problems, dangerous characters (single
##    quote (') and backslash (\)) are replaced by a space.
##
## There are two versions of this function: one that
## produces unquoted results, useful to infer the type of
## the columns, and a quoted one, for the actual generation
## of SQL code.
##

sub process_values_unquoted {
  my @a = map { $a = $_;   # Do not modify $_: it would
                           # change the elements of
                           # @extra_values...
                $a =~ s/\'/_/g;
                $a = "" if $a =~ m/$$switch{"NA"}/o;
                $a;
              } @_;
  return @a;
}

sub process_values_quoted {
  my @a = map { $a = $_;   # Do not modify $_: it would
                           # change the elements of
                           # @extra_values...
                $a =~ s/\'/_/g;
                if ($a =~ m/$$switch{"NA"}/o) {
                  $a = "NULL";
                } else {
                  $a = "\'$a\'";
                }
                $a;
              } @_;
  return @a;
}

##
## Some command line options expect comma-seperated lists of
## column names or numbers: this function transforms them
## into lists of column names.
##

sub get_column_names ($@) {
  my ($col, @column_names) = @_;
  my @col = split(",", $col);
  # Convert the column numbers to column names
  for (my $i=0; $i<=$#col; $i++) {
    if ( (POSIX::strtod($col[$i])) [1] == 0 ){
      $col[$i] = $column_names[ $col[$i] - 1 ];
    }
  }
  return @col;
}

############################################################
##
## Parameters: how does the CSV file(s) look like?
##
############################################################

my $option = Getopt::Simple -> new();
$option -> getOptions({
  quote_char => { type    => "=s",
                  default => q/"/,  # Usually NOT ', because it
                                    # appears in some French names...
                  verbose => "quote character" },
  sep_char   => { type    => "=s",
                  default => q/,/,  # Could also be | or \t
                  verbose => "field separator" },
  header     => { type    => "=i",
                  default => 1,
                  verbose => "number of the line containing the headers" },
  data       => { type    => "=i",
                  default => 2,
                  verbose => "number of the first line after the headers" },
  table_name => { type    => "=s",
                  default => "Foo",
                  verbose => "Name of the SQL table to create and populate" },
  NA         => { type    => "=s",
                  default => '^\s*(|\.|NA|NULL|Null|Default|Inf|-Inf|Err|-999(\.0*)?|[#@]?N/?A\!?)\s*$',
                  verbose => 'Regular expression to match missing values, e.g., ^NA$' },
  "add-column" => { type  => "=s@",
                  default => [],
                  verbose => "Columns missing in the CSV file, usually because they are constant and can be inferred from the file name; e.g., date=2006-03-27"
                },
  "index"    => { type    => "=s@",
                  default => [],
                  verbose => "Columns on which to create an INDEX, e.g. '1,2,3' or 'id,date'"
                },
  "unique"   => { type    => "=s@",
                  default => [],
                  verbose => "UNIQUE constraints to impose"
                },
  "not-null" => { type    => "=s@",
                  default => [],
                  verbose => "NOT NULL constraints to impose"
                },
  "after-begin-transaction" => { type    => "=s@",
                                 default => [],
                                 verbose => "SQL commands to execute just after the transaction begins, e.g., \"DELETE FROM TABLE Foo;\" or \"DELETE FROM TABLE Foo WHERE date = '2006-12-25';\""
                   },
  "before-commit-transaction" => { type    => "=s@",
                                   default => [],
                                   verbose => "SQL commands to execure just before the transaction is committed, e.g., \"DELETE FROM TABLE Foo WHERE x IS NULL OR y IS NULL;\""
                                 },
  "ascii"    => { type => "",
                  default => "",
                  verbose => "Should the encoding be left untouched (some DBMS expect UTF8, others latin1, and the encoding of the data files is rarely mentionned) or should non-ASCII characters be discarded?"
                },
  "no-multiline-fields" => { type => "",
                             default => "",
                             verbose => "Should we consider the files containing multiline fields as corrupted and discard them?"
                            },
  "only-data"           => { type => "",
                             default => "",
                             verbose => "Only print the INSERT statements, not the table and index creation and alterations"
                            },
  "rawdata"             => { type => "",
                             default => "",
                             verbose => "Print just the data, as a delimited instead of INSERT statements (to allow simple data transformations)"
                            },
  "rawdata-sepchar"      => { type => "",
                             default => "|",
                             verbose => "Separator character to use when outputting rawdata"
                            },
  "no-column-type-check" => { type => "",
                              default => "",
                              verbose => "Should we try to guess the type of all the columns or set them all to VARCHAR(255)?"
                            },
  "wide"     => { type    => "=i",
                  default => -1,
                  verbose => "If the file contains wide data, number of the column where these data start; e.g., if the columns are factor1,factor2,2000,2001,2002,etc., this would be 3"
                },
  "wide-name" => { type   => "=s",
                  default => "Wide_column_name",
                  verbose => "If the file contains wide data, name of the (SQL) column that will identify those data, e.g., if the file header is factor1,factor2,2000,2001,2002,etc., this could be 'date'"
                 },
  "wide-value" => { type  => "=s",
                  default => "Wide_value",
                  verbose => "If the file contains wide data, name of the (SQL) column that will contain those data, e.g., if the file header is factor1,factor2,2000,2001,2002,etc., this could be 'date'"
                 }
}, "usage: $0 [options] file.csv");

if ($$switch{"rawdata"} && ! $$switch{"only-data"}) {
  warn "$0: --rawdata implies --only-data";
  $$switch{"only-data"} = 1;
}

my @extra_headers = map { $a = $_; $a =~ s/=.*//; $a; }
                        @{ $$switch{"add-column"} };
my @extra_values  = map { $a = $_; $a =~ s/^.*?=//; $a; }
                        @{ $$switch{"add-column"} };

if ($$switch{"data"} <= $$switch{"header"}) {
  $$switch{"data"} = $$switch{"header"} + 1;
}

#print STDERR "Options:\n";
#print STDERR Dumper($switch);

if (@extra_headers) {
  print STDERR "Extra headers: " . join(", ", @extra_headers) . "\n";
  print STDERR "       values: " . join(", ", @extra_values) . "\n";
}

if ($$switch{"sep_char"} eq "TAB") {
  $$switch{"sep_char"} = "\t";
}
my $csv = new Text::CSV_XS({ 
  quote_char => $$switch{"quote_char"},
  sep_char   => $$switch{"sep_char"},
  binary     => TRUE
});

my $file = shift @ARGV or die "usage: $0 file.csv";
my $fh = new IO::File;

############################################################
##
## Trying to infer the type of the columns
##
############################################################

my @types;
{
  print STDERR "Reading file $file to get the number of columns and their types\n";
  open($fh, "<", $file) || die "Cannot open $file for reading: $!";
  my $line = 0;
  while(1) {
    my $fields = $csv->getline($fh);
    last unless $fields;
    last unless @$fields;
    if ($$switch{"no-multiline-fields"}) {
      if (grep /\n./, @$fields) {
        # BUG: This is not SQL but PL/SQL...
        print "RAISE NOTICE 'Multiline field -- aborting';\n";
        die "Multiline field in $file line " . ($line + 1) .
            ": aborting\n";
      }
    }
    next if $#$fields == 0 and $$fields[0] =~ m/^\s*$/;   # Skip blank lines
    $line++;
    if ($line == $$switch{"header"}) {
      if ($$switch{"no-column-type-check"}) {
        @types = map { FALSE } (@extra_headers, @$fields);
        last;
      } else {
        @types = map { TRUE } (@extra_headers, @$fields);
      }
    } elsif ($line >= $$switch{"data"}) {
      #print STDERR $line . " " . join(", ", @$fields) . "\n";
      my @values = process_values_unquoted(@extra_values, @$fields);
      #print STDERR $line . " " . join(", ", @values) . "\n";
      #print STDERR $line . " " . join(", ", map { (POSIX::strtod($_))[1] > 0 ? "VARCHAR(255)" : "NUMERIC" } @values) . "\n";
      @values = map { (POSIX::strtod($_))[1] == 0 } @values;
      for (my $i=0; $i<=$#values; $i++) {
        $types[$i] &&= $values[$i];
      }
      #print STDERR $line . " " . join(", ", map { $_ ? "NUMERIC" : "VARCHAR(255)" } @types) . "\n";
    }
  }
  close($fh);
  if ($$switch{"wide"} > 0) {
    for (my $i = $#extra_headers + 1 + $$switch{"wide"}; $i <= $#types; $i++) {
      $types[ $#extra_headers + 1 + $$switch{"wide"} ] = 
        $types[ $#extra_headers + 1 + $$switch{"wide"} ] && $types[$i];
    }
    $types[ $#extra_headers + 1 + $$switch{"wide"} - 1 ] = FALSE;
    @types = @types[0..($#extra_headers + 1 + $$switch{"wide"})];
  }
  @types = map { $_ ? "NUMERIC" : "VARCHAR(255)" } @types;
  print STDERR "Column types: ";
  print STDERR join(", ", @types) . "\n";
}

############################################################

print STDERR "Reading file $file\n";
open($fh, "<", $file) || die "Cannot open $file for reading: $!";
my @column_names;
my @wide_values;
my $line = 0;
while(1) {
  my $fields = $csv->getline($fh);
  last unless $fields;
  last unless @$fields;
  next if $#$fields == 0 and $$fields[0] =~ m/^\s*$/;   # Skip blank lines
  $line++;
  if ($line == $$switch{"header"}) {    ## Header
    @column_names = @$fields;
    if ($$switch{"wide"} > 0) {
      @wide_values = @column_names[ ($$switch{"wide"}-1) .. ($#column_names) ];
      map { s/^\s+//; s/\s+$//; } @wide_values;
      @column_names = @column_names[ 0 .. ($$switch{"wide"}-1) ];
      $column_names[ $$switch{"wide"} - 1 ] = $$switch{"wide-name"};
      $column_names[ $$switch{"wide"}     ] = $$switch{"wide-value"};
    }
    @column_names = (@extra_headers, @column_names);
    @column_names = map { y/A-Z/a-z/;       # Only lower case
                          s/\s+$//;         # No trailing spaces
                          s/^\s+//;         # No leading spaces
                          s/[^a-z0-9]/_/g;  # Only alphanumeric characters
                          s/^([0-9])/x$1/;  # First character is a letter
                          s/^$/nameless_column/; # At least one character
                          $_; } @column_names;
    @column_names = alter_duplicate_column_names(@column_names);
    my %not_null = ();
    foreach my $i (@{$$switch{"not-null"}}) {
      foreach my $j (get_column_names($i, @column_names)) {
        $not_null{$j} = 1;
      }
    }

    if ( ! $$switch{"only-data"} ) {

      print "-- Table schema\n";
      print "CREATE TABLE " . $$switch{"table_name"} . " (\n";
      for (my $i=0; $i <= $#column_names; $i++) {
        print "  \"" . $column_names[$i] . "\" " . $types[$i];
        print " NOT NULL" if exists $not_null{ $column_names[$i] };
        print "," if $i < $#column_names or @{$$switch{"unique"}};
        print "\n";
      }
      foreach (my $j=0; $j <= $#{ $$switch{"unique"} }; $j++) {
        my $col = ${ $$switch{"unique"} }[$j];
        my @col = get_column_names($col, @column_names);
        map { s/(.*)/"$1"/ } @col;
        print "  UNIQUE (" . join(", ", @col) . ")";
        #     ." ON CONFLICT REPLACE";
        print "," unless $j == $#{ $$switch{"unique"} };
        print "\n";
      }
      print ");\n";
      print "-- In case the table already exists, we make sure it has enough columns...\n";
      for (my $i=0; $i<=$#column_names; $i++) {
        print "ALTER TABLE " . $$switch{"table_name"} .
              " ADD COLUMN \"" . $column_names[$i] . "\" " .
              $types[$i] . ";\n";
        if ($types[$i] eq "VARCHAR(255)") {
          print "SELECT num_col_to_varchar('".
                $$switch{"table_name"} .
                "', '".
                $column_names[$i] .
                "');\n";
        }
      }
      if (@{ $$switch{"index"} }) {
        print "-- Indices\n";
        foreach my $col (@{ $$switch{"index"} }) {
          my @col = get_column_names($col, @column_names);
          print "CREATE INDEX " .
                "idx_" . $$switch{"table_name"} . "_" .
                join("_", @col) .
                " ON " .
                $$switch{"table_name"} .
                " (".
                join(", ", @col) .
                ");\n";
        }
      }
    }
    
    if ( ! $$switch{"rawdata"} ) {
      print "-- The data from $file\n";
      print "BEGIN TRANSACTION;\n";
      if (@{$$switch{"after-begin-transaction"}}) {
        print "-- User-provided SQL commands\n";
        print join("\n", @{$$switch{"after-begin-transaction"}});
        print "\n-- End of user-provided SQL commands\n";
      }
      #print "PRAGMA cache_size = 500000;\n";
    }
  } elsif ($line >= $$switch{"data"}) {     ## Data
    map { s/^\s+//; s/\s+$//; } @$fields;
    map { s/\\//g; s/'//g; } @$fields;  # Discard weird characters: '\
    if ($$switch{"ascii"}) { # Discard non-ASCII characters
      map { s/[\x80-\xFF]/ /g } @$fields;
    }
    if ($$switch{"wide"} > 0) {
      if ( $$switch{"rawdata"} ) {
        for (my $i=0; $i <= $#wide_values; $i++) {
          print "" .
                join($$switch{"rawdata-sepchar"}, process_values_unquoted(
                            @extra_values,
                @$fields[ 0 .. ($$switch{"wide"}-2) ],
                            $wide_values[$i],
                            $$fields[$$switch{"wide"} - 1 + $i]) ) .
                "\n";
        }
      } else {

        for (my $i=0; $i <= $#wide_values; $i++) {
          print "INSERT INTO " . $$switch{"table_name"} . " (\"" .
                join("\", \"", @column_names) .
                "\")\n";
          print "  VALUES (" .
                join(", ", process_values_quoted(
                            @extra_values,
                @$fields[ 0 .. ($$switch{"wide"}-2) ],
                            $wide_values[$i],
                            $$fields[$$switch{"wide"} - 1 + $i]) ) .
                ");\n";
        }
      }

    } else {
	if ( $$switch{"rawdata"} ) {
	    print "" . join($$switch{"rawdata-sepchar"}, 
			    process_values_unquoted(
						    @extra_values,
						    @$fields)) . "\n";
	} else {

	    print "INSERT INTO " . $$switch{"table_name"} . " (\"" .
		join("\", \"", @column_names) .
		"\")\n";
	    print "  VALUES (" .
		join(", ", process_values_quoted(@extra_values, @$fields)) .
		");\n";
	}
    }
  }
}

if ( ! $$switch{"rawdata"} ) {
  if (@{$$switch{"before-commit-transaction"}}) {
    print "-- User-provided SQL commands\n";
    print join("\n", @{$$switch{"before-commit-transaction"}});
    print "\n-- End of user-provided SQL commands\n";
  }
  print "COMMIT TRANSACTION;\n";
}
close($fh);
