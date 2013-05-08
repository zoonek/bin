#! /bin/sh
# Reduce the margin around the text in a PDF file, to enlarge it before printing.

# Process the arguments
input=$1
output=$2
tmp=/tmp/pdf_enlarge_$$.pdf

# Find the bounding box (the right margin is larger, so that I can write in it)
echo Processing "$input"
bounding_box=$(
  gs -dNOPAUSE -dBATCH -sDEVICE=bbox "$input" 2>&1 |
    grep "%%BoundingBox" |
    perl -n -e '
      if( m/\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)/) {
          print $1 - .05*($3-$1), " ",
                $2 - .1*($4-$2), " ",
                $3 + .2*($3-$1), " ",
                $4 + .1*($4-$2);
          exit 0;
        }
      '
  )

# Crop
echo Cropping to $bounding_box
gs -sDEVICE=pdfwrite -o $tmp -c "[/CropBox [$bounding_box] /PAGES pdfmark" -f "$input"

# Resize to A4
echo Saving to "$output", A4 format
gs \
  -sOutputFile="$output" \
  -sDEVICE=pdfwrite \
  -sPAPERSIZE=a4 \
  -dCompatibilityLevel=1.4 \
  -dNOPAUSE \
  -dBATCH \
  -dPDFFitPage \
  $tmp

rm -f $tmp
