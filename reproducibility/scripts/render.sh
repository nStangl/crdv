#!/bin/bash

./plot.sh
cd ../document
echo "Building document"
max_print_line=1000 pdflatex --interaction=nonstopmode main > log.txt
grep '^!.*' log.txt
pdflatex --interaction=nonstopmode main > /dev/null
rm main.aux main.log log.txt
cd ../scripts
