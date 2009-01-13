#!/bin/sh
BUILD=`deptool cdopt $PWD | tail | awk '{print $2}'`
echo $BUILD
rm -rf html
mkdir html
ln -s $BUILD/fast2009-nfs-analysis.aux .
ln -s $BUILD/fast2009-nfs-analysis.bbl .

PATH=/opt/beta/latex2html/2008/bin:$PATH
latex2html -verbosity=2 -antialias -split 0 -show_section_numbers -local_icons -no_navigation -dir html -init_file latex2html.init ~/projects/DataSeries/doc/fast2009-nfs-analysis/fast2009-nfs-analysis.tex
rm fast2009-nfs-analysis.{aux,bbl}

