# xfig documents whose corresponding EPS fies are used in the paper
FIGFILES = 

# grace files.   we need to support NXY format with its own rules, so
# we use two extensions: .agr and .nxyagr.
AGRFILES =
NXYAGRFILES =


# matlab scripts that generate output for the paper. These are *.m files
# that generate a corresponding *.eps file when run.
MFILES =

# GNUplot files
PLTFILES = 

# Other EPS files that are necessary to build the document
# If you have eps files that you generate by hand, list them here.
EPSFILES =

# Non-refdbms bib files to be included
# If you need a local .bib file, put that here.
LOCALBIB = local.bib

# default targets to make for make all
TARGETS = techreport.ps


###########################################################################
## Do not modify anything beyond this point                              ##
###########################################################################

####################
## Change log:
## 01/07/21 - SRCFILES now created w/ wildcard function instead of
##            requiring all sources to be listed individually. Problem is
##            the modification of draft.tex will rebuild submit.dvi, but
##            that's not a big deal.

####################
## Bugs:
## If I use the code for only generating refdbms if the .aux differs, there
## is the problem that when make exits, it deletes the .bbl since it was an
## intermediate target.


#-- Programs used when runing latex etc.                       
# note: REFDIR comes from the environment, not set in Makefile
REFBIBTEX = $(REFDIR)/bin/refbibtex
BIBTEX = bibtex -terse
LATEX = latex -interaction=batchmode
DVIPS = dvips -tletter
FIG2DEV = fig2dev
MATLAB = matlab
GNUPLOT = gnuplot
PS2PDF = ps2pdf13 # use Acrobat-4 compatible files

SRCFILES = $(wildcard *.tex)
EPSFIGFILES = $(FIGFILES:.fig=.eps)
EPSMFILES = $(MFILES:.m=.eps)
EPSPLTFILES = $(PLTFILES:.plt=.eps)
EPSAGRFILES = $(AGRFILES:.agr=.eps)
EPSNXYAGRFILES = $(NXYAGRFILES:.nxyagr=.eps)
EPSGENFILES = $(EPSFIGFILES) $(EPSMFILES) $(EPSPLTFILES) $(EPSNXYAGRFILES) \
              $(EPSAGRFILES)
EPSFILES += $(EPSGENFILES)

#--------------------------------------------------------------------------
# Production/target rules                       
#--------------------------------------------------------------------------
.SUFFIXES:
.PHONY: clean all bibclean veryclean
#-- EPS files may be expensive to regenerate, so keep them even though they
#-- are intermediate targets
.PRECIOUS: $(EPSGENFILES)
.SECONDARY: %.bib

all: $(TARGETS)

#-- Make EPS files from matlab documents
%.eps: %.m
	$(MATLAB) -nojvm -nosplash -r "$*;quit" || $(MATLAB) -nodisplay -r "$*;quit"

#-- Make EPS files from xfig documents
%.eps: %.fig
	$(FIG2DEV) -L eps $< $@ || $(FIG2DEV) -L ps $< $@

#-- Make EPS files from GNUplot scripts
%.eps: %.plt
	$(GNUPLOT) $<

#-- Make EPS files from normal grace documents
%.eps: %.agr %.dat
	gracebat $*.dat -param $< -hdevice EPS -printfile $@ -hardcopy

#-- Make EPS files from nxy grace documents
%.eps: %.nxyagr %.dat
	gracebat -nxy $*.dat -param $< -hdevice EPS -printfile $@ -hardcopy

#-- Creation of initial .aux file
%.aux: %.tex $(SRCFILES) $(EPSFILES)
	$(LATEX) $<

#-- Creation of refdbms's .bib file
%.bib: %.aux
#   may fail if there are no citations yet, so ignore errors
	-@if [ -e $(REFBIBTEX) ]; then \
	$(REFBIBTEX) $< > $@; \
	else touch $@; \
	fi;

#-- Create .bbl from .bib
%.bbl: %.bib %.aux
	-$(BIBTEX) $*
	$(LATEX) $*.tex

#-- Build a dvi... 
%.dvi: %.tex %.bbl $(EPSFILES) $(LOCALBIB)
	$(LATEX) $<
#   Pull warnings (about citations and references) out of the log files
	@echo ''
	@echo '*=*=*=*=*=*=*=*=*=*=*=*  LaTeX Warnings  =*=*=*=*=*=*=*=*=*=*=*=*=*'
	@grep 'LaTeX Warning:' $(<:.tex=.log) || true
	@echo '*=*=*=*=*=*=*=*=*=*=*=*  BibTeX Warnings  *=*=*=*=*=*=*=*=*=*=*=*=*'
	@grep 'Warning--' $(<:.tex=.blg) || true
	@echo '*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*'
	@echo ''

#-- converting .dvi to .ps... requires the EPS files
%.ps: %.dvi $(EPSFILES)
	$(DVIPS) -o $@ $<

#-- converting .dvi to .pdf.
%.pdf: %.dvi
	# The ps2pdf settings force all fonts to be embedded and produce 
	# Acrobat 4-compatible files.  Some conferences require this.
	$(DVIPS) -Ppdf -G0 -o - $< | $(PS2PDF) -dPDFSETTINGS=/prepress -dCompatibiliyLevel=1.3 - $@

clean:
	rm -f *~ *.dvi *.log *.aux *.blg *.bbl *.out submit.ps draft.ps techreport.ps submit.pdf

bibclean: clean
	rm -f submit.bib draft.bib

veryclean: bibclean
	rm -f $(EPSGENFILES)

