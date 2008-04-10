TEXFILES = $(wildcard *.tex *.sty)
MAIN = tr
TMPFILE = /tmp/$(PAPER).latex-log

all: $(MAIN).ps $(MAIN).pdf

#$(MAIN).bbl: references.bib $(TEXFILES)
#	latex $(MAIN).tex </dev/null
#	-bibtex $(MAIN)

$(MAIN).dvi: $(TEXFILES) $(wildcard *.bib)
	-latex $(MAIN).tex </dev/null >/dev/null
	-bibtex $(MAIN)
	latex $(MAIN).tex </dev/null | tee $(TMPFILE)
	-bibtex $(MAIN)
	@echo "RUNNING LATEX UNTIL CONVERGENCE DON'T BE SCARED"
	while [ "`grep -c 'Rerun to get' $(TMPFILE)`" != 0 ]; do \
	   latex $(MAIN).tex </dev/null | tee $(TMPFILE); \
	   bibtex $(MAIN); \
	done; \

	rm $(TMPFILE)

$(MAIN).ps: $(MAIN).dvi
	dvips -tletter -o$(MAIN).ps $(MAIN).dvi

$(MAIN).pdf: $(MAIN).dvi
	dvipdf $(MAIN).dvi

clean:
	-rm $(MAIN).dvi $(MAIN).ps $(MAIN).aux $(MAIN).log
	-rm $(MAIN).bbl $(MAIN).blg $(MAIN).pdf
