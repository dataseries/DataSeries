TEXFILES = $(wildcard *.tex *.sty)
MAIN = userguide
TMPFILE = /tmp/$(PAPER).latex-log

all: $(MAIN).ps

$(MAIN).bbl: references.bib $(TEXFILES)
	[ -f $(MAIN).dvi ] || latex $(MAIN).tex </dev/null
	-bibtex $(MAIN)

$(MAIN).dvi: $(TEXFILES) $(MAIN).bbl
	cat /dev/null | latex $(MAIN).tex | tee $(TMPFILE); \
	if [ "`grep 'Rerun to get' $(TMPFILE)`" != "" ]; then \
	   latex $(MAIN).tex; \
	fi; \
	rm $(TMPFILE)

$(MAIN).ps: $(MAIN).dvi
	dvips -tletter -o$(MAIN).ps $(MAIN).dvi

$(MAIN).pdf: $(MAIN).ps
	distill $(MAIN).ps .

clean:
	-rm $(MAIN).dvi $(MAIN).ps $(MAIN).aux $(MAIN).log
	-rm $(MAIN).bbl $(MAIN).blg
