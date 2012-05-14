base = auctionmark-benchmark
images = images/*.eps

$(base).pdf: $(base).tex
	latex $(base).tex
	bibtex $(base)
	latex $(base).tex
	latex $(base).tex
	dvips -t letter $(base).dvi -o $(base).ps
	ps2pdf $(base).ps
clean:
	rm -f *.log *.bbl *.blg *.aux *.dvi *.out *.*.backup $(base).ps $(base).pdf
	rm -f $(base).zip
	rm -f *.*~

zip: $(base).pdf
	rm -f $(base).zip
	zip -r $(base).zip $(base).tex $(base).bib $(base).pdf\
		 sig-alternate.cls Makefile \
	       images/*.eps graphs/*.eps algorithms/*.tex 