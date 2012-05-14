base = FinalReport

$(base).pdf: $(base).tex
	pdflatex $(base).tex
	bibtex $(base)
	pdflatex $(base).tex
	pdflatex $(base).tex

clean:
	rm -f *.log *.bbl *.blg *.aux *.dvi *.out *.*.backup $(base).ps $(base).pdf
	rm -f *.*~
