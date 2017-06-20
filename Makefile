
pkg=$(shell cat NAME.txt)

test:
	python3 -m unittest $(pkg).tests -v

