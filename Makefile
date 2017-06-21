
pkg=$(shell cat NAME.txt)

test:
	python3 -m unittest $(pkg).tests -v

doc:
	make -C docs html
	cp -r docs/_build/html ${LOCAL_DOCS_DIR}/$(pkg)

