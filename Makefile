
pkg=$(shell cat NAME.txt)

test:
	python3 -m unittest $(pkg).tests -v

doc:
	make -C docs html
ifndef LOCAL_DOCS_DIR
	$(error LOCAL_DOCS_DIR is undefined)
endif
	cp -r docs/_build/html ${LOCAL_DOCS_DIR}/$(pkg)

