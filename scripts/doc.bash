cd docs
make html
rsync -avz _build/html/ ~/WindowsShare/temp/html/pymake
